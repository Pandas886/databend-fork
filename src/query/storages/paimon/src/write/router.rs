// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use paimon::spec::CoreOptions;
use paimon::spec::TableSchema;

use crate::error::map_paimon_error;

/// Length-prefixed partition bytes + big-endian bucket id.
///
/// Keeps the partition/bucket boundary unambiguous so Exchange can hash the
/// opaque key without decoding Paimon partition encoding.
pub fn encode_route_key(partition: &[u8], bucket: i32) -> Vec<u8> {
    let mut key = Vec::with_capacity(4 + partition.len() + 4);
    key.extend_from_slice(&(partition.len() as u32).to_be_bytes());
    key.extend_from_slice(partition);
    key.extend_from_slice(&bucket.to_be_bytes());
    key
}

/// Test/debug-only: record that `route_key` is owned by `(executor_id, lane_id)`
/// for `query_id`. Same route mapped to two lanes returns an internal error so
/// cluster regressions fail the constraint rather than only checking final rows.
///
/// Compiled out of release builds (no production HashMap).
#[cfg(any(test, debug_assertions))]
pub fn observe_route_lane(
    query_id: &str,
    route_key: &[u8],
    executor_id: &str,
    lane_id: u64,
) -> Result<()> {
    lane_observe::observe_route_lane(query_id, route_key, executor_id, lane_id)
}

#[cfg(not(any(test, debug_assertions)))]
pub fn observe_route_lane(
    _query_id: &str,
    _route_key: &[u8],
    _executor_id: &str,
    _lane_id: u64,
) -> Result<()> {
    Ok(())
}

/// Allocate a process-local writer lane id (test/debug observation).
#[cfg(any(test, debug_assertions))]
pub fn next_lane_id_for_test() -> u64 {
    lane_observe::next_lane_id()
}

#[cfg(not(any(test, debug_assertions)))]
pub fn next_lane_id_for_test() -> u64 {
    0
}

#[doc(hidden)]
#[cfg(any(test, debug_assertions))]
pub fn reset_lane_observations_for_test() {
    lane_observe::reset();
}

#[doc(hidden)]
#[cfg(any(test, debug_assertions))]
pub fn lane_observation_count_for_test() -> usize {
    lane_observe::count()
}

#[cfg(any(test, debug_assertions))]
mod lane_observe {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::OnceLock;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    use databend_common_exception::ErrorCode;
    use databend_common_exception::Result;

    static NEXT_LANE_ID: AtomicU64 = AtomicU64::new(1);
    static OBSERVATIONS: OnceLock<Mutex<HashMap<(String, Vec<u8>), (String, u64)>>> =
        OnceLock::new();

    fn observations() -> &'static Mutex<HashMap<(String, Vec<u8>), (String, u64)>> {
        OBSERVATIONS.get_or_init(|| Mutex::new(HashMap::new()))
    }

    pub fn next_lane_id() -> u64 {
        NEXT_LANE_ID.fetch_add(1, Ordering::SeqCst)
    }

    pub fn observe_route_lane(
        query_id: &str,
        route_key: &[u8],
        executor_id: &str,
        lane_id: u64,
    ) -> Result<()> {
        let mut map = observations().lock().map_err(|_| {
            ErrorCode::Internal("Paimon lane observation lock poisoned".to_string())
        })?;
        let key = (query_id.to_string(), route_key.to_vec());
        match map.get(&key) {
            Some((prev_executor, prev_lane))
                if prev_executor.as_str() != executor_id || *prev_lane != lane_id =>
            {
                Err(ErrorCode::Internal(format!(
                    "Paimon route key mapped to multiple writer lanes in one query: \
                     query_id={query_id}, executor={prev_executor}/{executor_id}, \
                     lane={prev_lane}/{lane_id}"
                )))
            }
            Some(_) => Ok(()),
            None => {
                map.insert(key, (executor_id.to_string(), lane_id));
                Ok(())
            }
        }
    }

    pub fn reset() {
        if let Ok(mut map) = observations().lock() {
            map.clear();
        }
        NEXT_LANE_ID.store(1, Ordering::SeqCst);
    }

    pub fn count() -> usize {
        observations()
            .lock()
            .map(|map| map.len())
            .unwrap_or(0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PaimonWriteRoute {
    pub partition: Vec<u8>,
    pub bucket: i32,
    pub key: Vec<u8>,
}

pub struct PaimonWriteRouter {
    fields: Vec<paimon::spec::DataField>,
    partition_indices: Vec<usize>,
    bucket_key_indices: Vec<usize>,
    bucket_count: i32,
}

impl PaimonWriteRouter {
    pub fn try_create(schema: &TableSchema) -> Result<Self> {
        let core_options = CoreOptions::new(schema.options());
        let bucket_count = core_options.bucket();
        let partition_keys = schema.partition_keys();
        let bucket_keys = schema.bucket_keys();
        let config_hint = format!(
            "bucket={bucket_count}, partition_keys={partition_keys:?}, bucket_keys={bucket_keys:?}, primary_keys={:?}",
            schema.primary_keys()
        );

        if schema.primary_keys().is_empty() {
            return Err(ErrorCode::BadArguments(format!(
                "PaimonWriteRouter requires a primary-key table ({config_hint})"
            )));
        }
        if bucket_count < 1 {
            return Err(ErrorCode::BadArguments(format!(
                "PaimonWriteRouter requires fixed bucket >= 1 ({config_hint})"
            )));
        }
        if bucket_keys.is_empty() {
            return Err(ErrorCode::BadArguments(format!(
                "PaimonWriteRouter requires non-empty bucket keys ({config_hint})"
            )));
        }

        let fields = schema.fields().to_vec();
        let partition_indices = resolve_field_indices(&fields, partition_keys, "partition", &config_hint)?;
        let bucket_key_indices =
            resolve_field_indices(&fields, &bucket_keys, "bucket key", &config_hint)?;

        Ok(Self {
            fields,
            partition_indices,
            bucket_key_indices,
            bucket_count,
        })
    }

    pub fn route_batch(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> Result<Vec<PaimonWriteRoute>> {
        let partitions = paimon::spec::batch_to_serialized_bytes(
            batch,
            &self.partition_indices,
            &self.fields,
        )
        .map_err(map_paimon_error)?;
        let hashes =
            paimon::spec::batch_hash_codes(batch, &self.bucket_key_indices, &self.fields)
                .map_err(map_paimon_error)?;
        Ok(partitions
            .into_iter()
            .zip(hashes)
            .map(|(partition, hash)| {
                let bucket = (hash % self.bucket_count).wrapping_abs();
                let key = encode_route_key(&partition, bucket);
                PaimonWriteRoute {
                    partition,
                    bucket,
                    key,
                }
            })
            .collect())
    }
}

fn resolve_field_indices(
    fields: &[paimon::spec::DataField],
    names: &[String],
    kind: &str,
    config_hint: &str,
) -> Result<Vec<usize>> {
    let mut indices = Vec::with_capacity(names.len());
    for name in names {
        match fields.iter().position(|f| f.name() == name) {
            Some(idx) => indices.push(idx),
            None => {
                return Err(ErrorCode::BadArguments(format!(
                    "PaimonWriteRouter cannot find {kind} field '{name}' ({config_hint})"
                )));
            }
        }
    }
    Ok(indices)
}
