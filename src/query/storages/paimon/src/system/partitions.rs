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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;

use super::format_partition;
use super::partition_fields;
use crate::error::map_paimon_result;

#[derive(Default)]
struct Aggregate {
    record_count: i64,
    file_size: i64,
    file_count: i64,
    last_update_time: Option<i64>,
    total_buckets: i32,
}

pub async fn read(table: &paimon::Table) -> Result<DataBlock> {
    let read_builder = table.new_read_builder();
    let plan = map_paimon_result(read_builder.new_scan().plan().await)?;
    let part_fields = partition_fields(table)?;

    // Aggregate per partition, preserving first-seen order for determinism.
    let mut groups: Vec<(Option<String>, Aggregate)> = Vec::new();
    for split in plan.splits() {
        let partition = format_partition(split.partition(), &part_fields)?;
        let index = match groups.iter().position(|(key, _)| key == &partition) {
            Some(index) => index,
            None => {
                groups.push((partition, Aggregate::default()));
                groups.len() - 1
            }
        };
        let agg = &mut groups[index].1;
        agg.total_buckets = split.total_buckets();
        for file in split.data_files() {
            agg.record_count += file.row_count;
            agg.file_size += file.file_size;
            agg.file_count += 1;
            if let Some(time) = file.creation_time.map(|time| time.timestamp_micros()) {
                agg.last_update_time = Some(
                    agg.last_update_time
                        .map_or(time, |current| current.max(time)),
                );
            }
        }
    }

    let rows = groups.len();
    Ok(DataBlock::new_from_columns(vec![
        StringType::from_opt_data(groups.iter().map(|(key, _)| key.clone()).collect()),
        Int64Type::from_data(groups.iter().map(|(_, agg)| agg.record_count).collect()),
        Int64Type::from_data(groups.iter().map(|(_, agg)| agg.file_size).collect()),
        Int64Type::from_data(groups.iter().map(|(_, agg)| agg.file_count).collect()),
        TimestampType::from_opt_data(groups.iter().map(|(_, agg)| agg.last_update_time).collect()),
        // created_at / created_by / updated_by / options are not tracked by the
        // filesystem scan; expose them as null placeholders.
        TimestampType::from_opt_data(vec![None::<i64>; rows]),
        StringType::from_opt_data(vec![None::<String>; rows]),
        StringType::from_opt_data(vec![None::<String>; rows]),
        StringType::from_opt_data(vec![None::<String>; rows]),
        Int32Type::from_data(groups.iter().map(|(_, agg)| agg.total_buckets).collect()),
        BooleanType::from_data(vec![true; rows]),
    ]))
}
