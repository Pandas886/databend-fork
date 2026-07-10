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
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::StringType;
use paimon::SnapshotManager;
use paimon::spec::ManifestFileMeta;
use paimon::spec::ManifestList;

use crate::error::map_paimon_result;

pub async fn read(table: &paimon::Table) -> Result<DataBlock> {
    let manager = SnapshotManager::new(table.file_io().clone(), table.location().to_string());
    let metas = match map_paimon_result(manager.get_latest_snapshot().await)? {
        Some(snapshot) => {
            let mut metas: Vec<ManifestFileMeta> = Vec::new();
            for list_name in [
                snapshot.base_manifest_list(),
                snapshot.delta_manifest_list(),
            ] {
                if list_name.is_empty() {
                    continue;
                }
                let path = manager.manifest_path(list_name);
                metas.extend(map_paimon_result(
                    ManifestList::read(table.file_io(), &path).await,
                )?);
            }
            metas
        }
        None => Vec::new(),
    };

    Ok(DataBlock::new_from_columns(vec![
        StringType::from_data(metas.iter().map(|m| m.file_name()).collect()),
        Int64Type::from_data(metas.iter().map(|m| m.file_size()).collect()),
        Int64Type::from_data(metas.iter().map(|m| m.num_added_files()).collect()),
        Int64Type::from_data(metas.iter().map(|m| m.num_deleted_files()).collect()),
        Int64Type::from_data(metas.iter().map(|m| m.schema_id()).collect()),
        // TODO: decode `partition_stats` min/max BinaryRow into typed JSON; left
        // null until the shared stats decoder lands.
        StringType::from_opt_data(metas.iter().map(|_| None::<String>).collect()),
        StringType::from_opt_data(metas.iter().map(|_| None::<String>).collect()),
        Int64Type::from_opt_data(metas.iter().map(|m| m.min_row_id()).collect()),
        Int64Type::from_opt_data(metas.iter().map(|m| m.max_row_id()).collect()),
    ]))
}
