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

mod common;

use common::TestWarehouse;
use common::databend_table;
use common::filesystem_catalog;
use common::part_infos;
use common::pipeline::collect_id_name_rows;
use common::pipeline::projection_indices;
use common::pipeline::pushdown_residual_only_limit;
use common::pipeline::read_blocks_via_pipeline;
use common::pipeline::total_rows;
use common::setup_append_table;
use common::setup_pk_table;
use databend_common_storages_paimon::PaimonTable;
use databend_common_storages_paimon::apply_pushdowns;
use databend_common_storages_paimon::can_push_limit;
use paimon::Catalog;

#[tokio::test]
async fn test_append_table_via_pipeline() {
    let wh = TestWarehouse::new();
    let identifier = setup_append_table(&wh.warehouse).await;
    let table = databend_table(&wh.warehouse, &identifier).await;
    let blocks = read_blocks_via_pipeline(table.clone(), None)
        .await
        .expect("pipeline read");
    let rows = collect_id_name_rows(&blocks);
    assert_eq!(rows, vec![
        (1, "a".to_string()),
        (2, "b".to_string()),
        (3, "c".to_string()),
    ]);
}

#[tokio::test]
async fn test_pk_table_deduplicates_via_pipeline() {
    let wh = TestWarehouse::new();
    let identifier = setup_pk_table(&wh.warehouse).await;
    let table = databend_table(&wh.warehouse, &identifier).await;
    let blocks = read_blocks_via_pipeline(table.clone(), None)
        .await
        .expect("pipeline read");
    let rows = collect_id_name_rows(&blocks);
    assert_eq!(rows, vec![(1, "new".to_string())]);
}

#[tokio::test]
async fn test_projection_and_zero_column_via_pipeline() {
    let wh = TestWarehouse::new();
    let identifier = setup_append_table(&wh.warehouse).await;
    let table = databend_table(&wh.warehouse, &identifier).await;
    let blocks = read_blocks_via_pipeline(table.clone(), Some(projection_indices(vec![0])))
        .await
        .expect("projection read");
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].num_columns(), 1);
    assert_eq!(blocks[0].num_rows(), 3);

    let zero_blocks = read_blocks_via_pipeline(table, Some(projection_indices(vec![])))
        .await
        .expect("zero projection read");
    assert_eq!(total_rows(&zero_blocks), 3);
    assert!(zero_blocks.iter().all(|block| block.num_columns() == 0));
}

#[tokio::test]
async fn test_residual_filter_blocks_limit_pushdown_via_pipeline() {
    let wh = TestWarehouse::new();
    let identifier = setup_append_table(&wh.warehouse).await;
    let table = databend_table(&wh.warehouse, &identifier).await;
    let blocks = read_blocks_via_pipeline(table, Some(pushdown_residual_only_limit(1)))
        .await
        .expect("residual limit read");
    assert_eq!(
        total_rows(&blocks),
        3,
        "residual filter must prevent Paimon LIMIT pushdown"
    );
}

#[tokio::test]
async fn test_no_filter_limit_pushdown_in_read_path() {
    let wh = TestWarehouse::new();
    let identifier = setup_append_table(&wh.warehouse).await;
    let catalog = filesystem_catalog(&wh.warehouse);
    let inner = catalog.get_table(&identifier).await.expect("paimon table");
    let table = databend_table(&wh.warehouse, &identifier).await;
    let mut pushdown = projection_indices(vec![]);
    pushdown.limit = Some(1);
    let (read_builder, analysis) =
        apply_pushdowns(&inner, Some(&pushdown), table.schema().as_ref());
    assert!(can_push_limit(Some(1), &analysis, &read_builder));
    let plan = read_builder
        .new_scan()
        .plan()
        .await
        .expect("scan plan with limit");
    assert!(!plan.splits().is_empty());
}

#[tokio::test]
async fn test_pk_split_keeps_merge_files_atomic() {
    let wh = TestWarehouse::new();
    let identifier = setup_pk_table(&wh.warehouse).await;
    let table = databend_table(&wh.warehouse, &identifier).await;
    let paimon_table = table
        .as_any()
        .downcast_ref::<PaimonTable>()
        .expect("paimon");
    let (_, partitions) = paimon_table
        .plan_partitions_for_test(None)
        .await
        .expect("read partitions");
    let parts = part_infos(&partitions);
    assert!(!parts.is_empty());
    let split = &parts[0].split;
    assert!(
        !split.data_files.is_empty(),
        "PK split must retain data files"
    );
    assert!(
        split.data_files.len() >= 2
            || split
                .deletion_files
                .as_ref()
                .is_some_and(|files| files.iter().any(Option::is_some)),
        "PK merge split should retain multiple data files or deletion files"
    );
}
