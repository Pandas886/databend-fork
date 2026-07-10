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

use common::{
    databend_table, part_infos, read_rows_via_paimon, setup_append_table, setup_pk_table,
    TestWarehouse,
};
use databend_common_storages_paimon::PaimonTable;

#[tokio::test]
async fn test_append_table_rows_and_partitions() {
    let wh = TestWarehouse::new();
    let identifier = setup_append_table(&wh.warehouse).await;
    let rows = read_rows_via_paimon(&wh.warehouse, &identifier).await;
    assert_eq!(
        rows,
        vec![
            (1, "a".to_string()),
            (2, "b".to_string()),
            (3, "c".to_string()),
        ]
    );

    let table = databend_table(&wh.warehouse, &identifier).await;
    let paimon_table = table.as_any().downcast_ref::<PaimonTable>().expect("paimon");
    let (_, partitions) = paimon_table
        .plan_partitions_for_test(None)
        .await
        .expect("read partitions");
    assert!(!part_infos(&partitions).is_empty());
}

#[tokio::test]
async fn test_pk_table_deduplicates_and_splits() {
    let wh = TestWarehouse::new();
    let identifier = setup_pk_table(&wh.warehouse).await;
    let rows = read_rows_via_paimon(&wh.warehouse, &identifier).await;
    assert_eq!(rows, vec![(1, "new".to_string())]);

    let table = databend_table(&wh.warehouse, &identifier).await;
    let paimon_table = table.as_any().downcast_ref::<PaimonTable>().expect("paimon");
    let (_, partitions) = paimon_table
        .plan_partitions_for_test(None)
        .await
        .expect("read partitions");
    let parts = part_infos(&partitions);
    assert!(!parts.is_empty());
    assert!(
        parts.iter().any(|part| !part.split.data_files.is_empty()),
        "expected data files in split"
    );
}
