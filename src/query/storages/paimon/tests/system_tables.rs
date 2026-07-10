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

use std::sync::Arc;

use common::TestWarehouse;
use common::filesystem_catalog;
use common::setup_metadata_table;
use common::setup_multi_split_append_table;
use databend_common_expression::Column;
use databend_common_expression::Value;
use databend_common_expression::types::NumberColumn;
use databend_common_storages_paimon::PaimonSystemTableKind;
use databend_common_storages_paimon::read_system_table;
use paimon::Catalog;

#[tokio::test]
async fn simple_metadata() {
    let warehouse = TestWarehouse::new();
    let identifier = setup_metadata_table(&warehouse.warehouse).await;
    let catalog = Arc::new(filesystem_catalog(&warehouse.warehouse));
    let table = catalog.get_table(&identifier).await.expect("get table");

    for (kind, expected_rows) in [
        (PaimonSystemTableKind::Options, 1),
        (PaimonSystemTableKind::Snapshots, 2),
        (PaimonSystemTableKind::Schemas, 1),
        (PaimonSystemTableKind::Branches, 1),
        (PaimonSystemTableKind::Tags, 1),
    ] {
        let block = read_system_table(kind, catalog.clone(), identifier.clone(), table.clone())
            .await
            .unwrap_or_else(|err| panic!("read {kind:?}: {err}"));
        assert_eq!(block.num_columns(), kind.schema().num_fields(), "{kind:?}");
        assert_eq!(block.num_rows(), expected_rows, "{kind:?}");
    }
}

#[tokio::test]
async fn files_manifests_partitions() {
    let warehouse = TestWarehouse::new();
    let identifier = setup_multi_split_append_table(&warehouse.warehouse).await;
    let catalog = Arc::new(filesystem_catalog(&warehouse.warehouse));
    let table = catalog.get_table(&identifier).await.expect("get table");

    let files = read_system_table(
        PaimonSystemTableKind::Files,
        catalog.clone(),
        identifier.clone(),
        table.clone(),
    )
    .await
    .expect("read files");
    assert_eq!(
        files.num_columns(),
        PaimonSystemTableKind::Files.schema().num_fields()
    );
    let record_count: i64 = match files.get_by_offset(6).value() {
        Value::Column(Column::Number(NumberColumn::Int64(values))) => values.iter().sum(),
        value => panic!("unexpected record_count column: {value:?}"),
    };
    assert_eq!(record_count, 4);

    let manifests = read_system_table(
        PaimonSystemTableKind::Manifests,
        catalog.clone(),
        identifier.clone(),
        table.clone(),
    )
    .await
    .expect("read manifests");
    assert_eq!(
        manifests.num_columns(),
        PaimonSystemTableKind::Manifests.schema().num_fields()
    );
    assert!(manifests.num_rows() > 0);

    let expected_partitions = catalog
        .list_partitions(&identifier)
        .await
        .expect("list partitions");
    let partitions = read_system_table(
        PaimonSystemTableKind::Partitions,
        catalog,
        identifier,
        table,
    )
    .await
    .expect("read partitions");
    assert_eq!(
        partitions.num_columns(),
        PaimonSystemTableKind::Partitions.schema().num_fields()
    );
    assert_eq!(partitions.num_rows(), expected_partitions.len());
    assert_eq!(partitions.num_rows(), 4);
}
