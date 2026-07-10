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
