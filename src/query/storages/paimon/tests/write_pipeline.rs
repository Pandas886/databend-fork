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

use arrow_array::Int32Array;
use arrow_array::RecordBatch;
use arrow_array::StringArray;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field as ArrowField;
use arrow_schema::Schema as ArrowSchema;
use chrono::DateTime;
use chrono::Utc;
use common::TestWarehouse;
use common::filesystem_catalog;
use databend_common_storages_paimon::PaimonCommitMeta;
use paimon::Catalog;
use paimon::catalog::Identifier;
use paimon::spec::DataFileMeta;
use paimon::spec::DataType;
use paimon::spec::IndexFileMeta;
use paimon::spec::IntType;
use paimon::spec::Schema;
use paimon::spec::VarCharType;
use paimon::table::CommitMessage;

#[test]
fn test_commit_meta_round_trip() {
    let message = fixture_commit_message();
    let meta = PaimonCommitMeta::try_from_messages(vec![message.clone()]).unwrap();
    let json = serde_json::to_string(&meta).unwrap();
    let decoded: PaimonCommitMeta = serde_json::from_str(&json).unwrap();
    let restored = decoded.into_messages().unwrap();
    assert_eq!(restored[0].partition, message.partition);
    assert_eq!(restored[0].bucket, message.bucket);
    assert_eq!(restored[0].new_files, message.new_files);
    assert_eq!(restored[0].deleted_files, message.deleted_files);
    assert_eq!(restored[0].new_index_files, message.new_index_files);
}

fn fixture_commit_message() -> CommitMessage {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let wh = TestWarehouse::new();
        let catalog = filesystem_catalog(&wh.warehouse);
        catalog
            .create_database("db", false, Default::default())
            .await
            .expect("create db");

        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::VarChar(VarCharType::string_type()))
            .primary_key(["id"])
            .option("bucket", "1")
            .build()
            .expect("schema");
        let identifier = Identifier::new("db", "commit_meta");
        catalog
            .create_table(&identifier, schema, false)
            .await
            .expect("create table");
        let table = catalog.get_table(&identifier).await.expect("get table");

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["a"])),
        ])
        .expect("record batch");

        let write_builder = table.new_write_builder();
        let mut table_write = write_builder.new_write().expect("table write");
        table_write
            .write_arrow_batch(&batch)
            .await
            .expect("write batch");
        let mut messages = table_write.prepare_commit().await.expect("prepare commit");
        assert!(!messages.is_empty(), "expected at least one commit message");

        let mut message = messages.remove(0);
        // DataFileMeta serde stores creation_time as epoch millis; truncate so the
        // fixture matches what a JSON round-trip can preserve.
        truncate_creation_time_to_millis(&mut message.new_files);
        // Exercise deleted_files / new_index_files round-trip with realistic nested metas.
        message.deleted_files = message.new_files.clone();
        message.new_index_files = vec![IndexFileMeta {
            index_type: "HASH".to_string(),
            file_name: "index-0".to_string(),
            file_size: 32,
            row_count: 1,
            deletion_vectors_ranges: None,
            global_index_meta: None,
        }];
        message
    })
}

fn truncate_creation_time_to_millis(files: &mut [DataFileMeta]) {
    for file in files {
        if let Some(ts) = file.creation_time {
            file.creation_time = DateTime::<Utc>::from_timestamp_millis(ts.timestamp_millis());
        }
    }
}
