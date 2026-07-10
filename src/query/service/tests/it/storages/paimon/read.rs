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

use common::TestWarehouse;
use common::create_paimon_catalog_sql;
use common::databend_table;
use common::drop_paimon_catalog_sql;
use common::part_infos;
use common::setup_append_table;
use common::setup_multi_split_append_table;
use common::setup_paimon_fixture;
use common::setup_pk_table;
use databend_common_catalog::table::Table;
use databend_common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use databend_common_storages_paimon::PaimonTable;
use databend_common_storages_paimon::apply_pushdowns;
use databend_common_storages_paimon::can_push_limit;
use databend_common_storages_paimon::reset_table_load_count_for_test;
use databend_common_storages_paimon::table_load_count_for_test;
use databend_query::sessions::TableContextSettings;
use futures::StreamExt;
use paimon::Catalog;
use pipeline::collect_id_name_rows;
use pipeline::projection_indices;
use pipeline::pushdown_residual_only_limit;
use pipeline::read_blocks_via_pipeline;
use pipeline::read_blocks_via_pipeline_with_ctx;
use pipeline::total_rows;

use super::common;
use super::pipeline;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_append_table_via_pipeline() -> databend_common_exception::Result<()> {
    let wh = TestWarehouse::new();
    let identifier = setup_append_table(&wh.warehouse).await;
    let table = databend_table(&wh.warehouse, &identifier).await;
    let blocks = read_blocks_via_pipeline(table.clone(), None).await?;
    let rows = collect_id_name_rows(&blocks);
    assert_eq!(rows, vec![
        (1, "a".to_string()),
        (2, "b".to_string()),
        (3, "c".to_string()),
    ]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_pk_table_deduplicates_via_pipeline() -> databend_common_exception::Result<()> {
    let wh = TestWarehouse::new();
    let identifier = setup_pk_table(&wh.warehouse).await;
    let table = databend_table(&wh.warehouse, &identifier).await;
    let blocks = read_blocks_via_pipeline(table.clone(), None).await?;
    let rows = collect_id_name_rows(&blocks);
    assert_eq!(rows, vec![(1, "new".to_string())]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_projection_and_zero_column_via_pipeline() -> databend_common_exception::Result<()> {
    let wh = TestWarehouse::new();
    let identifier = setup_append_table(&wh.warehouse).await;
    let table = databend_table(&wh.warehouse, &identifier).await;
    let blocks = read_blocks_via_pipeline(table.clone(), Some(projection_indices(vec![0]))).await?;
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].num_columns(), 1);
    assert_eq!(blocks[0].num_rows(), 3);

    let zero_blocks = read_blocks_via_pipeline(table, Some(projection_indices(vec![]))).await?;
    assert_eq!(total_rows(&zero_blocks), 3);
    assert!(zero_blocks.iter().all(|block| block.num_columns() == 0));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_no_filter_limit_one_row_via_sql() -> databend_common_exception::Result<()> {
    let wh = TestWarehouse::new();
    let _ = setup_append_table(&wh.warehouse).await;
    let fixture = setup_paimon_fixture().await?;
    fixture.execute_command(drop_paimon_catalog_sql()).await?;
    fixture
        .execute_command(&create_paimon_catalog_sql(&wh.warehouse))
        .await?;
    fixture.execute_command("USE CATALOG paimon_it").await?;
    let sql = "SELECT id, name FROM db.append_t ORDER BY id LIMIT 1";
    let mut stream = fixture.execute_query(sql).await?;
    let block = stream.next().await.transpose()?.expect("result block");
    assert_eq!(block.num_rows(), 1);
    assert_eq!(collect_id_name_rows(&[block]).len(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_no_filter_limit_returns_one_row_via_paimon_source()
-> databend_common_exception::Result<()> {
    let wh = TestWarehouse::new();
    let identifier = setup_append_table(&wh.warehouse).await;
    let table = databend_table(&wh.warehouse, &identifier).await;
    let paimon_table = table
        .as_any()
        .downcast_ref::<PaimonTable>()
        .expect("paimon table");
    let fixture = setup_paimon_fixture().await?;
    let ctx = fixture.new_query_ctx().await?;
    let mut pushdown = projection_indices(vec![0, 1]);
    pushdown.limit = Some(1);
    let (stats, _) = paimon_table
        .read_partitions(ctx.clone(), Some(pushdown), false)
        .await?;
    assert_eq!(
        stats.read_rows, 1,
        "LIMIT pushdown should cap planned read rows to one"
    );
    let mut limit_pushdown = projection_indices(vec![0, 1]);
    limit_pushdown.limit = Some(1);
    let limited_blocks =
        read_blocks_via_pipeline_with_ctx(table, Some(limit_pushdown), Some(1), Some(ctx)).await?;
    assert_eq!(
        total_rows(&limited_blocks),
        1,
        "PaimonTableSource with LIMIT pushdown must emit one row"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_limit_pushdown_matrix() -> databend_common_exception::Result<()> {
    let wh = TestWarehouse::new();
    let identifier = setup_append_table(&wh.warehouse).await;
    let catalog = common::filesystem_catalog(&wh.warehouse);
    let inner = catalog.get_table(&identifier).await.expect("paimon table");
    let table = databend_table(&wh.warehouse, &identifier).await;

    let mut no_filter = projection_indices(vec![]);
    no_filter.limit = Some(1);
    let (read_builder, analysis) =
        apply_pushdowns(&inner, Some(&no_filter), table.schema().as_ref());
    assert!(can_push_limit(Some(1), &analysis, &read_builder));

    let residual = pushdown_residual_only_limit(1);
    let (read_builder, analysis) =
        apply_pushdowns(&inner, Some(&residual), table.schema().as_ref());
    assert!(!can_push_limit(Some(1), &analysis, &read_builder));
    let blocks = read_blocks_via_pipeline(table.clone(), Some(residual)).await?;
    assert_eq!(
        total_rows(&blocks),
        3,
        "residual filter must prevent storage LIMIT pushdown"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_residual_filter_and_limit_via_sql() -> databend_common_exception::Result<()> {
    let wh = TestWarehouse::new();
    let _ = setup_append_table(&wh.warehouse).await;
    let fixture = setup_paimon_fixture().await?;
    fixture.execute_command(drop_paimon_catalog_sql()).await?;
    fixture
        .execute_command(&create_paimon_catalog_sql(&wh.warehouse))
        .await?;
    fixture.execute_command("USE CATALOG paimon_it").await?;
    let sql = "SELECT id, name FROM db.append_t WHERE trim(name) = 'c' ORDER BY id LIMIT 1";
    let mut stream = fixture.execute_query(sql).await?;
    let block = stream.next().await.transpose()?.expect("result block");
    assert_eq!(block.num_rows(), 1);
    let rows = collect_id_name_rows(&[block]);
    assert_eq!(rows, vec![(3, "c".to_string())]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_source_loads_table_once_for_multiple_splits() -> databend_common_exception::Result<()>
{
    let wh = TestWarehouse::new();
    let identifier = setup_multi_split_append_table(&wh.warehouse).await;
    let table = databend_table(&wh.warehouse, &identifier).await;
    let paimon_table = table
        .as_any()
        .downcast_ref::<PaimonTable>()
        .expect("paimon table");
    let fixture = setup_paimon_fixture().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_current_session()
        .get_settings()
        .set_max_threads(1)?;
    assert_eq!(ctx.get_settings().get_max_threads()?, 1);
    let (_, partitions) = paimon_table
        .read_partitions(ctx.clone(), None, false)
        .await?;
    assert!(
        part_infos(&partitions).len() >= 4,
        "expected at least four splits"
    );
    let plan = table
        .read_plan(ctx.clone(), None, None, false, false)
        .await?;
    let num_sources = plan
        .parts
        .len()
        .max(1)
        .min(ctx.get_settings().get_max_threads()? as usize);
    assert_eq!(
        num_sources, 1,
        "single-threaded scan must use one PaimonTableSource"
    );

    reset_table_load_count_for_test();
    let blocks = read_blocks_via_pipeline_with_ctx(table, None, Some(1), Some(ctx.clone())).await?;
    assert!(total_rows(&blocks) >= 4);
    assert_eq!(
        table_load_count_for_test(),
        1,
        "one source worker must load the table once across multiple splits"
    );
    Ok(())
}
