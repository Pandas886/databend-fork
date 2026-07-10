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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_meta_app::schema::CatalogOption;
use databend_common_sql::Planner;
use databend_common_sql::plans::Plan;

use crate::framework::LiteTableContext;

async fn plan_sql(fixture: &Arc<LiteTableContext>, sql: &str) -> Result<Plan> {
    let mut planner = Planner::new(fixture.clone());
    let (plan, _) = planner.plan_sql(sql).await?;
    Ok(plan)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn paimon_catalog_rest() -> Result<()> {
    let fixture = LiteTableContext::create().await?;
    let sql = "CREATE CATALOG p TYPE = PAIMON CONNECTION = (METASTORE='rest', URI='http://127.0.0.1:8080', WAREHOUSE='demo')";
    let plan = plan_sql(&fixture, sql).await?;
    let Plan::CreateCatalog(plan) = plan else {
        panic!("expected CreateCatalog")
    };
    let CatalogOption::Paimon(option) = plan.meta.catalog_option else {
        panic!("expected paimon")
    };
    assert_eq!(option.options["metastore"], "rest");
    assert_eq!(option.options["uri"], "http://127.0.0.1:8080");
    assert_eq!(option.options["warehouse"], "demo");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn paimon_catalog_filesystem() -> Result<()> {
    let fixture = LiteTableContext::create().await?;
    let sql = "CREATE CATALOG p TYPE = PAIMON CONNECTION = (METASTORE='filesystem', WAREHOUSE='s3://bucket/warehouse')";
    let plan = plan_sql(&fixture, sql).await?;
    let Plan::CreateCatalog(plan) = plan else {
        panic!("expected CreateCatalog")
    };
    let CatalogOption::Paimon(option) = plan.meta.catalog_option else {
        panic!("expected paimon")
    };
    assert_eq!(option.options["metastore"], "filesystem");
    assert_eq!(option.options["warehouse"], "s3://bucket/warehouse");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn paimon_catalog_missing_warehouse() -> Result<()> {
    let fixture = LiteTableContext::create().await?;
    let sql = "CREATE CATALOG p TYPE = PAIMON CONNECTION = (METASTORE='filesystem')";
    let err = plan_sql(&fixture, sql).await.unwrap_err();
    assert!(err
        .message()
        .contains("warehouse for paimon catalog is not specified"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn paimon_catalog_rest_missing_uri() -> Result<()> {
    let fixture = LiteTableContext::create().await?;
    let sql = "CREATE CATALOG p TYPE = PAIMON CONNECTION = (METASTORE='rest', WAREHOUSE='demo')";
    let err = plan_sql(&fixture, sql).await.unwrap_err();
    assert!(err
        .message()
        .contains("uri for paimon rest catalog is not specified"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn paimon_catalog_invalid_metastore() -> Result<()> {
    let fixture = LiteTableContext::create().await?;
    let sql = "CREATE CATALOG p TYPE = PAIMON CONNECTION = (METASTORE='hive', WAREHOUSE='demo')";
    let err = plan_sql(&fixture, sql).await.unwrap_err();
    assert!(err
        .message()
        .contains("paimon catalog metastore hive is not supported"));
    Ok(())
}
