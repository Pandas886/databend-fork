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

use std::any::Any;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use databend_common_catalog::catalog::StorageDescription;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::DistributionLevel;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline::core::Pipeline;
use paimon::CatalogFactory;
use paimon::Options;
use paimon::catalog::Identifier;
use paimon::spec::TableSchema as PaimonTableSchema;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::OnceCell;

use crate::PaimonPartInfo;
use crate::SerializableDataSplit;
use crate::error::map_paimon_error;
use crate::error::map_paimon_result;
use crate::predicate::apply_pushdowns;
use crate::source::PaimonTableSource;
use crate::table::descriptor::PAIMON_TABLE_DESCRIPTOR_KEY;

pub const PAIMON_ENGINE: &str = "PAIMON";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaimonTableDescriptor {
    pub catalog_options: HashMap<String, String>,
    pub identifier: Identifier,
    pub location: String,
    pub schema: PaimonTableSchema,
}

pub struct PaimonTable {
    info: TableInfo,
    descriptor: PaimonTableDescriptor,
    table: OnceCell<paimon::Table>,
}

mod descriptor {
    pub const PAIMON_TABLE_DESCRIPTOR_KEY: &str = "paimon.table_descriptor";
}

impl PaimonTable {
    pub fn try_create(info: TableInfo) -> Result<Box<dyn Table>> {
        let descriptor = parse_descriptor(&info)?;
        Ok(Box::new(Self {
            info,
            descriptor,
            table: OnceCell::new(),
        }))
    }

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: PAIMON_ENGINE.to_string(),
            comment: "PAIMON Storage Engine".to_string(),
            support_cluster_key: false,
        }
    }

    pub fn from_paimon_table(
        catalog_info: Arc<CatalogInfo>,
        catalog_options: HashMap<String, String>,
        table: paimon::Table,
    ) -> Result<Arc<dyn Table>> {
        let descriptor = PaimonTableDescriptor {
            catalog_options,
            identifier: table.identifier().clone(),
            location: table.location().to_string(),
            schema: table.schema().clone(),
        };
        let databend_schema = paimon_schema_to_databend(table.schema())?;
        let descriptor_json = serde_json::to_string(&descriptor).map_err(|err| {
            ErrorCode::Internal(format!("serialize paimon table descriptor failed: {err:?}"))
        })?;
        let mut engine_options = BTreeMap::new();
        engine_options.insert(
            PAIMON_TABLE_DESCRIPTOR_KEY.to_string(),
            descriptor_json,
        );
        let info = TableInfo {
            ident: TableIdent::new(0, 0),
            desc: format!(
                "'{}'.'{}'.'{}'",
                catalog_info.name_ident.catalog_name,
                descriptor.identifier.database(),
                descriptor.identifier.object()
            ),
            name: descriptor.identifier.object().to_string(),
            catalog_info,
            meta: TableMeta {
                schema: Arc::new(databend_schema),
                engine: PAIMON_ENGINE.to_string(),
                engine_options,
                ..Default::default()
            },
            ..Default::default()
        };
        Ok(Arc::new(Self {
            info,
            descriptor,
            table: OnceCell::new(),
        }))
    }

    fn descriptor(&self) -> &PaimonTableDescriptor {
        &self.descriptor
    }

    async fn loaded_table(&self) -> Result<&paimon::Table> {
        self.table
            .get_or_try_init(|| async {
                let options = options_from_map(&self.descriptor.catalog_options)?;
                let catalog = map_paimon_result(CatalogFactory::create(options).await)?;
                let loaded = map_paimon_result(
                    catalog
                        .get_table(&self.descriptor.identifier)
                        .await,
                )?;
                Ok(paimon::Table::new(
                    loaded.file_io().clone(),
                    self.descriptor.identifier.clone(),
                    self.descriptor.location.clone(),
                    self.descriptor.schema.clone(),
                    None,
                ))
            })
            .await
    }

    #[async_backtrace::framed]
    async fn do_read_partitions(
        &self,
        push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let table = self.loaded_table().await?;
        let (read_builder, _analysis) =
            apply_pushdowns(table, push_downs.as_ref(), self.schema().as_ref());
        let plan = map_paimon_result(read_builder.new_scan().plan().await)?;
        let mut read_rows = 0usize;
        let mut read_bytes = 0usize;
        let parts: Vec<PartInfoPtr> = plan
            .splits()
            .iter()
            .map(|split| {
                read_rows += split
                    .merged_row_count()
                    .unwrap_or_else(|| split.row_count()) as usize;
                read_bytes += split
                    .data_files()
                    .iter()
                    .map(|file| file.file_size as usize)
                    .sum::<usize>();
                let part: PartInfoPtr = Arc::new(Box::new(PaimonPartInfo {
                    split: SerializableDataSplit::from(split),
                }));
                part
            })
            .collect();
        Ok((
            PartStatistics::new_exact(read_rows, read_bytes, parts.len(), parts.len()),
            Partitions::create(PartitionsShuffleKind::Mod, parts),
        ))
    }

    pub async fn plan_partitions_for_test(
        &self,
        push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        self.do_read_partitions(push_downs).await
    }

    pub fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let parts_len = plan.parts.len();
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let num_sources = parts_len.max(1).min(max_threads);
        ctx.set_partitions(plan.parts.clone())?;
        pipeline.add_source(
            |output| PaimonTableSource::create(ctx.clone(), output, plan.clone()),
            num_sources,
        )?;
        Ok(())
    }
}

pub(crate) fn parse_descriptor_from_plan(plan: &DataSourcePlan) -> Result<PaimonTableDescriptor> {
    let table_info = match &plan.source_info {
        DataSourceInfo::TableSource(table_info) => table_info,
        _ => {
            return Err(ErrorCode::Internal(
                "paimon read plan must use table source".to_string(),
            ));
        }
    };
    let raw = table_info
        .meta
        .engine_options
        .get(PAIMON_TABLE_DESCRIPTOR_KEY)
        .ok_or_else(|| {
            ErrorCode::Internal("missing paimon table descriptor in engine options".to_string())
        })?;
    serde_json::from_str(raw).map_err(|err| {
        ErrorCode::Internal(format!("deserialize paimon table descriptor failed: {err:?}"))
    })
}

fn parse_descriptor(info: &TableInfo) -> Result<PaimonTableDescriptor> {
    let raw = info
        .meta
        .engine_options
        .get(PAIMON_TABLE_DESCRIPTOR_KEY)
        .ok_or_else(|| {
            ErrorCode::Internal("missing paimon table descriptor in engine options".to_string())
        })?;
    serde_json::from_str(raw).map_err(|err| {
        ErrorCode::Internal(format!("deserialize paimon table descriptor failed: {err:?}"))
    })
}

fn options_from_map(options: &HashMap<String, String>) -> Result<Options> {
    let mut paimon_options = Options::new();
    for (key, value) in options {
        paimon_options.set(key, value.clone());
    }
    Ok(paimon_options)
}

pub(crate) fn paimon_schema_to_databend(schema: &PaimonTableSchema) -> Result<TableSchema> {
    let arrow_schema = paimon::arrow::build_target_arrow_schema(schema.fields())
        .map_err(map_paimon_error)?;
    TableSchema::try_from(arrow_schema.as_ref()).map_err(ErrorCode::from_std_error)
}

#[async_trait::async_trait]
impl Table for PaimonTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn distribution_level(&self) -> DistributionLevel {
        DistributionLevel::Cluster
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.info
    }

    fn name(&self) -> &str {
        &self.get_table_info().name
    }

    fn is_read_only(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        self.do_read_partitions(push_downs).await
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        self.do_read_data(ctx, plan, pipeline)
    }

    fn table_args(&self) -> Option<TableArgs> {
        None
    }

    fn support_column_projection(&self) -> bool {
        true
    }
}
