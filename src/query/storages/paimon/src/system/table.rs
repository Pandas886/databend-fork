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
use std::sync::Arc;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::DistributionLevel;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_pipeline::sources::EmptySource;
use paimon::catalog::Identifier;
use serde::Deserialize;
use serde::Serialize;

use super::PaimonSystemTableKind;
use super::read_system_table;
use crate::PAIMON_ENGINE;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaimonSystemPartInfo {
    pub kind: PaimonSystemTableKind,
}

#[typetag::serde(name = "paimon_system")]
impl PartInfo for PaimonSystemPartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, other: &Box<dyn PartInfo>) -> bool {
        other.as_any().downcast_ref::<Self>() == Some(self)
    }

    fn hash(&self) -> u64 {
        self.kind as u64
    }
}

pub struct PaimonSystemTable {
    info: TableInfo,
    kind: PaimonSystemTableKind,
    catalog: Arc<dyn paimon::Catalog>,
    identifier: Identifier,
    table: paimon::Table,
}

impl PaimonSystemTable {
    pub fn create(
        catalog_info: Arc<CatalogInfo>,
        name: String,
        kind: PaimonSystemTableKind,
        catalog: Arc<dyn paimon::Catalog>,
        identifier: Identifier,
        table: paimon::Table,
    ) -> Arc<dyn Table> {
        let info = TableInfo {
            ident: TableIdent::new(0, 0),
            desc: format!(
                "'{}'.'{}'.'{}'",
                catalog_info.name_ident.catalog_name,
                identifier.database(),
                name
            ),
            name,
            catalog_info,
            meta: TableMeta {
                schema: kind.schema(),
                engine: PAIMON_ENGINE.to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        Arc::new(Self {
            info,
            kind,
            catalog,
            identifier,
            table,
        })
    }
}

#[async_trait::async_trait]
impl Table for PaimonSystemTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.info
    }

    fn distribution_level(&self) -> DistributionLevel {
        DistributionLevel::Cluster
    }

    fn is_read_only(&self) -> bool {
        true
    }

    fn support_column_projection(&self) -> bool {
        false
    }

    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((
            PartStatistics::new_exact(1, 0, 1, 1),
            Partitions::create(PartitionsShuffleKind::Seq, vec![Arc::new(Box::new(
                PaimonSystemPartInfo { kind: self.kind },
            ))]),
        ))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        if plan.parts.partitions.is_empty() {
            pipeline.add_source(EmptySource::create, 1)?;
            return Ok(());
        }
        pipeline.add_source(
            |output| {
                PaimonSystemSource::create(
                    ctx.clone(),
                    output,
                    self.kind,
                    self.catalog.clone(),
                    self.identifier.clone(),
                    self.table.clone(),
                )
            },
            1,
        )?;
        Ok(())
    }
}

struct PaimonSystemSource {
    finished: bool,
    kind: PaimonSystemTableKind,
    catalog: Arc<dyn paimon::Catalog>,
    identifier: Identifier,
    table: paimon::Table,
}

impl PaimonSystemSource {
    fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        kind: PaimonSystemTableKind,
        catalog: Arc<dyn paimon::Catalog>,
        identifier: Identifier,
        table: paimon::Table,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.get_scan_progress(), output, Self {
            finished: false,
            kind,
            catalog,
            identifier,
            table,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for PaimonSystemSource {
    const NAME: &'static str = "paimon_system";

    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finished {
            return Ok(None);
        }
        self.finished = true;
        read_system_table(
            self.kind,
            self.catalog.clone(),
            self.identifier.clone(),
            self.table.clone(),
        )
        .await
        .map(Some)
    }
}
