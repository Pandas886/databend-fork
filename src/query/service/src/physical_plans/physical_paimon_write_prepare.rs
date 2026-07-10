// Copyright 2021 Datafuse Labs
// Licensed under the Apache License, Version 2.0.

use std::any::Any;

use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_pipeline_transforms::blocks::TransformCastSchema;
use databend_common_sql::ColumnBinding;

use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PaimonWritePrepare {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub table_info: TableInfo,
    pub insert_schema: DataSchemaRef,
    pub select_schema: DataSchemaRef,
    pub select_column_bindings: Vec<ColumnBinding>,
    pub cast_needed: bool,
}

#[typetag::serde]
impl IPhysicalPlan for PaimonWritePrepare {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }
    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }
    fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.insert_schema.clone())
    }
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }
    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }
    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        PhysicalPlan::new(Self {
            input: children.remove(0),
            ..self.clone()
        })
    }
    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;
        PipelineBuilder::build_result_projection(
            &builder.func_ctx,
            self.input.output_schema()?,
            &self.select_column_bindings,
            &mut builder.main_pipeline,
            false,
        )?;
        if self.cast_needed {
            builder.main_pipeline.try_add_transformer(|| {
                TransformCastSchema::try_new(
                    self.select_schema.clone(),
                    self.insert_schema.clone(),
                    builder.func_ctx.clone(),
                )
            })?;
        }
        let table = builder
            .ctx
            .build_table_by_table_info(&self.table_info, None)?;
        PipelineBuilder::fill_and_reorder_columns(
            builder.ctx.clone(),
            &mut builder.main_pipeline,
            table,
            self.insert_schema.clone(),
        )
    }
}
