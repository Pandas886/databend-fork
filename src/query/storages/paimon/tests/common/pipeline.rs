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

#![allow(dead_code)]

use std::sync::Arc;

use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionID;
use databend_common_expression::RemoteExpr as Expr;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::PipelinePullingExecutor;
use databend_query::test_kits::TestFixture;

pub fn pushdown_eq_id(value: i32) -> PushDownInfo {
    let filter = Expr::FunctionCall {
        span: None,
        id: Box::new(FunctionID::Builtin {
            name: "eq".to_string(),
            id: 0,
        }),
        generics: vec![],
        args: vec![
            Expr::ColumnRef {
                span: None,
                id: "id".to_string(),
                data_type: DataType::Number(
                    databend_common_expression::types::NumberDataType::Int32,
                ),
                display_name: "id".to_string(),
            },
            Expr::Constant {
                span: None,
                scalar: Scalar::Number(NumberScalar::Int32(value)),
                data_type: DataType::Number(
                    databend_common_expression::types::NumberDataType::Int32,
                ),
            },
        ],
        return_type: DataType::Boolean,
    };
    PushDownInfo {
        filters: Some(Filters {
            filter,
            inverted_filter: Expr::Constant {
                span: None,
                scalar: Scalar::Boolean(false),
                data_type: DataType::Boolean,
            },
        }),
        is_deterministic: true,
        ..Default::default()
    }
}

pub fn pushdown_with_limit(limit: usize, residual: bool) -> PushDownInfo {
    let mut pushdown = if residual {
        let mut info = pushdown_eq_id(1);
        let not_filter = Expr::FunctionCall {
            span: None,
            id: Box::new(FunctionID::Builtin {
                name: "not".to_string(),
                id: 0,
            }),
            generics: vec![],
            args: vec![Expr::FunctionCall {
                span: None,
                id: Box::new(FunctionID::Builtin {
                    name: "eq".to_string(),
                    id: 0,
                }),
                generics: vec![],
                args: vec![
                    Expr::ColumnRef {
                        span: None,
                        id: "name".to_string(),
                        data_type: DataType::String,
                        display_name: "name".to_string(),
                    },
                    Expr::Constant {
                        span: None,
                        scalar: Scalar::String("x".to_string()),
                        data_type: DataType::String,
                    },
                ],
                return_type: DataType::Boolean,
            }],
            return_type: DataType::Boolean,
        };
        info.filters = Some(Filters {
            filter: Expr::FunctionCall {
                span: None,
                id: Box::new(FunctionID::Builtin {
                    name: "and".to_string(),
                    id: 0,
                }),
                generics: vec![],
                args: vec![info.filters.unwrap().filter, not_filter],
                return_type: DataType::Boolean,
            },
            inverted_filter: Expr::Constant {
                span: None,
                scalar: Scalar::Boolean(false),
                data_type: DataType::Boolean,
            },
        });
        info
    } else {
        pushdown_eq_id(1)
    };
    pushdown.limit = Some(limit);
    pushdown
}

pub fn projection_indices(indices: Vec<usize>) -> PushDownInfo {
    PushDownInfo {
        projection: Some(Projection::Columns(indices)),
        is_deterministic: true,
        ..Default::default()
    }
}

pub fn pushdown_residual_only_limit(limit: usize) -> PushDownInfo {
    let mut pushdown = projection_indices(vec![]);
    pushdown.filters = Some(Filters {
        filter: Expr::FunctionCall {
            span: None,
            id: Box::new(FunctionID::Builtin {
                name: "not".to_string(),
                id: 0,
            }),
            generics: vec![],
            args: vec![function("eq", vec![
                string_column("name"),
                string_literal("missing"),
            ])],
            return_type: DataType::Boolean,
        },
        inverted_filter: Expr::Constant {
            span: None,
            scalar: Scalar::Boolean(false),
            data_type: DataType::Boolean,
        },
    });
    pushdown.limit = Some(limit);
    pushdown
}

fn string_column(name: &str) -> Expr<String> {
    Expr::ColumnRef {
        span: None,
        id: name.to_string(),
        data_type: DataType::String,
        display_name: name.to_string(),
    }
}

fn string_literal(value: &str) -> Expr<String> {
    Expr::Constant {
        span: None,
        scalar: Scalar::String(value.to_string()),
        data_type: DataType::String,
    }
}

fn function(name: &str, args: Vec<Expr<String>>) -> Expr<String> {
    Expr::FunctionCall {
        span: None,
        id: Box::new(FunctionID::Builtin {
            name: name.to_string(),
            id: 0,
        }),
        generics: vec![],
        args,
        return_type: DataType::Boolean,
    }
}

pub async fn read_blocks_via_pipeline(
    table: Arc<dyn Table>,
    push_downs: Option<PushDownInfo>,
) -> Result<Vec<DataBlock>> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let plan = table
        .read_plan(ctx.clone(), push_downs, None, false, false)
        .await?;
    let mut pipeline = databend_common_pipeline::core::Pipeline::create();
    table.read_data(ctx.clone(), &plan, &mut pipeline, false)?;
    let settings = ExecutorSettings::try_create(ctx)?;
    let mut executor = PipelinePullingExecutor::try_create(pipeline, settings)?;
    executor.start();
    let mut blocks = Vec::new();
    while let Some(block) = executor.pull_data().await? {
        blocks.push(block);
    }
    Ok(blocks)
}

pub fn collect_id_name_rows(blocks: &[DataBlock]) -> Vec<(i32, String)> {
    use databend_common_expression::ScalarRef;
    use databend_common_expression::types::NumberScalar;

    let mut rows = Vec::new();
    for block in blocks {
        if block.num_columns() < 2 {
            continue;
        }
        for i in 0..block.num_rows() {
            let id = match block.get_by_offset(0).index(i).unwrap() {
                ScalarRef::Number(NumberScalar::Int32(v)) => v,
                other => panic!("unexpected id scalar: {other:?}"),
            };
            let name = match block.get_by_offset(1).index(i).unwrap() {
                ScalarRef::String(v) => v.to_string(),
                other => panic!("unexpected name scalar: {other:?}"),
            };
            rows.push((id, name));
        }
    }
    rows
}

pub fn total_rows(blocks: &[DataBlock]) -> usize {
    blocks.iter().map(|block| block.num_rows()).sum()
}
