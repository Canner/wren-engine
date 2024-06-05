use arrow_schema::Field;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, DFSchemaRef, Result};
use datafusion::logical_expr::logical_plan::tree_node::unwrap_arc;
use datafusion::logical_expr::{
    col, Expr, Join, LogicalPlan, LogicalPlanBuilder, Projection, SubqueryAlias,
    UserDefinedLogicalNodeCore,
};
use datafusion::logical_expr::{utils, Extension};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::sql::TableReference;
use std::cell::RefCell;
use std::collections::{HashSet, VecDeque};
use std::{collections::HashMap, fmt, fmt::Debug, sync::Arc};

use crate::mdl::manifest::Model;
use crate::mdl::WrenMDL;

use super::utils::{create_remote_table_source, map_data_type};

/// Regonzied the model. Turn TableScan from a model to MdoelPlanNode
/// We collect the requried field from the projection, filter, aggregation and join
/// and pass it to the ModelPlanNode
pub struct ModelAnalyzeRule {
    mdl: Arc<WrenMDL>,
}

impl ModelAnalyzeRule {
    pub fn new(mdl: Arc<WrenMDL>) -> Self {
        Self { mdl }
    }

    fn analyze_model_internal(
        &self,
        plan: LogicalPlan,
        used_columns: &RefCell<HashSet<Expr>>,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Projection(projection) => {
                let mut buffer = used_columns.borrow_mut();
                buffer.clear();
                projection.expr.iter().for_each(|expr| {
                    let mut acuum = HashSet::new();
                    let _ = utils::expr_to_columns(expr, &mut acuum);
                    acuum.iter().for_each(|expr| {
                        buffer.insert(Expr::Column(expr.clone()));
                    });
                });
                Ok(Transformed::no(LogicalPlan::Projection(projection)))
            }
            LogicalPlan::Filter(filter) => {
                let mut acuum = HashSet::new();
                let _ = utils::expr_to_columns(&filter.predicate, &mut acuum);
                let mut buffer = used_columns.borrow_mut();
                acuum.iter().for_each(|expr| {
                    buffer.insert(Expr::Column(expr.clone()));
                });
                Ok(Transformed::no(LogicalPlan::Filter(filter)))
            }
            LogicalPlan::Aggregate(aggregate) => {
                let mut buffer = used_columns.borrow_mut();
                buffer.clear();
                let mut accum = HashSet::new();
                let _ = utils::exprlist_to_columns(&aggregate.aggr_expr, &mut accum);
                let _ = utils::exprlist_to_columns(&aggregate.group_expr, &mut accum);
                accum.iter().for_each(|expr| {
                    buffer.insert(Expr::Column(expr.clone()));
                });
                Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)))
            }
            LogicalPlan::TableScan(table_scan) => {
                if let Some(model) = self
                    .mdl
                    .get_model(table_scan.table_name.to_string().as_str())
                {
                    let model = LogicalPlan::Extension(Extension {
                        node: Arc::new(ModelPlanNode::new(
                            model,
                            used_columns.borrow().iter().cloned().collect(),
                            LogicalPlan::TableScan(table_scan.clone()),
                        )),
                    });
                    used_columns.borrow_mut().clear();
                    Ok(Transformed::yes(model))
                } else {
                    Ok(Transformed::no(LogicalPlan::TableScan(table_scan)))
                }
            }
            LogicalPlan::Join(join) => {
                let mut buffer = used_columns.borrow_mut();
                let mut accum = HashSet::new();
                join.on.iter().for_each(|expr| {
                    let _ = utils::expr_to_columns(&expr.0, &mut accum);
                    let _ = utils::expr_to_columns(&expr.1, &mut accum);
                });
                if let Some(filter_expr) = &join.filter {
                    let _ = utils::expr_to_columns(filter_expr, &mut accum);
                }
                accum.iter().for_each(|expr| {
                    buffer.insert(Expr::Column(expr.clone()));
                });

                let left = match unwrap_arc(join.left) {
                    LogicalPlan::TableScan(table_scan) => {
                        if let Some(model) = self
                            .mdl
                            .get_model(table_scan.table_name.to_string().as_str())
                        {
                            LogicalPlan::Extension(Extension {
                                node: Arc::new(ModelPlanNode::new(
                                    model,
                                    buffer.iter().cloned().collect(),
                                    LogicalPlan::TableScan(table_scan.clone()),
                                )),
                            })
                        } else {
                            LogicalPlan::TableScan(table_scan)
                        }
                    }
                    ignore => ignore,
                };

                let right = match unwrap_arc(join.right) {
                    LogicalPlan::TableScan(table_scan) => {
                        if let Some(model) = self
                            .mdl
                            .get_model(table_scan.table_name.to_string().as_str())
                        {
                            LogicalPlan::Extension(Extension {
                                node: Arc::new(ModelPlanNode::new(
                                    model,
                                    buffer.iter().cloned().collect(),
                                    LogicalPlan::TableScan(table_scan.clone()),
                                )),
                            })
                        } else {
                            LogicalPlan::TableScan(table_scan)
                        }
                    }
                    ignore => ignore,
                };
                buffer.clear();
                Ok(Transformed::no(LogicalPlan::Join(Join {
                    left: Arc::new(left),
                    right: Arc::new(right),
                    on: join.on,
                    join_type: join.join_type,
                    schema: join.schema,
                    filter: join.filter,
                    join_constraint: join.join_constraint,
                    null_equals_null: join.null_equals_null,
                })))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

impl AnalyzerRule for ModelAnalyzeRule {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        let used_columns = RefCell::new(HashSet::new());
        plan.transform_down(&|plan| -> Result<Transformed<LogicalPlan>> {
            self.analyze_model_internal(plan, &used_columns)
        })
        .data()
    }

    fn name(&self) -> &str {
        "ModelAnalyzeRule"
    }
}

#[derive(PartialEq, Eq, Hash, Debug)]
struct ModelPlanNode {
    model_name: String,
    required_exprs: Vec<Expr>,
    schema_ref: DFSchemaRef,
    original_table_scan: LogicalPlan,
}

impl ModelPlanNode {
    pub fn new(
        model: Arc<Model>,
        requried_fields: Vec<Expr>,
        original_table_scan: LogicalPlan,
    ) -> Self {
        let mut required_exprs_buffer = VecDeque::new();
        let fields = model
            .get_physical_columns()
            .iter()
            .filter(|column| {
                requried_fields.iter().any(|expr| {
                    if let Expr::Column(column_expr) = expr {
                        column_expr.flat_name() == format!("{}.{}", model.name, column.name)
                    } else {
                        false
                    }
                })
            })
            .map(|column| {
                let expr_plan = if let Some(expression) = &column.expression {
                    col(expression).alias(column.name.clone())
                } else {
                    col(column.name.clone())
                };
                required_exprs_buffer.push_back(expr_plan);

                (
                    Some(TableReference::bare(model.name.clone())),
                    Arc::new(Field::new(
                        &column.name,
                        map_data_type(&column.r#type),
                        column.no_null,
                    )),
                )
            })
            .collect();

        let schema_ref = DFSchemaRef::new(
            DFSchema::new_with_metadata(fields, HashMap::new()).expect("create schema failed"),
        );

        Self {
            model_name: model.name.clone(),
            required_exprs: required_exprs_buffer.into_iter().collect(),
            schema_ref,
            original_table_scan,
        }
    }
}

// Just mock up the impl for UserDefinedLogicalNodeCore
impl UserDefinedLogicalNodeCore for ModelPlanNode {
    fn name(&self) -> &str {
        "Model"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema_ref
    }

    fn expressions(&self) -> Vec<Expr> {
        self.schema_ref
            .fields()
            .iter()
            .map(|field| col(field.name()))
            .collect()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Model: name={}", self.model_name)
    }

    fn from_template(&self, _: &[Expr], _: &[LogicalPlan]) -> Self {
        ModelPlanNode {
            model_name: self.model_name.clone(),
            required_exprs: self.required_exprs.clone(),
            schema_ref: self.schema_ref.clone(),
            original_table_scan: self.original_table_scan.clone(),
        }
    }
}

// Generate the query plan for the ModelPlanNode
pub struct ModelGenerationRule {
    mdl: Arc<WrenMDL>,
}

impl ModelGenerationRule {
    pub fn new(mdl: Arc<WrenMDL>) -> Self {
        Self { mdl }
    }

    fn generate_model_internal(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Extension(extension) => {
                if let Some(model_plan) = extension.node.as_any().downcast_ref::<ModelPlanNode>() {
                    let model: Arc<Model> = Arc::clone(
                        &self
                            .mdl
                            .get_model(&model_plan.model_name)
                            .expect("Model not found"),
                    );
                    // support table reference
                    let table_scan = match &model_plan.original_table_scan {
                        LogicalPlan::TableScan(original_scan) => {
                            LogicalPlanBuilder::scan_with_filters(
                                model.name.clone(),
                                create_remote_table_source(&model, &self.mdl),
                                None,
                                original_scan.filters.clone(),
                            )
                            .unwrap()
                            .build()
                        }
                        _ => Err(datafusion::error::DataFusionError::Internal(
                            "ModelPlanNode should have a TableScan as original_table_scan"
                                .to_string(),
                        )),
                    };

                    // it could be count(*) query
                    if model_plan.required_exprs.is_empty() {
                        return Ok(Transformed::no(table_scan?));
                    }

                    let result = Projection::try_new(
                        model_plan.required_exprs.clone(),
                        Arc::new(table_scan?),
                    )
                    .unwrap();

                    let alias_name = model.name.clone();
                    let alias = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                        Arc::new(LogicalPlan::Projection(result)),
                        alias_name,
                    )?);
                    Ok(Transformed::yes(alias))
                } else {
                    Ok(Transformed::no(LogicalPlan::Extension(extension)))
                }
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

impl AnalyzerRule for ModelGenerationRule {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_down(&|plan| -> Result<Transformed<LogicalPlan>> {
            self.generate_model_internal(plan)
        })
        .data()
    }

    fn name(&self) -> &str {
        "ModelGenerationRule"
    }
}
