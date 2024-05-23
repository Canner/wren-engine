use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Column, DFField, DFSchema, DFSchemaRef, Result};
use datafusion::logical_expr::{
    col, Expr, LogicalPlan, LogicalPlanBuilder, Projection, SubqueryAlias,
    UserDefinedLogicalNodeCore,
};
use datafusion::logical_expr::{utils, Extension};
use datafusion::optimizer::analyzer::AnalyzerRule;
use std::cell::RefCell;
use std::collections::{vec_deque, HashSet, VecDeque};
use std::{collections::HashMap, fmt, fmt::Debug, sync::Arc};

use crate::mdl::manifest::Model;
use crate::mdl::WrenMDL;

use super::utils::{create_remote_table_source, map_data_type};

// Regonzied the model. Turn TableScan from a model to MdoelPlanNode
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
        used_columns: &RefCell<VecDeque<Expr>>,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Projection(projection) => {
                projection.expr.iter().for_each(|expr| {
                    let mut buffer = used_columns.borrow_mut();
                    let mut acuum = HashSet::new();
                    let _ = utils::expr_to_columns(expr, &mut acuum);
                    acuum.iter().for_each(|expr| {
                        buffer.push_back(Expr::Column(expr.clone()));
                    });
                });
                return Ok(Transformed::no(LogicalPlan::Projection(projection)));
            }
            LogicalPlan::Filter(filter) => {
                let mut acuum = HashSet::new();
                let _ = utils::expr_to_columns(&filter.predicate, &mut acuum);
                let mut buffer = used_columns.borrow_mut();
                acuum.iter().for_each(|expr| {
                    buffer.push_back(Expr::Column(expr.clone()));
                });
                return Ok(Transformed::no(LogicalPlan::Filter(filter)));
            }
            LogicalPlan::Aggregate(aggregate) => {
                let mut accum = HashSet::new();
                let _ = utils::exprlist_to_columns(&aggregate.aggr_expr, &mut accum);
                let _ = utils::exprlist_to_columns(&aggregate.group_expr, &mut accum);
                let mut buffer = used_columns.borrow_mut();
                accum.iter().for_each(|expr| {
                    buffer.push_back(Expr::Column(expr.clone()));
                });
                return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
            }
            LogicalPlan::TableScan(table_scan) => {
                if let Some(model) = self
                    .mdl
                    .get_model(&table_scan.table_name.to_string().as_str())
                {
                    dbg!(used_columns.borrow());
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
            _ => return Ok(Transformed::no(plan)),
        }
    }
}

impl AnalyzerRule for ModelAnalyzeRule {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        let used_columns = RefCell::new(VecDeque::new());
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
    requried_fields: Vec<Expr>,
    schema_ref: DFSchemaRef,
    original_table_scan: LogicalPlan,
}

impl ModelPlanNode {
    pub fn new(
        model: Arc<Model>,
        requried_fields: Vec<Expr>,
        original_table_scan: LogicalPlan,
    ) -> Self {
        let schema_ref = create_df_schema(Arc::clone(&model), requried_fields.clone());
        Self {
            model_name: model.name.clone(),
            requried_fields,
            schema_ref,
            original_table_scan,
        }
    }
}

fn create_df_schema(model: Arc<Model>, required_fields: Vec<Expr>) -> DFSchemaRef {
    let fields: Vec<DFField> = required_fields
        .iter()
        .map(|column| {
            let column_ref = model
                .columns
                .iter()
                .find(|col| format!("{}.{}", model.name, col.name) == column.to_string())
                .expect(&format!("Column {} not found in {}", column, &model.name));
            let data_type = map_data_type(&column_ref.r#type);
            DFField::new(
                Some(model.name.clone()),
                &column_ref.name,
                data_type,
                column_ref.no_null,
            )
        })
        .collect();

    DFSchemaRef::new(
        DFSchema::new_with_metadata(fields, HashMap::new()).expect("create schema failed"),
    )
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
        self.requried_fields.clone()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Model: name={}", self.model_name)
    }

    fn from_template(&self, _: &[Expr], _: &[LogicalPlan]) -> Self {
        ModelPlanNode {
            model_name: self.model_name.clone(),
            requried_fields: vec![],
            schema_ref: DFSchemaRef::new(DFSchema::empty()),
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
                    dbg!(model_plan);
                    let mut exprs = vec_deque::VecDeque::new();

                    // required fields should be qulified with table name
                    model_plan
                        .requried_fields
                        .iter()
                        .filter(|expr| self.mdl.qualifed_references.contains_key(&expr.to_string()))
                        .for_each(|expr| {
                            let qualifed_ref =
                                self.mdl.qualifed_references.get(&expr.to_string()).unwrap();
                            let column_ref = qualifed_ref.get_column();
                            let expr_plan = if let Some(expression) = &column_ref.expression {
                                col(expression).alias(column_ref.name.clone())
                            } else {
                                col(column_ref.name.clone())
                            };
                            exprs.push_back(expr_plan);
                        });

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

                    let exprs_vec = exprs.into_iter().collect();
                    let result = Projection::try_new(exprs_vec, Arc::new(table_scan?)).unwrap();

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
