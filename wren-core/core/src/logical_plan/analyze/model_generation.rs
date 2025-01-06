use std::fmt::Debug;
use std::sync::Arc;

use crate::logical_plan::analyze::plan::{
    CalculationPlanNode, ModelPlanNode, ModelSourceNode, PartialModelPlanNode,
};
use crate::logical_plan::utils::{
    create_remote_table_source, eliminate_ambiguous_columns, rebase_column,
};
use crate::mdl::manifest::Model;
use crate::mdl::utils::quoted;
use crate::mdl::{AnalyzedWrenMDL, SessionStateRef};
use crate::DataFusionError;
use datafusion::common::alias::AliasGenerator;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult};
use datafusion::common::{plan_err, Result};
use datafusion::logical_expr::{col, ident, Extension, UserDefinedLogicalNodeCore};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::physical_plan::internal_err;
use datafusion::sql::TableReference;

pub const SOURCE_ALIAS: &str = "__source";

/// [ModelGenerationRule] is responsible for generating the model plan node.
pub struct ModelGenerationRule {
    analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
    session_state: SessionStateRef,
}

impl ModelGenerationRule {
    pub fn new(mdl: Arc<AnalyzedWrenMDL>, session_state: SessionStateRef) -> Self {
        Self {
            analyzed_wren_mdl: mdl,
            session_state,
        }
    }

    pub(crate) fn generate_model_internal(
        &self,
        plan: LogicalPlan,
    ) -> Result<Transformed<LogicalPlan>> {
        let alias_generator = AliasGenerator::default();
        match plan {
            LogicalPlan::Extension(extension) => {
                if let Some(model_plan) =
                    extension.node.as_any().downcast_ref::<ModelPlanNode>()
                {
                    let (source_plan, alias) = model_plan.relation_chain.clone().plan(
                        ModelGenerationRule::new(
                            Arc::clone(&self.analyzed_wren_mdl),
                            Arc::clone(&self.session_state),
                        ),
                        &alias_generator,
                    )?;

                    let projections = if let Some(alias) = alias {
                        model_plan
                            .required_exprs
                            .iter()
                            .map(|expr| rebase_column(expr, &alias).unwrap())
                            .collect()
                    } else {
                        model_plan.required_exprs.clone()
                    };
                    let projections = eliminate_ambiguous_columns(projections);
                    let result = match source_plan {
                        Some(plan) => {
                            if model_plan.required_exprs.is_empty() {
                                plan
                            } else {
                                LogicalPlanBuilder::from(plan)
                                    .project(projections)?
                                    .build()?
                            }
                        }
                        _ => {
                            return plan_err!("Failed to generate source plan");
                        }
                    };
                    // calculated field scope
                    Ok(Transformed::yes(result))
                } else if let Some(model_plan) =
                    extension.node.as_any().downcast_ref::<ModelSourceNode>()
                {
                    let model: Arc<Model> = Arc::clone(
                        &self
                            .analyzed_wren_mdl
                            .wren_mdl()
                            .get_model(&model_plan.model_name)
                            .expect("Model not found"),
                    );
                    let mut required_exprs = model_plan.required_exprs.clone();
                    required_exprs.iter_mut().try_for_each(|expr| {
                        *expr = rebase_column(expr, SOURCE_ALIAS)?;
                        Ok::<(), DataFusionError>(())
                    })?;
                    // support table reference
                    let table_scan = match &model_plan.original_table_scan {
                        Some(LogicalPlan::TableScan(original_scan)) => {
                            LogicalPlanBuilder::scan_with_filters(
                                TableReference::from(model.table_reference()),
                                create_remote_table_source(
                                    Arc::clone(&model),
                                    &self.analyzed_wren_mdl.wren_mdl(),
                                    Arc::clone(&self.session_state),
                                )?,
                                None,
                                original_scan.filters.clone(),
                            )?
                                .alias(SOURCE_ALIAS)?
                            .project(required_exprs)?
                            .build()
                        }
                        Some(_) => Err(datafusion::error::DataFusionError::Internal(
                            "ModelPlanNode should have a TableScan as original_table_scan"
                                .to_string(),
                        )),
                        None => {
                            LogicalPlanBuilder::scan(
                                TableReference::from(model.table_reference()),
                                create_remote_table_source(
                                    Arc::clone(&model),
                                    &self.analyzed_wren_mdl.wren_mdl(),
                                    Arc::clone(&self.session_state))?,
                                None,
                            )?
                                .alias(SOURCE_ALIAS)?
                                .project(required_exprs)?
                                .build()
                        },
                    }?;

                    // it could be count(*) query
                    if model_plan.required_exprs.is_empty() {
                        return Ok(Transformed::no(table_scan));
                    }
                    let result = LogicalPlanBuilder::from(table_scan)
                        .alias(quoted(model.name()))?
                        .build()?;
                    Ok(Transformed::yes(result))
                } else if let Some(calculation_plan) = extension
                    .node
                    .as_any()
                    .downcast_ref::<CalculationPlanNode>(
                ) {
                    let (source_plan, plan_alias) =
                        calculation_plan.relation_chain.clone().plan(
                            ModelGenerationRule::new(
                                Arc::clone(&self.analyzed_wren_mdl),
                                Arc::clone(&self.session_state),
                            ),
                            &alias_generator,
                        )?;

                    let plan_alias = if let Some(alias) = plan_alias {
                        alias
                    } else {
                        return internal_err!("calculation plan should have an alias");
                    };

                    if let Expr::Alias(alias) = calculation_plan.measures[0].clone() {
                        let measure: Expr = *alias.expr.clone();
                        let rebased_measure = rebase_column(&measure, &plan_alias)?;
                        let name = alias.name.clone();
                        let ident =
                            ident(rebased_measure.to_string()).alias(name.clone());
                        let rebased_dimension =
                            rebase_column(&calculation_plan.dimensions[0], &plan_alias)?;
                        let project = vec![rebased_dimension.clone(), ident];
                        let result = match source_plan {
                            Some(plan) => LogicalPlanBuilder::from(plan)
                                .aggregate(
                                    vec![rebased_dimension],
                                    vec![rebased_measure],
                                )?
                                .project(project)?
                                .build()?,
                            _ => {
                                return plan_err!("Failed to generate source plan");
                            }
                        };
                        let alias = LogicalPlanBuilder::from(result)
                            .alias(quoted(calculation_plan.calculation.column.name()))?
                            .build()?;
                        Ok(Transformed::yes(alias))
                    } else {
                        return plan_err!("measures should have an alias");
                    }
                } else if let Some(partial_model) = extension
                    .node
                    .as_any()
                    .downcast_ref::<PartialModelPlanNode>(
                ) {
                    let plan = LogicalPlan::Extension(Extension {
                        node: Arc::new(partial_model.model_node.clone()),
                    });

                    let subquery = LogicalPlanBuilder::from(plan)
                        .alias(quoted(partial_model.model_node.plan_name()))?
                        .build()?;
                    let source_plan = self.generate_model_internal(subquery)?.data;
                    let projection: Vec<_> = partial_model
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| col(datafusion::common::Column::from((None, f))))
                        .collect();
                    let projection = eliminate_ambiguous_columns(projection);
                    let alias = LogicalPlanBuilder::from(source_plan)
                        .project(projection)?
                        .alias(quoted(&partial_model.model_node.plan_name))?
                        .build()?;
                    Ok(Transformed::yes(alias))
                } else {
                    Ok(Transformed::no(LogicalPlan::Extension(extension)))
                }
            }
            _ => Ok(Transformed::yes(plan.recompute_schema()?)),
        }
    }
}

impl Debug for ModelGenerationRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModelGenerationRule").finish()
    }
}

impl AnalyzerRule for ModelGenerationRule {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        let transformed_up = plan
            .transform_up_with_subqueries(&|plan| -> Result<Transformed<LogicalPlan>> {
                self.generate_model_internal(plan)
            })
            .data()?;
        transformed_up
            .transform_down_with_subqueries(&|plan| -> Result<Transformed<LogicalPlan>> {
                self.generate_model_internal(plan)
            })
            .data()
    }

    fn name(&self) -> &str {
        "ModelGenerationRule"
    }
}
