use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{plan_err, Result};
use datafusion::logical_expr::logical_plan::tree_node::unwrap_arc;
use datafusion::logical_expr::{col, ident, utils, Extension};
use datafusion::logical_expr::{Expr, Join, LogicalPlan, LogicalPlanBuilder, TableScan};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::sql::TableReference;

use crate::logical_plan::analyze::plan::{
    CalculationPlanNode, ModelPlanNode, ModelSourceNode,
};
use crate::logical_plan::utils::create_remote_table_source;
use crate::mdl::manifest::Model;
use crate::mdl::{AnalyzedWrenMDL, WrenMDL};

/// [ModelAnalyzeRule] responsible for analyzing the model plan node. Turn TableScan from a model to a ModelPlanNode.
/// We collect the required fields from the projection, filter, aggregation, and join,
/// and pass them to the ModelPlanNode.
pub struct ModelAnalyzeRule {
    analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
}

impl ModelAnalyzeRule {
    pub fn new(analyzed_wren_mdl: Arc<AnalyzedWrenMDL>) -> Self {
        Self { analyzed_wren_mdl }
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
                if belong_to_mdl(
                    &self.analyzed_wren_mdl.wren_mdl(),
                    table_scan.table_name.clone(),
                ) {
                    let table_name = table_scan.table_name.table();
                    // transform ViewTable to a subquery plan
                    if let Some(logical_plan) = table_scan.source.get_logical_plan() {
                        let subquery = LogicalPlanBuilder::from(logical_plan.clone())
                            .alias(table_name)?
                            .build()?;
                        return Ok(Transformed::yes(subquery));
                    }
                    if let Some(model) =
                        self.analyzed_wren_mdl.wren_mdl().get_model(table_name)
                    {
                        let field: Vec<Expr> =
                            used_columns.borrow().iter().cloned().collect();
                        let model = LogicalPlan::Extension(Extension {
                            node: Arc::new(ModelPlanNode::new(
                                model,
                                field,
                                Some(LogicalPlan::TableScan(table_scan.clone())),
                                Arc::clone(&self.analyzed_wren_mdl),
                            )?),
                        });
                        used_columns.borrow_mut().clear();
                        Ok(Transformed::yes(model))
                    } else {
                        Ok(Transformed::no(LogicalPlan::TableScan(table_scan)))
                    }
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
                    LogicalPlan::TableScan(table_scan) => analyze_table_scan(
                        Arc::clone(&self.analyzed_wren_mdl),
                        table_scan,
                        buffer.iter().cloned().collect(),
                    )?,
                    ignore => ignore,
                };

                let right = match unwrap_arc(join.right) {
                    LogicalPlan::TableScan(table_scan) => analyze_table_scan(
                        Arc::clone(&self.analyzed_wren_mdl),
                        table_scan,
                        buffer.iter().cloned().collect(),
                    )?,
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

fn belong_to_mdl(mdl: &WrenMDL, table_reference: TableReference) -> bool {
    let catalog_match = table_reference
        .catalog()
        .map_or(false, |c| c == mdl.catalog());

    let schema_match = table_reference
        .schema()
        .map_or(false, |s| s == mdl.schema());

    catalog_match && schema_match
}

fn analyze_table_scan(
    analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
    table_scan: TableScan,
    required_field: Vec<Expr>,
) -> Result<LogicalPlan> {
    if belong_to_mdl(&analyzed_wren_mdl.wren_mdl(), table_scan.table_name.clone()) {
        let table_name = table_scan.table_name.table();
        if let Some(model) = analyzed_wren_mdl.wren_mdl.get_model(table_name) {
            Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(ModelPlanNode::new(
                    model,
                    required_field,
                    Some(LogicalPlan::TableScan(table_scan.clone())),
                    Arc::clone(&analyzed_wren_mdl),
                )?),
            }))
        } else {
            Ok(LogicalPlan::TableScan(table_scan))
        }
    } else {
        Ok(LogicalPlan::TableScan(table_scan))
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

/// [ModelGenerationRule] is responsible for generating the model plan node.
pub struct ModelGenerationRule {
    analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
}

impl ModelGenerationRule {
    pub fn new(mdl: Arc<AnalyzedWrenMDL>) -> Self {
        Self {
            analyzed_wren_mdl: mdl,
        }
    }

    pub(crate) fn generate_model_internal(
        &self,
        plan: LogicalPlan,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Extension(extension) => {
                if let Some(model_plan) =
                    extension.node.as_any().downcast_ref::<ModelPlanNode>()
                {
                    let source_plan = model_plan.relation_chain.clone().plan(
                        ModelGenerationRule::new(Arc::clone(&self.analyzed_wren_mdl)),
                    )?;

                    let model: Arc<Model> = Arc::clone(
                        &self
                            .analyzed_wren_mdl
                            .wren_mdl()
                            .get_model(&model_plan.model_name)
                            .expect("Model not found"),
                    );
                    let result = match source_plan {
                        Some(plan) => LogicalPlanBuilder::from(plan)
                            .project(model_plan.required_exprs.clone())?
                            .build()?,
                        _ => {
                            return plan_err!("Failed to generate source plan");
                        }
                    };
                    // calculated field scope

                    let alias = LogicalPlanBuilder::from(result)
                        .alias(model.name.clone())?
                        .build()?;
                    Ok(Transformed::yes(alias))
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
                    // support table reference
                    let table_scan = match &model_plan.original_table_scan {
                        Some(LogicalPlan::TableScan(original_scan)) => {
                            LogicalPlanBuilder::scan_with_filters(
                                TableReference::from(&model.table_reference),
                                create_remote_table_source(
                                    &model,
                                    &self.analyzed_wren_mdl.wren_mdl(),
                                ),
                                None,
                                original_scan.filters.clone(),
                            ).expect("Failed to create table scan")
                                .project(model_plan.required_exprs.clone())?
                                .build()
                        }
                        Some(_) => Err(datafusion::error::DataFusionError::Internal(
                            "ModelPlanNode should have a TableScan as original_table_scan"
                                .to_string(),
                        )),
                        None => {
                            LogicalPlanBuilder::scan(
                                TableReference::from(&model.table_reference),
                                create_remote_table_source(&model, &self.analyzed_wren_mdl.wren_mdl()),
                                None,
                            ).expect("Failed to create table scan")
                                .project(model_plan.required_exprs.clone())?
                                .build()
                        },
                    }?;

                    // it could be count(*) query
                    if model_plan.required_exprs.is_empty() {
                        return Ok(Transformed::no(table_scan));
                    }
                    let result = LogicalPlanBuilder::from(table_scan)
                        .alias(model.name.clone())?
                        .build()?;
                    Ok(Transformed::yes(result))
                } else if let Some(calculation_plan) = extension
                    .node
                    .as_any()
                    .downcast_ref::<CalculationPlanNode>(
                ) {
                    let source_plan = calculation_plan.relation_chain.clone().plan(
                        ModelGenerationRule::new(Arc::clone(&self.analyzed_wren_mdl)),
                    )?;

                    if let Expr::Alias(alias) = calculation_plan.measures[0].clone() {
                        let measure: Expr = *alias.expr.clone();
                        let name = alias.name.clone();
                        let ident = ident(measure.to_string()).alias(name);
                        let project = vec![calculation_plan.dimensions[0].clone(), ident];
                        let result = match source_plan {
                            Some(plan) => LogicalPlanBuilder::from(plan)
                                .aggregate(
                                    calculation_plan.dimensions.clone(),
                                    vec![measure],
                                )?
                                .project(project)?
                                .build()?,
                            _ => {
                                return plan_err!("Failed to generate source plan");
                            }
                        };
                        let alias = LogicalPlanBuilder::from(result)
                            .alias(calculation_plan.calculation.column.name())?
                            .build()?;
                        Ok(Transformed::yes(alias))
                    } else {
                        return plan_err!("measures should have an alias");
                    }
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

/// [RemoveWrenPrefixRule] is responsible for removing the wren prefix from the column.
/// After [ModelGenerationRule] generates the model plan node, the column name will keep the schema prefix.
/// This rule removes the schema prefix from the column name.
pub struct RemoveWrenPrefixRule {
    analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
}

impl RemoveWrenPrefixRule {
    pub fn new(analyzed_wren_mdl: Arc<AnalyzedWrenMDL>) -> Self {
        Self { analyzed_wren_mdl }
    }
}

impl AnalyzerRule for RemoveWrenPrefixRule {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_down(&|plan: LogicalPlan| -> Result<Transformed<LogicalPlan>> {
            plan.map_expressions(&|expr: Expr| {
                expr.transform_down(&|expr: Expr| -> Result<Transformed<Expr>> {
                    if let Expr::Column(ref column) = expr {
                        if let Some(relation) = &column.relation {
                            match relation {
                                TableReference::Full {
                                    catalog,
                                    schema,
                                    table,
                                } => {
                                    if **catalog
                                        == *self.analyzed_wren_mdl.wren_mdl().catalog()
                                        && **schema
                                            == *self.analyzed_wren_mdl.wren_mdl().schema()
                                    {
                                        return Ok(Transformed::yes(col(format!(
                                            "{}.{}",
                                            table, column.name
                                        ))));
                                    }
                                }
                                TableReference::Partial { schema, table } => {
                                    if **schema
                                        == *self.analyzed_wren_mdl.wren_mdl().schema()
                                    {
                                        return Ok(Transformed::yes(col(format!(
                                            "{}.{}",
                                            table, column.name
                                        ))));
                                    }
                                }
                                TableReference::Bare { table: _ } => {
                                    return Ok(Transformed::no(expr.clone()));
                                }
                            }
                        }
                        return Ok(Transformed::no(expr.clone()));
                    }
                    Ok(Transformed::no(expr.clone()))
                })
            })
        })
        .data()
    }

    fn name(&self) -> &str {
        "RemoveWrenPrefixRule"
    }
}

#[cfg(test)]
mod test {
    use crate::logical_plan::analyze::rule::RemoveWrenPrefixRule;
    use crate::logical_plan::context_provider::WrenContextProvider;
    use crate::mdl::builder::{ColumnBuilder, ManifestBuilder, ModelBuilder};
    use crate::mdl::AnalyzedWrenMDL;
    use datafusion::common::DataFusionError;
    use datafusion::config::ConfigOptions;
    use datafusion::error::Result;
    use datafusion::optimizer::Analyzer;
    use datafusion::sql::planner::SqlToRel;
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;
    use datafusion::sql::unparser::plan_to_sql;
    use log::info;
    use std::sync::Arc;

    #[test]
    fn test_remove_prefix() -> Result<(), DataFusionError> {
        let manifest = ManifestBuilder::new()
            .model(
                ModelBuilder::new("a")
                    .column(ColumnBuilder::new("c1", "integer").build())
                    .column(ColumnBuilder::new("c2", "varchar").build())
                    .build(),
            )
            .build();
        let analyzed_wren_mdl = Arc::new(AnalyzedWrenMDL::analyze(manifest)?);

        // [RemoveWrenPrefixRule] only remove the prefix of identifiers, so that the table name in
        // the expected result will have the schema prefix.
        let tests = vec![
            ("select wrenai.public.a.c1, wrenai.public.a.c2 from wrenai.public.a",
             r#"SELECT a.c1, a.c2 FROM wrenai.public.a"#),
            ("select wrenai.public.a.c1, wrenai.public.a.c2 from wrenai.public.a where wrenai.public.a.c1 = 1",
                 r#"SELECT a.c1, a.c2 FROM wrenai.public.a WHERE (a.c1 = 1)"#),
            ("select wrenai.public.a.c1 + 1 from wrenai.public.a",
            r#"SELECT (a.c1 + 1) FROM wrenai.public.a"#)
        ];

        let context_provider = WrenContextProvider::new(&analyzed_wren_mdl.wren_mdl)?;
        let sql_to_rel = SqlToRel::new(&context_provider);
        let dialect = GenericDialect {};
        let analyzer = Analyzer::with_rules(vec![Arc::new(RemoveWrenPrefixRule::new(
            Arc::clone(&analyzed_wren_mdl),
        ))]);

        for (sql, expected) in tests {
            let ast = Parser::parse_sql(&dialect, sql).unwrap();
            let statement = &ast[0];

            // create a logical query plan
            let plan = match sql_to_rel.sql_statement_to_plan(statement.clone()) {
                Ok(plan) => plan,
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                    return Err(e);
                }
            };

            let config = ConfigOptions::default();

            let analyzed = match analyzer.execute_and_check(plan, &config, |_, _| {}) {
                Ok(analyzed) => analyzed,
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                    return Err(e);
                }
            };

            let actual = match plan_to_sql(&analyzed) {
                Ok(sql) => {
                    info!("wren-core planned SQL: {}", sql.to_string());
                    sql.to_string()
                }
                Err(e) => return Err(e),
            };
            assert_eq!(actual, expected);
        }
        Ok(())
    }
}
