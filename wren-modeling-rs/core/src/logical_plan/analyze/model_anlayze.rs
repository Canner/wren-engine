use std::cell::{RefCell, RefMut};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::catalog_common::TableReference;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Column, DFSchemaRef, Result};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::{
    col, ident, utils, Aggregate, Distinct, DistinctOn, Expr, Extension, Filter, Join,
    LogicalPlan, LogicalPlanBuilder, Projection, Subquery, SubqueryAlias, TableScan,
    Window,
};
use datafusion::optimizer::AnalyzerRule;

use crate::logical_plan::analyze::plan::ModelPlanNode;
use crate::mdl::utils::quoted;
use crate::mdl::{AnalyzedWrenMDL, Dataset, SessionStateRef, WrenMDL};

/// [ModelAnalyzeRule] responsible for analyzing the model plan node. Turn TableScan from a model to a ModelPlanNode.
/// We collect the required fields from the projection, filter, aggregation, and join,
/// and pass them to the ModelPlanNode.
pub struct ModelAnalyzeRule {
    analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
    session_state: SessionStateRef,
}

impl ModelAnalyzeRule {
    pub fn new(
        analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
        session_state: SessionStateRef,
    ) -> Self {
        Self {
            analyzed_wren_mdl,
            session_state,
        }
    }

    fn session_state(&self) -> SessionStateRef {
        Arc::clone(&self.session_state)
    }

    fn analyze_model_internal(
        &self,
        plan: LogicalPlan,
        analysis: &RefCell<Analysis>,
    ) -> datafusion::common::Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Projection(projection) => {
                let mut analysis_mut = analysis.borrow_mut();
                let mut buffer = analysis_mut.required_columns_mut();
                projection.expr.iter().try_for_each(|expr| {
                    let mut acuum = HashSet::new();
                    utils::expr_to_columns(expr, &mut acuum)?;
                    acuum.iter().try_for_each(|expr| {
                        self.collect_column(Expr::Column(expr.clone()), &mut buffer)
                    })
                })?;
                Ok(Transformed::no(LogicalPlan::Projection(projection)))
            }
            LogicalPlan::Filter(filter) => {
                let mut acuum = HashSet::new();
                utils::expr_to_columns(&filter.predicate, &mut acuum)?;
                let mut analysis_mut = analysis.borrow_mut();
                let mut buffer = analysis_mut.required_columns_mut();
                acuum.iter().try_for_each(|expr| {
                    self.collect_column(Expr::Column(expr.clone()), &mut buffer)
                })?;
                Ok(Transformed::no(LogicalPlan::Filter(filter)))
            }
            LogicalPlan::Aggregate(aggregate) => {
                let mut analysis_mut = analysis.borrow_mut();
                let mut buffer = analysis_mut.required_columns_mut();
                let mut accum = HashSet::new();
                let _ = &aggregate.aggr_expr.iter().for_each(|expr| {
                    Expr::add_column_refs(expr, &mut accum);
                });
                let _ = &aggregate.group_expr.iter().for_each(|expr| {
                    Expr::add_column_refs(expr, &mut accum);
                });
                accum.iter().try_for_each(|expr| {
                    self.collect_column(
                        Expr::Column(expr.to_owned().clone()),
                        &mut buffer,
                    )
                })?;
                Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)))
            }
            LogicalPlan::Subquery(Subquery {
                subquery,
                outer_ref_columns,
            }) => {
                let mut analysis_mut = analysis.borrow_mut();
                let mut buffer = analysis_mut.required_columns_mut();
                outer_ref_columns.iter().try_for_each(|expr| {
                    self.collect_column(expr.clone(), &mut buffer)
                })?;
                Ok(Transformed::no(LogicalPlan::Subquery(Subquery {
                    subquery,
                    outer_ref_columns,
                })))
            }
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
                let mut analysis_mut = analysis.borrow_mut();
                match Arc::unwrap_or_clone(input) {
                    LogicalPlan::TableScan(table_scan) => {
                        let model_plan = self
                            .analyze_table_scan(
                                Arc::clone(&self.analyzed_wren_mdl),
                                Arc::clone(&self.session_state),
                                table_scan,
                                Some(alias.clone()),
                                &mut analysis_mut,
                            )?
                            .data;
                        Ok(Transformed::yes(LogicalPlan::SubqueryAlias(
                            SubqueryAlias::try_new(Arc::new(model_plan), alias)?,
                        )))
                    }
                    ignore => Ok(Transformed::no(LogicalPlan::SubqueryAlias(
                        SubqueryAlias::try_new(Arc::new(ignore), alias)?,
                    ))),
                }
            }
            LogicalPlan::TableScan(table_scan) => self.analyze_table_scan(
                Arc::clone(&self.analyzed_wren_mdl),
                Arc::clone(&self.session_state),
                table_scan,
                None,
                &mut analysis.borrow_mut(),
            ),
            LogicalPlan::Join(join) => {
                let mut analysis_mut = analysis.borrow_mut();
                let mut buffer = analysis_mut.required_columns_mut();
                let mut accum = HashSet::new();
                join.on.iter().for_each(|expr| {
                    let _ = utils::expr_to_columns(&expr.0, &mut accum);
                    let _ = utils::expr_to_columns(&expr.1, &mut accum);
                });
                if let Some(filter_expr) = &join.filter {
                    let _ = utils::expr_to_columns(filter_expr, &mut accum);
                }
                accum.iter().try_for_each(|expr| {
                    self.collect_column(Expr::Column(expr.clone()), &mut buffer)
                })?;

                let left = match Arc::unwrap_or_clone(join.left) {
                    LogicalPlan::TableScan(table_scan) => {
                        self.analyze_table_scan(
                            Arc::clone(&self.analyzed_wren_mdl),
                            Arc::clone(&self.session_state),
                            table_scan,
                            None,
                            &mut analysis_mut,
                        )?
                        .data
                    }
                    ignore => ignore,
                };

                let right = match Arc::unwrap_or_clone(join.right) {
                    LogicalPlan::TableScan(table_scan) => {
                        self.analyze_table_scan(
                            Arc::clone(&self.analyzed_wren_mdl),
                            Arc::clone(&self.session_state),
                            table_scan,
                            None,
                            &mut analysis_mut,
                        )?
                        .data
                    }
                    ignore => ignore,
                };
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

    fn collect_column(
        &self,
        expr: Expr,
        buffer: &mut HashMap<TableReference, HashSet<Expr>>,
    ) -> Result<()> {
        match expr {
            Expr::Column(Column {
                relation: Some(relation),
                name,
            }) => {
                if belong_to_mdl(
                    &self.analyzed_wren_mdl.wren_mdl(),
                    relation.clone(),
                    self.session_state(),
                ) {
                    buffer
                        .entry(relation.clone())
                        .or_default()
                        .insert(Expr::Column(Column {
                            relation: Some(relation),
                            name,
                        }));
                }
            }
            Expr::OuterReferenceColumn(_, column) => {
                self.collect_column(Expr::Column(column), buffer)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn analyze_table_scan(
        &self,
        analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
        session_state_ref: SessionStateRef,
        table_scan: TableScan,
        alias: Option<TableReference>,
        analysis: &mut RefMut<Analysis>,
    ) -> Result<Transformed<LogicalPlan>> {
        if belong_to_mdl(
            &analyzed_wren_mdl.wren_mdl(),
            table_scan.table_name.clone(),
            Arc::clone(&session_state_ref),
        ) {
            let table_name = table_scan.table_name.table();
            // transform ViewTable to a subquery plan
            if let Some(logical_plan) = table_scan.source.get_logical_plan() {
                let subquery = LogicalPlanBuilder::from(logical_plan.clone())
                    .alias(quoted(table_name))?
                    .build()?;
                return Ok(Transformed::yes(subquery));
            }

            if let Some(model) = analyzed_wren_mdl.wren_mdl.get_model(table_name) {
                let table_ref = alias.unwrap_or(table_scan.table_name.clone());
                let mut used_columns = analysis.required_columns_mut();
                let buffer = used_columns.get(&table_ref);
                let field: Vec<Expr> = buffer
                    .map(|s| s.iter().cloned().collect())
                    .unwrap_or(vec![]);
                let model_plan = LogicalPlan::Extension(Extension {
                    node: Arc::new(ModelPlanNode::new(
                        Arc::clone(&model),
                        field,
                        Some(LogicalPlan::TableScan(table_scan.clone())),
                        Arc::clone(&self.analyzed_wren_mdl),
                        Arc::clone(&self.session_state),
                    )?),
                });
                let subquery = LogicalPlanBuilder::from(model_plan)
                    .alias(quoted(model.name()))?
                    .build()?;
                if let Some(buffer) = used_columns.get_mut(&table_ref) {
                    buffer.clear();
                }
                Ok(Transformed::yes(subquery))
            } else {
                Ok(Transformed::no(LogicalPlan::TableScan(table_scan)))
            }
        } else {
            Ok(Transformed::no(LogicalPlan::TableScan(table_scan)))
        }
    }

    fn replace_model_prefix_and_refresh_schema(
        &self,
        plan: LogicalPlan,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
                let subquery = self
                    .replace_model_prefix_and_refresh_schema(Arc::unwrap_or_clone(input))?
                    .data;
                Ok(Transformed::yes(LogicalPlan::SubqueryAlias(
                    SubqueryAlias::try_new(Arc::new(subquery), alias)?,
                )))
            }
            LogicalPlan::Subquery(Subquery {
                subquery,
                outer_ref_columns,
            }) => {
                let subquery = self
                    .replace_model_prefix_and_refresh_schema(Arc::unwrap_or_clone(
                        subquery,
                    ))?
                    .data;
                Ok(Transformed::yes(LogicalPlan::Subquery(Subquery {
                    subquery: Arc::new(subquery),
                    outer_ref_columns,
                })))
            }
            LogicalPlan::Distinct(Distinct::On(DistinctOn {
                on_expr,
                select_expr,
                sort_expr,
                input,
                ..
            })) => Ok(Transformed::yes(LogicalPlan::Distinct(Distinct::On(
                DistinctOn::try_new(on_expr, select_expr, sort_expr, input)?,
            )))),
            LogicalPlan::Window(Window {
                input, window_expr, ..
            }) => Ok(Transformed::yes(LogicalPlan::Window(Window::try_new(
                window_expr,
                input,
            )?))),
            LogicalPlan::Projection(Projection { expr, input, .. }) => {
                let Some(alias_model) = Self::find_alias_model(Arc::clone(&input)) else {
                    return Ok(Transformed::no(LogicalPlan::Projection(
                        Projection::try_new(expr, input)?,
                    )));
                };
                let expr = expr
                    .into_iter()
                    .map(|e| {
                        self.map_column_and_rewrite_qualifier(
                            e,
                            &alias_model,
                            input.schema().clone(),
                        )
                        .data()
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Transformed::yes(LogicalPlan::Projection(
                    Projection::try_new(expr, input)?,
                )))
            }
            LogicalPlan::Filter(Filter {
                input, predicate, ..
            }) => {
                let Some(alias_model) = Self::find_alias_model(Arc::clone(&input)) else {
                    return Ok(Transformed::no(LogicalPlan::Filter(Filter::try_new(
                        predicate, input,
                    )?)));
                };
                let expr = self
                    .map_column_and_rewrite_qualifier(
                        predicate,
                        &alias_model,
                        input.schema().clone(),
                    )?
                    .data;
                Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
                    expr, input,
                )?)))
            }
            LogicalPlan::Aggregate(Aggregate {
                input,
                aggr_expr,
                group_expr,
                ..
            }) => {
                let Some(alias_model) = Self::find_alias_model(Arc::clone(&input)) else {
                    return Ok(Transformed::no(LogicalPlan::Aggregate(
                        Aggregate::try_new(input, group_expr, aggr_expr)?,
                    )));
                };
                let aggr_expr = aggr_expr
                    .into_iter()
                    .map(|e| {
                        self.map_column_and_rewrite_qualifier(
                            e,
                            &alias_model,
                            input.schema().clone(),
                        )
                        .data()
                    })
                    .collect::<Result<Vec<_>>>()?;
                let group_expr = group_expr
                    .into_iter()
                    .map(|e| {
                        self.map_column_and_rewrite_qualifier(
                            e,
                            &alias_model,
                            input.schema().clone(),
                        )
                        .data()
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Transformed::yes(LogicalPlan::Aggregate(
                    Aggregate::try_new(input, group_expr, aggr_expr)?,
                )))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }

    fn map_column_and_rewrite_qualifier(
        &self,
        expr: Expr,
        alias_model: &str,
        schema: DFSchemaRef,
    ) -> Result<Transformed<Expr>> {
        match expr {
            Expr::Column(Column { relation, name }) => {
                if let Some(relation) = relation {
                    Ok(self.rewrite_column_qualifier(relation, name, alias_model))
                } else {
                    let catalog_schema = format!(
                        "{}.{}.",
                        self.analyzed_wren_mdl.wren_mdl().catalog(),
                        self.analyzed_wren_mdl.wren_mdl().schema()
                    );
                    let name = name.replace(&catalog_schema, "");
                    let ident = ident(&name);
                    Ok(Transformed::yes(ident))
                }
            }
            Expr::Alias(Alias {
                expr,
                relation,
                name,
            }) => {
                let expr =
                    self.map_column_and_rewrite_qualifier(*expr, alias_model, schema)?;
                Ok(Transformed::yes(Expr::Alias(Alias {
                    expr: Box::new(expr.data),
                    relation,
                    name,
                })))
            }
            _ => expr.map_children(|e| {
                self.map_column_and_rewrite_qualifier(e, alias_model, schema.clone())
            }),
        }
    }

    fn rewrite_column_qualifier(
        &self,
        relation: TableReference,
        name: String,
        alias_model: &str,
    ) -> Transformed<Expr> {
        if belong_to_mdl(
            &self.analyzed_wren_mdl.wren_mdl(),
            relation.clone(),
            self.session_state(),
        ) {
            if self
                .analyzed_wren_mdl
                .wren_mdl()
                .get_model(relation.table())
                .is_some()
            {
                Transformed::yes(col(format!("{}.{}", alias_model, quoted(&name))))
            } else {
                // handle Wren View
                let catalog_schema = format!(
                    "{}.{}.",
                    self.analyzed_wren_mdl.wren_mdl().catalog(),
                    self.analyzed_wren_mdl.wren_mdl().schema()
                );
                let name = name.replace(&catalog_schema, "");
                Transformed::yes(Expr::Column(Column::new(
                    Some(TableReference::bare(relation.table())),
                    &name,
                )))
            }
        } else {
            Transformed::no(Expr::Column(Column {
                relation: Some(relation),
                name,
            }))
        }
    }

    /// Find Plan pattern like
    /// SubqueryAlias
    ///     Extension
    ///         ModelPlanNode
    /// and return the model name
    fn find_alias_model(plan: Arc<LogicalPlan>) -> Option<String> {
        let plan = Arc::unwrap_or_clone(plan);
        match plan {
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
                if let LogicalPlan::Extension(Extension { node }) =
                    Arc::unwrap_or_clone(Arc::clone(&input))
                {
                    if node.as_any().downcast_ref::<ModelPlanNode>().is_some() {
                        Some(alias.to_quoted_string())
                    } else {
                        None
                    }
                } else {
                    Self::find_alias_model(input)
                }
            }
            LogicalPlan::Filter(Filter { input, .. }) => Self::find_alias_model(input),
            LogicalPlan::Aggregate(Aggregate { input, .. }) => {
                Self::find_alias_model(input)
            }
            LogicalPlan::Projection(Projection { input, .. }) => {
                Self::find_alias_model(input)
            }
            _ => None,
        }
    }
}

fn belong_to_mdl(
    mdl: &WrenMDL,
    table_reference: TableReference,
    session: SessionStateRef,
) -> bool {
    let session = session.read();
    let catalog = table_reference
        .catalog()
        .unwrap_or(&session.config_options().catalog.default_catalog);
    let catalog_match = catalog == mdl.catalog();

    let schema = table_reference
        .schema()
        .unwrap_or(&session.config_options().catalog.default_schema);
    let schema_match = schema == mdl.schema();

    catalog_match && schema_match
}

impl AnalyzerRule for ModelAnalyzeRule {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        let analysis = RefCell::new(Analysis::default());
        // plan.map_subqueries(&|plan| {
        //     self.analyze_model_internal(plan, &analysis)
        // })?;
        // replace the top level plan node with ModelPlanNode first
        plan.transform_down_with_subqueries(
            &|plan| -> Result<Transformed<LogicalPlan>> {
                self.analyze_model_internal(plan, &analysis)
            },
        )?
        // After planned the top-level, replace the ModelPlanNode in the subquery
        // .map_data(|plan| {
        //     plan.transform_down_with_subqueries(
        //         &|plan| -> Result<Transformed<LogicalPlan>> {
        //             self.analyze_model_internal(plan, &analysis)
        //         },
        //     )
        //     .data()
        // })?
        .map_data(|plan| {
            plan.transform_up_with_subqueries(
                &|plan| -> Result<Transformed<LogicalPlan>> {
                    self.replace_model_prefix_and_refresh_schema(plan)
                },
            )
        })?
        .map_data(|plan| plan.data.recompute_schema())
        .data()
    }

    fn name(&self) -> &str {
        "ModelAnalyzeRule"
    }
}

/// The context of the analysis
#[derive(Debug, Default)]
struct Analysis {
    /// The columns required by the dataset
    required_columns: HashMap<TableReference, HashSet<Expr>>,
    /// The map from alias to dataset
    visited_alias_table: HashMap<TableReference, Dataset>,
}

impl Analysis {
    fn new() -> Self {
        Self {
            required_columns: HashMap::new(),
            visited_alias_table: HashMap::new(),
        }
    }

    fn required_columns(&self) -> &HashMap<TableReference, HashSet<Expr>> {
        &self.required_columns
    }

    fn required_columns_mut(&mut self) -> &mut HashMap<TableReference, HashSet<Expr>> {
        &mut self.required_columns
    }

    fn visited_alias_table(&self) -> &HashMap<TableReference, Dataset> {
        &self.visited_alias_table
    }

    fn visited_alias_table_mut(&mut self) -> &mut HashMap<TableReference, Dataset> {
        &mut self.visited_alias_table
    }
}
