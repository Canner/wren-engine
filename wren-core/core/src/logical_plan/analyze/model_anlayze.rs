use crate::logical_plan::analyze::plan::ModelPlanNode;
use crate::logical_plan::utils::{belong_to_mdl, expr_to_columns};
use crate::mdl::utils::quoted;
use crate::mdl::{AnalyzedWrenMDL, Dataset, SessionStateRef};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{internal_err, plan_err, Column, DFSchemaRef, Result, Spans};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::{
    col, ident, Aggregate, Distinct, DistinctOn, Expr, Extension, Filter, Join,
    LogicalPlan, LogicalPlanBuilder, Projection, Subquery, SubqueryAlias, TableScan,
    Window,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::sql::TableReference;
use std::cell::{RefCell, RefMut};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::sync::Arc;

/// [ModelAnalyzeRule] responsible for analyzing the model plan node. Turn TableScan from a model to a ModelPlanNode.
/// We collect the required fields from the projection, filter, aggregation, and join,
/// and pass them to the ModelPlanNode.
///
/// There are three main steps in this rule:
/// 1. Analyze the scope of the logical plan and collect the required columns for models and visited tables. (button-up and depth-first)
/// 2. Analyze the model and generate the ModelPlanNode according to the scope analysis. (button-up and depth-first)
/// 3. Remove the catalog and schema prefix of Wren for the column and refresh the schema. (top-down)
///
/// The traverse path of step 1 and step 2 should be same.
/// The corresponding scope will be pushed to or popped from the childs of [Scope] sequentially.
pub struct ModelAnalyzeRule {
    analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
    session_state: SessionStateRef,
}

impl Debug for ModelAnalyzeRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModelAnalyzeRule").finish()
    }
}

impl AnalyzerRule for ModelAnalyzeRule {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        let root = RefCell::new(Scope::new());
        self.analyze_scope(plan, &root)?
            .map_data(|plan| self.analyze_model(plan, &root).data())?
            .map_data(|plan| {
                plan.transform_up_with_subqueries(&|plan| -> Result<
                    Transformed<LogicalPlan>,
                > {
                    self.remove_wren_catalog_schema_prefix_and_refresh_schema(plan)
                })
                .data()
            })?
            .map_data(|plan| plan.recompute_schema())
            .data()
    }

    fn name(&self) -> &str {
        "ModelAnalyzeRule"
    }
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

    /// The goal of this function is to analyze the scope of the logical plan and collect the required columns for models and visited tables.
    /// If the plan contains subquery, we should create a new child scope and analyze the subquery recursively.
    /// After leaving the subquery, we should push(push_back) the child scope to the scope_queue.
    fn analyze_scope(
        &self,
        plan: LogicalPlan,
        root: &RefCell<Scope>,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(&mut |plan| -> Result<Transformed<LogicalPlan>> {
            let plan = self.analyze_scope_internal(plan, root)?.data;
            plan.map_subqueries(|plan| {
                if let LogicalPlan::Subquery(Subquery {
                    subquery,
                    outer_ref_columns,
                }) = &plan
                {
                    outer_ref_columns.iter().try_for_each(|expr| {
                        let mut root_mut = root.borrow_mut();
                        self.collect_required_column(expr.clone(), &mut root_mut)
                    })?;
                    let child_scope =
                        RefCell::new(Scope::new_child(RefCell::clone(root)));
                    self.analyze_scope(
                        Arc::unwrap_or_clone(Arc::clone(subquery)),
                        &child_scope,
                    )?;
                    let mut root_mut = root.borrow_mut();
                    root_mut.push_child(child_scope);
                }
                Ok(Transformed::no(plan))
            })
        })
    }

    /// Collect the visited dataset and required columns
    fn analyze_scope_internal(
        &self,
        plan: LogicalPlan,
        scope: &RefCell<Scope>,
    ) -> Result<Transformed<LogicalPlan>> {
        match &plan {
            LogicalPlan::TableScan(table_scan) => {
                if belong_to_mdl(
                    &self.analyzed_wren_mdl.wren_mdl(),
                    table_scan.table_name.clone(),
                    Arc::clone(&self.session_state),
                ) {
                    let mut scope_mut = scope.borrow_mut();
                    if let Some(model) = self
                        .analyzed_wren_mdl
                        .wren_mdl
                        .get_model(table_scan.table_name.table())
                    {
                        scope_mut.add_visited_dataset(
                            table_scan.table_name.clone(),
                            Dataset::Model(model),
                        );
                    }
                    scope_mut.add_visited_table(table_scan.table_name.clone());
                    Ok(Transformed::no(plan))
                } else {
                    Ok(Transformed::no(plan))
                }
            }
            LogicalPlan::Join(Join { on, filter, .. }) => {
                let mut scope_mut = scope.borrow_mut();
                let mut accum = HashSet::new();
                on.iter().try_for_each(|expr| {
                    expr_to_columns(&expr.0, &mut accum)?;
                    expr_to_columns(&expr.1, &mut accum)?;
                    Ok::<_, DataFusionError>(())
                })?;
                if let Some(filter_expr) = &filter {
                    expr_to_columns(filter_expr, &mut accum)?;
                }
                accum.iter().try_for_each(|expr| {
                    self.collect_required_column(
                        Expr::Column(expr.clone()),
                        &mut scope_mut,
                    )
                })?;
                Ok(Transformed::no(plan))
            }
            LogicalPlan::Projection(projection) => {
                let mut scope_mut = scope.borrow_mut();
                projection.expr.iter().try_for_each(|expr| {
                    let mut acuum = HashSet::new();
                    expr_to_columns(expr, &mut acuum)?;
                    acuum.into_iter().try_for_each(|expr| {
                        self.collect_required_column(Expr::Column(expr), &mut scope_mut)
                    })
                })?;
                Ok(Transformed::no(plan))
            }
            LogicalPlan::Filter(filter) => {
                let mut scope_mut = scope.borrow_mut();
                let mut acuum = HashSet::new();
                expr_to_columns(&filter.predicate, &mut acuum)?;
                acuum.into_iter().try_for_each(|expr| {
                    self.collect_required_column(Expr::Column(expr), &mut scope_mut)
                })?;
                Ok(Transformed::no(plan))
            }
            LogicalPlan::Aggregate(aggregate) => {
                let mut scope_mut = scope.borrow_mut();
                let mut accum = HashSet::new();
                aggregate.aggr_expr.iter().for_each(|expr| {
                    Expr::add_column_refs(expr, &mut accum);
                });
                aggregate.group_expr.iter().for_each(|expr| {
                    Expr::add_column_refs(expr, &mut accum);
                });
                accum.iter().try_for_each(|expr| {
                    self.collect_required_column(
                        Expr::Column(expr.to_owned().clone()),
                        &mut scope_mut,
                    )
                })?;
                Ok(Transformed::no(plan))
            }
            LogicalPlan::SubqueryAlias(subquery_alias) => {
                let mut scope_mut = scope.borrow_mut();
                if let LogicalPlan::TableScan(table_scan) =
                    Arc::unwrap_or_clone(Arc::clone(&subquery_alias.input))
                {
                    if belong_to_mdl(
                        &self.analyzed_wren_mdl.wren_mdl(),
                        table_scan.table_name.clone(),
                        Arc::clone(&self.session_state),
                    ) {
                        if let Some(model) = self
                            .analyzed_wren_mdl
                            .wren_mdl
                            .get_model(table_scan.table_name.table())
                        {
                            scope_mut.add_visited_dataset(
                                subquery_alias.alias.clone(),
                                Dataset::Model(model),
                            );
                        }
                    }
                }
                scope_mut.add_visited_table(subquery_alias.alias.clone());
                Ok(Transformed::no(plan))
            }
            LogicalPlan::Window(window) => {
                let mut scope_mut = scope.borrow_mut();
                window
                    .window_expr
                    .iter()
                    .fold(HashSet::new(), |mut set, expr| {
                        Expr::add_column_refs(expr, &mut set);
                        set
                    })
                    .into_iter()
                    .try_for_each(|col| {
                        self.collect_required_column(
                            Expr::Column(col.to_owned()),
                            &mut scope_mut,
                        )
                    })?;
                Ok(Transformed::no(plan))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }

    /// This function only collects the model required columns
    fn collect_required_column(
        &self,
        expr: Expr,
        scope: &mut RefMut<Scope>,
    ) -> Result<()> {
        match expr {
            Expr::Column(Column {
                relation: Some(relation),
                name,
                ..
            }) => {
                // only collect the required column if the relation belongs to the mdl
                if belong_to_mdl(
                    &self.analyzed_wren_mdl.wren_mdl(),
                    relation.clone(),
                    Arc::clone(&self.session_state),
                ) && self
                    .analyzed_wren_mdl
                    .wren_mdl()
                    .get_view(relation.table())
                    .is_none()
                {
                    let added = scope.add_required_column(
                        relation.clone(),
                        Expr::Column(Column::new(Some(relation.clone()), name)),
                    )?;
                    if !added {
                        return plan_err!("Relation {} isn't visited", relation);
                    }
                }
            }
            // It is possible that the column is a rebase column from the aggregation or join
            // e.g. Column {
            //         relation: None,
            //         name: "min(wrenai.public.order_items_model.price)",
            //     },
            Expr::Column(Column { relation: None, .. }) => {
                // do nothing
            }
            Expr::OuterReferenceColumn(_, column) => {
                self.collect_required_column(Expr::Column(column), scope)?;
            }
            _ => return plan_err!("Invalid column expression: {}", expr),
        }
        Ok(())
    }

    /// Analyze the table scan and rewrite the table scan to the ModelPlanNode according to the scope analysis.
    /// If the plan contains subquery, we should analyze the subquery recursively.
    /// Before enter the subquery, the corresponding child scope should be popped (pop_front) from the scope_queue.
    fn analyze_model(
        &self,
        plan: LogicalPlan,
        root: &RefCell<Scope>,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(&mut |plan| -> Result<Transformed<LogicalPlan>> {
            let plan = self.analyze_model_internal(plan, root)?.data;
            // If the plan contains subquery, we should analyze the subquery recursively
            let mut root = root.borrow_mut();
            plan.map_subqueries(|plan| {
                if let LogicalPlan::Subquery(subquery) = &plan {
                    let Some(child_scope) = root.pop_child() else {
                        return internal_err!("No child scope found for subquery");
                    };
                    let transformed = self
                        .analyze_model(
                            Arc::unwrap_or_clone(Arc::clone(&subquery.subquery)),
                            &child_scope,
                        )?
                        .data;
                    return Ok(Transformed::yes(LogicalPlan::Subquery(
                        subquery.with_plan(Arc::new(transformed)),
                    )));
                }
                Ok(Transformed::no(plan))
            })
        })
    }

    /// Analyze the model and generate the ModelPlanNode
    fn analyze_model_internal(
        &self,
        plan: LogicalPlan,
        scope: &RefCell<Scope>,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
                // Because the bottom-up transformation is used, the table_scan is already transformed
                // to the ModelPlanNode before the SubqueryAlias. We should check the patten of Wren-generated model plan like:
                //      SubqueryAlias -> SubqueryAlias -> Extension -> ModelPlanNode
                // to get the correct required columns
                match Arc::unwrap_or_clone(Arc::clone(&input)) {
                    LogicalPlan::SubqueryAlias(subquery_alias) => {
                        self.analyze_subquery_alias_model(subquery_alias, scope, alias)
                    }
                    LogicalPlan::TableScan(table_scan) => {
                        let model_plan = self
                            .analyze_table_scan(
                                Arc::clone(&self.analyzed_wren_mdl),
                                Arc::clone(&self.session_state),
                                table_scan,
                                Some(alias.clone()),
                                scope,
                            )?
                            .data;
                        let subquery =
                            LogicalPlanBuilder::from(model_plan).alias(alias)?.build()?;
                        Ok(Transformed::yes(subquery))
                    }
                    _ => Ok(Transformed::no(LogicalPlan::SubqueryAlias(
                        SubqueryAlias::try_new(input, alias)?,
                    ))),
                }
            }
            LogicalPlan::TableScan(table_scan) => self.analyze_table_scan(
                Arc::clone(&self.analyzed_wren_mdl),
                Arc::clone(&self.session_state),
                table_scan,
                None,
                scope,
            ),
            LogicalPlan::Join(join) => {
                let left = match Arc::unwrap_or_clone(join.left) {
                    LogicalPlan::TableScan(table_scan) => {
                        self.analyze_table_scan(
                            Arc::clone(&self.analyzed_wren_mdl),
                            Arc::clone(&self.session_state),
                            table_scan,
                            None,
                            scope,
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
                            scope,
                        )?
                        .data
                    }
                    ignore => ignore,
                };
                Ok(Transformed::yes(LogicalPlan::Join(Join {
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

    fn analyze_table_scan(
        &self,
        analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
        session_state_ref: SessionStateRef,
        table_scan: TableScan,
        alias: Option<TableReference>,
        scope: &RefCell<Scope>,
    ) -> Result<Transformed<LogicalPlan>> {
        if belong_to_mdl(
            &analyzed_wren_mdl.wren_mdl(),
            table_scan.table_name.clone(),
            Arc::clone(&session_state_ref),
        ) {
            let table_name = table_scan.table_name.table();
            if let Some(model) = analyzed_wren_mdl.wren_mdl.get_model(table_name) {
                let table_ref = alias.unwrap_or(table_scan.table_name.clone());
                let scope = scope.borrow();
                let field: Vec<Expr> = if let Some(used_columns) =
                    scope.try_get_required_columns(&table_ref)
                {
                    used_columns.iter().cloned().collect()
                } else {
                    // If the required columns are not found in the current scope but the table is visited,
                    // it could be a count(*) query
                    if scope.try_get_visited_dataset(&table_ref).is_none() {
                        return internal_err!(
                            "Table {} not found in the visited dataset and required columns map",
                            table_ref
                        );
                    };
                    vec![]
                };
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
                Ok(Transformed::yes(subquery))
            } else {
                Ok(Transformed::no(LogicalPlan::TableScan(table_scan)))
            }
        } else {
            Ok(Transformed::no(LogicalPlan::TableScan(table_scan)))
        }
    }

    /// Because the bottom-up transformation is used, the table_scan is already transformed
    /// to the ModelPlanNode before the SubqueryAlias. We should check the patten of Wren-generated model plan like:
    ///      SubqueryAlias -> SubqueryAlias -> Extension -> ModelPlanNode
    /// to get the correct required columns
    fn analyze_subquery_alias_model(
        &self,
        subquery_alias: SubqueryAlias,
        scope: &RefCell<Scope>,
        alias: TableReference,
    ) -> Result<Transformed<LogicalPlan>> {
        let SubqueryAlias { input, .. } = subquery_alias;
        if let LogicalPlan::Extension(Extension { node }) =
            Arc::unwrap_or_clone(Arc::clone(&input))
        {
            if let Some(model_node) = node.as_any().downcast_ref::<ModelPlanNode>() {
                if let Some(model) = self
                    .analyzed_wren_mdl
                    .wren_mdl()
                    .get_model(model_node.plan_name())
                {
                    let scope = scope.borrow();
                    let field: Vec<Expr> = if let Some(used_columns) =
                        scope.try_get_required_columns(&alias)
                    {
                        used_columns.iter().cloned().collect()
                    } else {
                        // If the required columns are not found in the current scope but the table is visited,
                        // it could be a count(*) query
                        if scope.try_get_visited_dataset(&alias).is_none() {
                            return internal_err!(
                                    "Table {} not found in the visited dataset and required columns map",
                                    alias);
                        };
                        vec![]
                    };
                    let model_plan = LogicalPlan::Extension(Extension {
                        node: Arc::new(ModelPlanNode::new(
                            Arc::clone(&model),
                            field,
                            None,
                            Arc::clone(&self.analyzed_wren_mdl),
                            Arc::clone(&self.session_state),
                        )?),
                    });
                    let subquery =
                        LogicalPlanBuilder::from(model_plan).alias(alias)?.build()?;
                    Ok(Transformed::yes(subquery))
                } else {
                    internal_err!(
                        "Model {} not found in the WrenMDL",
                        model_node.plan_name()
                    )
                }
            } else {
                internal_err!("ModelPlanNode not found in the Extension node")
            }
        } else {
            Ok(Transformed::no(LogicalPlan::SubqueryAlias(
                SubqueryAlias::try_new(input, alias)?,
            )))
        }
    }

    /// Remove the catalog and schema prefix of Wren for the column and refresh the schema.
    /// The plan created by DataFusion is always with the Wren prefix for the column name.
    /// Something like "wrenai.public.order_items_model.price". However, the model plan will be rewritten to a subquery alias
    /// The catalog and schema are invalid for the subquery alias. We should remove the prefix and refresh the schema.
    fn remove_wren_catalog_schema_prefix_and_refresh_schema(
        &self,
        plan: LogicalPlan,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
                let subquery = self
                    .remove_wren_catalog_schema_prefix_and_refresh_schema(
                        Arc::unwrap_or_clone(input),
                    )?
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
                    .remove_wren_catalog_schema_prefix_and_refresh_schema(
                        Arc::unwrap_or_clone(subquery),
                    )?
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
            Expr::Column(Column { relation, name, .. }) => {
                if let Some(relation) = relation {
                    Ok(self.rewrite_column_qualifier(relation, name, alias_model))
                } else {
                    let name = name.replace(
                        self.analyzed_wren_mdl.wren_mdl().catalog_schema_prefix(),
                        "",
                    );
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
                let name = name.replace(
                    self.analyzed_wren_mdl.wren_mdl().catalog_schema_prefix(),
                    "",
                );
                Transformed::yes(Expr::Column(Column::new(
                    Some(TableReference::bare(relation.table())),
                    &name,
                )))
            }
        } else {
            Transformed::no(Expr::Column(Column {
                relation: Some(relation),
                name,
                spans: Spans::new(),
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

/// [Scope] is used to collect the required columns for models and visited tables in a query scope.
/// A query scope means is a full query body contain projection, relation. e.g.
///    SELECT a, b, c FROM table
///
/// To avoid the table name be ambiguous, the relation name should be unique in the scope.
/// The relation of parent scope can be accessed by the child scope.
/// The child scope can also add the required columns to the parent scope.
#[derive(Clone, Debug, Default)]
pub struct Scope {
    /// The columns required by the dataset
    required_columns: HashMap<TableReference, HashSet<Expr>>,
    /// The Wren dataset visited in the scope (only the Wren dataset)
    visited_dataset: HashMap<TableReference, Dataset>,
    /// The table name visited in the scope (not only the Wren dataset)
    visited_tables: HashSet<TableReference>,
    /// The parent scope
    parent: Option<Box<RefCell<Scope>>>,
    childs: VecDeque<RefCell<Scope>>,
}

impl Scope {
    pub fn new() -> Self {
        Self {
            required_columns: HashMap::new(),
            visited_dataset: HashMap::new(),
            visited_tables: HashSet::new(),
            parent: None,
            childs: VecDeque::new(),
        }
    }

    pub fn new_child(parent: RefCell<Scope>) -> Self {
        Self {
            required_columns: HashMap::new(),
            visited_dataset: HashMap::new(),
            visited_tables: HashSet::new(),
            parent: Some(Box::new(parent)),
            childs: VecDeque::new(),
        }
    }

    pub fn pop_child(&mut self) -> Option<RefCell<Scope>> {
        self.childs.pop_front()
    }

    pub fn push_child(&mut self, child: RefCell<Scope>) {
        self.childs.push_back(child);
    }

    /// Add the required column to the scope, return true if the column is added successfully.
    /// If the table isn't exist in the current scope, try to add the column to the parent scope.
    /// If the table is not visited by the parent and the current scope, return false
    pub fn add_required_column(
        &mut self,
        table_ref: TableReference,
        expr: Expr,
    ) -> Result<bool> {
        let added = if self.visited_dataset.contains_key(&table_ref) {
            self.required_columns
                .entry(table_ref.clone())
                .or_default()
                .insert(expr);
            true
        } else if let Some(ref parent) = &self.parent {
            parent
                .clone()
                .borrow_mut()
                .add_required_column(table_ref.clone(), expr)?
        } else {
            false
        };

        if added {
            Ok(true)
        } else if self.try_get_visited_table(&table_ref).is_some() {
            // If the table is visited but the dataset is not found, it could be a subquery alias
            Ok(true)
        } else {
            // the table is not visited by both the parent and the current scope
            Ok(false)
        }
    }

    pub fn add_visited_dataset(&mut self, table_ref: TableReference, dataset: Dataset) {
        self.visited_dataset.insert(table_ref, dataset);
    }

    pub fn add_visited_table(&mut self, table_ref: TableReference) {
        self.visited_tables.insert(table_ref);
    }

    pub fn try_get_required_columns(
        &self,
        table_ref: &TableReference,
    ) -> Option<HashSet<Expr>> {
        let try_local = self.required_columns.get(table_ref).cloned();

        if try_local.is_some() {
            return try_local;
        }

        if let Some(ref parent) = &self.parent {
            let scope = parent.borrow();
            scope.try_get_required_columns(table_ref)
        } else {
            None
        }
    }

    pub fn try_get_visited_dataset(&self, table_ref: &TableReference) -> Option<Dataset> {
        let try_local = self.visited_dataset.get(table_ref).cloned();

        if try_local.is_some() {
            return try_local;
        }

        if let Some(ref parent) = &self.parent {
            let scope = parent.borrow();
            scope.try_get_visited_dataset(table_ref)
        } else {
            None
        }
    }

    pub fn try_get_visited_table(
        &self,
        table_ref: &TableReference,
    ) -> Option<TableReference> {
        if self.visited_tables.contains(table_ref) {
            return Some(table_ref.clone());
        }

        if let Some(ref parent) = &self.parent {
            let scope = parent.borrow();
            scope.try_get_visited_table(table_ref)
        } else {
            None
        }
    }
}
