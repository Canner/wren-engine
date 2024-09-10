use crate::logical_plan::analyze::plan::ModelPlanNode;
use crate::mdl::utils::quoted;
use crate::mdl::{AnalyzedWrenMDL, Dataset, SessionStateRef, WrenMDL};
use datafusion::catalog_common::TableReference;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{internal_err, plan_err, Column, DFSchemaRef, Result};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::utils::expr_to_columns;
use datafusion::logical_expr::{
    col, ident, Aggregate, Distinct, DistinctOn, Expr, Extension, Filter, Join,
    LogicalPlan, LogicalPlanBuilder, Projection, Subquery, SubqueryAlias, TableScan,
    Window,
};
use datafusion::optimizer::AnalyzerRule;
use std::cell::{RefCell, RefMut};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

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

    fn analyze_scope(
        &self,
        plan: LogicalPlan,
        root: &RefCell<Scope>,
        scope_buffer: &RefCell<VecDeque<RefCell<Scope>>>,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(&|plan| -> Result<Transformed<LogicalPlan>> {
            let plan = self.analyze_scope_internal(plan, &root, scope_buffer)?.data;
            plan.map_subqueries(|plan| {
                let child_scope = RefCell::new(Scope::new_child(RefCell::clone(root)));
                let plan = self.analyze_scope(plan, &child_scope, scope_buffer)?.data;
                let mut scope_buffer = scope_buffer.borrow_mut();
                scope_buffer.push_back(child_scope);
                Ok(Transformed::no(plan))
            })
        })
    }

    /// Collect the visited dataset and required columns
    fn analyze_scope_internal(
        &self,
        plan: LogicalPlan,
        scope: &RefCell<Scope>,
        scope_buffer: &RefCell<VecDeque<RefCell<Scope>>>,
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
                        )?;
                    }
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
            LogicalPlan::Subquery(subquery) => {
                let mut scope_mut = scope.borrow_mut();
                subquery.outer_ref_columns.iter().try_for_each(|expr| {
                    self.collect_required_column(expr.clone(), &mut scope_mut)
                })?;
                // create a new scope for the subquery
                let child_scope = RefCell::new(Scope::new_child(RefCell::clone(&scope)));
                let plan = self.analyze_scope(plan, &child_scope, scope_buffer)?.data;
                let mut scope_buffer = scope_buffer.borrow_mut();
                scope_buffer.push_back(child_scope);
                Ok(Transformed::no(plan))
            }
            LogicalPlan::SubqueryAlias(subquery_alias) => {
                if let LogicalPlan::TableScan(table_scan) =
                    Arc::unwrap_or_clone(Arc::clone(&subquery_alias.input))
                {
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
                                subquery_alias.alias.clone(),
                                Dataset::Model(model),
                            )?;
                        }
                    }
                }
                Ok(Transformed::no(plan))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }

    fn collect_required_column(
        &self,
        expr: Expr,
        scope: &mut RefMut<Scope>,
    ) -> Result<()> {
        match expr {
            Expr::Column(Column {
                relation: Some(relation),
                name,
            }) => {
                scope.add_required_column(
                    relation.clone(),
                    Expr::Column(Column::new(Some(relation), name)),
                )?;
            }
            // It is possible that the column is a rebase column from the aggregation or join
            // e.g. Column {
            //         relation: None,
            //         name: "min(wrenai.public.order_items_model.price)",
            //     },
            Expr::Column(Column {relation: None, ..}) => {
                // do nothing
            }
            Expr::OuterReferenceColumn(_, column) => {
                self.collect_required_column(Expr::Column(column), scope)?;
            }
            _ => return plan_err!("Invalid column expression: {}", expr),
        }
        Ok(())
    }

    fn analyze_model(
        &self,
        plan: LogicalPlan,
        root: &RefCell<Scope>,
        scope_buffer: &RefCell<VecDeque<RefCell<Scope>>>,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(&|plan| -> Result<Transformed<LogicalPlan>> {
            let plan = self.analyze_model_internal(plan, root, scope_buffer)?.data;
            plan.map_subqueries(|plan| {
                let mut scope_buffer_mut = scope_buffer.borrow_mut();
                let Some(child_scope) = scope_buffer_mut.pop_front() else {
                    return internal_err!("No child scope found for subquery");
                };
                let plan = self.analyze_model(plan, &child_scope, scope_buffer)?.data;
                Ok(Transformed::no(plan))
            })
        })
    }

    /// Analyze the model and generate the ModelPlanNode
    fn analyze_model_internal(
        &self,
        plan: LogicalPlan,
        scope: &RefCell<Scope>,
        scope_buffer: &RefCell<VecDeque<RefCell<Scope>>>,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
                match Arc::unwrap_or_clone(input) {
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
            LogicalPlan::Subquery(Subquery { subquery, .. }) => {
                let mut scope_buffer_mut = scope_buffer.borrow_mut();
                let Some(child_scope) = scope_buffer_mut.pop_front() else {
                    return internal_err!("No child scope found for subquery");
                };
                self.analyze_model(
                    Arc::unwrap_or_clone(subquery),
                    &child_scope,
                    scope_buffer,
                )
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
        scope: &RefCell<Scope>,
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
                let scope = scope.borrow();
                let Some(used_columns) = scope.try_get_required_columns(&table_ref)
                else {
                    return internal_err!(
                        "Table {} not found in the required columns",
                        table_ref
                    );
                };
                let field = used_columns.iter().cloned().collect();
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
        let mut queue = RefCell::new(VecDeque::new());
        let mut root = RefCell::new(Scope::new());
        self.analyze_scope(plan, &mut root, &mut queue)?
            .map_data(|plan| self.analyze_model(plan, &root, &mut queue).data())?
            .map_data(|plan| {
                plan.transform_up_with_subqueries(&|plan| -> Result<
                    Transformed<LogicalPlan>,
                > {
                    self.replace_model_prefix_and_refresh_schema(plan)
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
#[derive(Clone)]
pub struct Scope {
    /// The columns required by the dataset
    required_columns: HashMap<TableReference, HashSet<Expr>>,
    visited_dataset: HashMap<TableReference, Dataset>,
    parent: Option<Box<RefCell<Scope>>>,
}

impl Scope {
    pub fn new() -> Self {
        Self {
            required_columns: HashMap::new(),
            visited_dataset: HashMap::new(),
            parent: None,
        }
    }

    pub fn new_child(parent: RefCell<Scope>) -> Self {
        Self {
            required_columns: HashMap::new(),
            visited_dataset: HashMap::new(),
            parent: Some(Box::new(parent)),
        }
    }

    pub fn add_required_column(
        &mut self,
        table_ref: TableReference,
        expr: Expr,
    ) -> Result<()> {
        if self.visited_dataset.contains_key(&table_ref) {
            self.required_columns
                .entry(table_ref)
                .or_insert(HashSet::new())
                .insert(expr);
            Ok(())
        } else if let Some(ref parent) = &self.parent {
            parent
                .clone()
                .borrow_mut()
                .add_required_column(table_ref, expr)?;
            Ok(())
        } else {
            plan_err!("Table {} not found in the visited dataset", table_ref)
        }
    }

    pub fn add_visited_dataset(
        &mut self,
        table_ref: TableReference,
        dataset: Dataset,
    ) -> Result<()> {
        if self.visited_dataset.contains_key(&table_ref) {
            return plan_err!("Table {} already visited", table_ref);
        }
        self.visited_dataset.insert(table_ref, dataset);
        Ok(())
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
}
