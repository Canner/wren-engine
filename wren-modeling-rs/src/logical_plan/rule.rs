use crate::logical_plan::rule::RelationChain::Nil;
use crate::mdl;
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
use datafusion::prelude::Column;
use datafusion::sql::TableReference;
use petgraph::Graph;
use std::cell::RefCell;
use std::cmp::PartialEq;
use std::collections::{HashSet, VecDeque};
use std::{collections::HashMap, fmt, fmt::Debug, sync::Arc};

use crate::mdl::lineage::DatasetLink;
use crate::mdl::manifest::{JoinType, Model};
use crate::mdl::utils::is_dag;
use crate::mdl::{AnalyzedWrenMDL, Dataset};

use super::utils::{create_remote_table_source, map_data_type};

/// Recognized the model. Turn TableScan from a model to a ModelPlanNode.
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
                if let Some(model) = self
                    .analyzed_wren_mdl
                    .wren_mdl
                    .get_model(table_scan.table_name.to_string().as_str())
                {
                    let model = LogicalPlan::Extension(Extension {
                        node: Arc::new(ModelPlanNode::new(
                            model,
                            used_columns.borrow().iter().cloned().collect(),
                            Some(LogicalPlan::TableScan(table_scan.clone())),
                            Arc::clone(&self.analyzed_wren_mdl),
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
                            .analyzed_wren_mdl
                            .wren_mdl
                            .get_model(table_scan.table_name.to_string().as_str())
                        {
                            LogicalPlan::Extension(Extension {
                                node: Arc::new(ModelPlanNode::new(
                                    model,
                                    buffer.iter().cloned().collect(),
                                    Some(LogicalPlan::TableScan(table_scan.clone())),
                                    Arc::clone(&self.analyzed_wren_mdl),
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
                            .analyzed_wren_mdl
                            .wren_mdl
                            .get_model(table_scan.table_name.to_string().as_str())
                        {
                            LogicalPlan::Extension(Extension {
                                node: Arc::new(ModelPlanNode::new(
                                    model,
                                    buffer.iter().cloned().collect(),
                                    Some(LogicalPlan::TableScan(table_scan.clone())),
                                    Arc::clone(&self.analyzed_wren_mdl),
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

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
struct ModelPlanNode {
    model_name: String,
    required_exprs: Vec<Expr>,
    relation_chain: Box<RelationChain>,
    schema_ref: DFSchemaRef,
    original_table_scan: Option<LogicalPlan>,
}

impl ModelPlanNode {
    pub fn new(
        model: Arc<Model>,
        requried_fields: Vec<Expr>,
        original_table_scan: Option<LogicalPlan>,
        analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
    ) -> Self {
        let mut required_exprs_buffer = HashSet::new();
        let mut directed_graph: Graph<Dataset, DatasetLink> = Graph::new();
        let mut model_required_fields: HashMap<String, HashSet<Column>> = HashMap::new();
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
                required_exprs_buffer.insert(expr_plan);
                if column.is_calculated {
                    let qualified_column =
                        Column::from_qualified_name(format!("{}.{}", model.name, column.name));
                    match analyzed_wren_mdl
                        .lineage
                        .required_dataset_topo
                        .get(&qualified_column)
                    {
                        Some(column_graph) => {
                            merge_graph(&mut directed_graph, column_graph);
                        }
                        None => {
                            panic!("Column {} not found in the lineage", qualified_column)
                        }
                    }
                    analyzed_wren_mdl
                        .lineage
                        .required_fields_map
                        .get(&qualified_column)
                        .unwrap()
                        .iter()
                        .for_each(|c| {
                            let relation_name = match &c.relation {
                                Some(r) => r.to_string(),
                                None => panic!("Source dataset not found for column {}", c),
                            };
                            model_required_fields
                                .entry(relation_name)
                                .or_insert(HashSet::new())
                                .insert(c.clone());
                        });
                }
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

        if !is_dag(&directed_graph) {
            panic!("cyclic dependency detected: {}", model.name);
        }

        let schema_ref = DFSchemaRef::new(
            DFSchema::new_with_metadata(fields, HashMap::new()).expect("create schema failed"),
        );

        let mut relation_chain = Nil;
        for edge_index in directed_graph.edge_indices() {
            let link = directed_graph.edge_weight(edge_index).unwrap();

            // check if the chain is valid
            match &relation_chain {
                Nil => {
                    if link.source.get_name() != model.name {
                        panic!("Relation chain should start with source model");
                    }
                    let required_filed = model_required_fields
                        .get(model.name.as_str())
                        .unwrap()
                        .into_iter()
                        .map(|c| Expr::Column(c.clone()))
                        .collect();
                    match &link.source {
                        Dataset::Model(source_model) => {
                            relation_chain = RelationChain::Chain(
                                ModelPlanNode::new(
                                    Arc::clone(source_model),
                                    required_filed,
                                    None,
                                    Arc::clone(&analyzed_wren_mdl),
                                ),
                                link.join_type.clone(),
                                link.condition.clone(),
                                Box::new(relation_chain),
                            );
                        }
                        _ => {
                            unimplemented!("Only support model as source dataset")
                        }
                    }
                }
                RelationChain::Chain(plan, ..) => {
                    if link.source.get_name() != plan.model_name {
                        panic!("Relation chain should start with model");
                    }
                }
            }

            match &link.target {
                Dataset::Model(target_model) => {
                    let required_filed = model_required_fields
                        .get(target_model.name.as_str())
                        .unwrap()
                        .into_iter()
                        .map(|c| Expr::Column(c.clone()))
                        .collect();
                    relation_chain = RelationChain::Chain(
                        ModelPlanNode::new(
                            Arc::clone(target_model),
                            required_filed,
                            None,
                            Arc::clone(&analyzed_wren_mdl),
                        ),
                        link.join_type.clone(),
                        link.condition.clone(),
                        Box::new(relation_chain),
                    );
                }
                _ => {
                    unimplemented!("Only support model as target dataset")
                }
            }
        }

        Self {
            model_name: model.name.clone(),
            required_exprs: required_exprs_buffer.into_iter().collect(),
            relation_chain: Box::new(relation_chain),
            schema_ref,
            original_table_scan,
        }
    }
}

fn merge_graph(graph: &mut Graph<Dataset, DatasetLink>, new_graph: &Graph<Dataset, DatasetLink>) {
    let mut node_map = HashMap::new();
    for node in new_graph.node_indices() {
        let new_node = graph.add_node(new_graph[node].clone());
        node_map.insert(node, new_node);
    }

    for edge in new_graph.edge_indices() {
        let (source, target) = new_graph.edge_endpoints(edge).unwrap();
        let source = node_map.get(&source).unwrap();
        let target = node_map.get(&target).unwrap();
        graph.add_edge(*source, *target, new_graph[edge].clone());
    }
}

#[derive(Eq, PartialEq, Debug, Hash, Clone)]
enum RelationChain {
    Chain(ModelPlanNode, JoinType, String, Box<RelationChain>),
    Nil,
}

impl RelationChain {
    fn plan(&mut self, rule: ModelGenerationRule) -> (Option<LogicalPlan>, Option<Expr>) {
        match self {
            RelationChain::Chain(plan, _, condition, ref mut next) => {
                if let Nil = **next {
                    let join_keys: Vec<Expr> = mdl::utils::collect_identifiers(condition)
                        .iter()
                        .cloned()
                        .map(|c| col(c.flat_name()))
                        .collect();
                    let join_condition = join_keys[0].clone().eq(join_keys[1].clone());
                    let left = rule
                        .generate_model_internal(LogicalPlan::Extension(Extension {
                            node: Arc::new(plan.to_owned()),
                        }))
                        .unwrap()
                        .data;
                    (Some(left), Some(join_condition))
                } else {
                    let left = rule
                        .generate_model_internal(LogicalPlan::Extension(Extension {
                            node: Arc::new(plan.to_owned()),
                        }))
                        .unwrap()
                        .data;
                    let join_keys: Vec<Expr> = mdl::utils::collect_identifiers(condition)
                        .iter()
                        .cloned()
                        .map(|c| col(c.flat_name()))
                        .collect();
                    let join_condition = join_keys[0].clone().eq(join_keys[1].clone());
                    let (Some(next_plan), _) = next.plan(rule) else {
                        panic!("Nil relation chain")
                    };
                    (
                        Some(
                            LogicalPlanBuilder::from(left)
                                .join_on(
                                    next_plan,
                                    datafusion::logical_expr::JoinType::Left,
                                    vec![join_condition],
                                )
                                .unwrap()
                                .build()
                                .unwrap(),
                        ),
                        None,
                    )
                }
            }
            _ => (None, None),
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
            relation_chain: self.relation_chain.clone(),
            schema_ref: self.schema_ref.clone(),
            original_table_scan: self.original_table_scan.clone(),
        }
    }
}

// Generate the query plan for the ModelPlanNode
pub struct ModelGenerationRule {
    analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
}

impl ModelGenerationRule {
    pub fn new(mdl: Arc<AnalyzedWrenMDL>) -> Self {
        Self {
            analyzed_wren_mdl: mdl,
        }
    }

    fn generate_model_internal(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Extension(extension) => {
                if let Some(model_plan) = extension.node.as_any().downcast_ref::<ModelPlanNode>() {
                    let model: Arc<Model> = Arc::clone(
                        &self
                            .analyzed_wren_mdl
                            .wren_mdl
                            .get_model(&model_plan.model_name)
                            .expect("Model not found"),
                    );
                    // support table reference
                    let table_scan = match &model_plan.original_table_scan {
                        Some(LogicalPlan::TableScan(original_scan)) => {
                            LogicalPlanBuilder::scan_with_filters(
                                model.name.clone(),
                                create_remote_table_source(
                                    &model,
                                    &self.analyzed_wren_mdl.wren_mdl,
                                ),
                                None,
                                original_scan.filters.clone(),
                            )
                            .unwrap()
                            .build()
                        }
                        Some(_) => Err(datafusion::error::DataFusionError::Internal(
                            "ModelPlanNode should have a TableScan as original_table_scan"
                                .to_string(),
                        )),
                        None => LogicalPlanBuilder::scan(
                            model.name.clone(),
                            create_remote_table_source(&model, &self.analyzed_wren_mdl.wren_mdl),
                            None,
                        )
                        .unwrap()
                        .build(),
                    }?;

                    // it could be count(*) query
                    if model_plan.required_exprs.is_empty() {
                        return Ok(Transformed::no(table_scan));
                    }


                    // join relationship plan
                    let (join_plan, condition_opt) =
                        model_plan
                            .relation_chain
                            .clone()
                            .plan(ModelGenerationRule::new(Arc::clone(
                                &self.analyzed_wren_mdl,
                            )));

                    dbg!(&join_plan);
                    // calculated field scope
                    let result = match join_plan {
                        Some(plan) => LogicalPlanBuilder::from(plan)
                            .project(model_plan.required_exprs.clone())?
                            .build()?,
                        None => LogicalPlanBuilder::from(table_scan)
                            .project(model_plan.required_exprs.clone())?
                            .build()?,
                    };

                    let alias = LogicalPlanBuilder::from(result)
                        .alias(model.name.clone())?
                        .build()?;
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
