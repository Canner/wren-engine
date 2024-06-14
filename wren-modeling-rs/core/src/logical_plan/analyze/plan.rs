use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::sync::Arc;

use arrow_schema::Field;
use datafusion::common::{Column, DFSchema, DFSchemaRef, TableReference};
use datafusion::logical_expr::{
    col, Expr, Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNodeCore,
};
use petgraph::Graph;

use crate::logical_plan::analyze::plan::RelationChain::Start;
use crate::logical_plan::analyze::rule::ModelGenerationRule;
use crate::logical_plan::utils::{from_qualified_name, map_data_type};
use crate::mdl;
use crate::mdl::lineage::DatasetLink;
use crate::mdl::manifest::{JoinType, Model};
use crate::mdl::utils::{
    create_remote_expr_for_model, create_wren_expr_for_model, is_dag,
};
use crate::mdl::{AnalyzedWrenMDL, ColumnReference, Dataset};

/// [ModelPlanNode] is a logical plan node that represents a model. It contains the model name,
/// required fields, and the relation chain that connects the model with other models.
/// It only generates the top plan for the model, and the relation chain will generate the source plan.
#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub(crate) struct ModelPlanNode {
    pub(crate) model_name: String,
    pub(crate) required_exprs: Vec<Expr>,
    pub(crate) relation_chain: Box<RelationChain>,
    schema_ref: DFSchemaRef,
    pub(crate) original_table_scan: Option<LogicalPlan>,
}

impl ModelPlanNode {
    pub fn new(
        model: Arc<Model>,
        required_fields: Vec<Expr>,
        original_table_scan: Option<LogicalPlan>,
        analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
    ) -> Self {
        let mut required_exprs_buffer = BTreeSet::new();
        let mut directed_graph: Graph<Dataset, DatasetLink> = Graph::new();
        let mut model_required_fields: HashMap<TableReference, BTreeSet<OrdExpr>> =
            HashMap::new();
        let model_ref = TableReference::full(
            analyzed_wren_mdl.wren_mdl().catalog(),
            analyzed_wren_mdl.wren_mdl().schema(),
            model.name(),
        );
        let fields = model
            .get_physical_columns()
            .iter()
            .filter(|column| {
                required_fields.iter().any(|expr| {
                    if let Expr::Column(column_expr) = expr {
                        column_expr.name.as_str() == column.name()
                    } else {
                        false
                    }
                })
            })
            .map(|column| {
                if column.is_calculated {
                    if column.expression.is_some() {
                        let column_rf = analyzed_wren_mdl
                            .wren_mdl()
                            .get_column_reference(&from_qualified_name(
                                &analyzed_wren_mdl.wren_mdl(),
                                model.name(),
                                column.name(),
                            ));
                        let expr = mdl::utils::create_wren_calculated_field_expr(
                            column_rf.clone(),
                            Arc::clone(&analyzed_wren_mdl),
                        );
                        let expr_plan = expr.alias(column.name());
                        required_exprs_buffer.insert(OrdExpr::new(expr_plan));
                    }
                    else {
                        panic!("Only support calculated field with expression")
                    };

                    let qualified_column = from_qualified_name(
                        &analyzed_wren_mdl.wren_mdl(),
                        model.name(),
                        column.name(),
                    );

                    match analyzed_wren_mdl
                        .lineage()
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
                        .lineage()
                        .required_fields_map
                        .get(&qualified_column)
                        .unwrap()
                        .iter()
                        .for_each(|c| {
                            let Some(relation_ref) = &c.relation else {
                                panic!("Source dataset not found for {}", c)
                            };
                            let ColumnReference {dataset, column} = analyzed_wren_mdl.wren_mdl().get_column_reference(c);
                            if let Dataset::Model(m) = dataset {
                                if column.is_calculated {
                                    let expr_plan = if let Some(expression) = &column.expression {
                                        create_wren_expr_for_model(
                                            expression,
                                            Arc::clone(&m),
                                            Arc::clone(&analyzed_wren_mdl))
                                    } else {
                                        panic!("Only support calculated field with expression")
                                    }.alias(column.name.clone());
                                    model_required_fields
                                        .entry(relation_ref.clone())
                                        .or_default()
                                        .insert(OrdExpr::new(expr_plan));
                                }
                                else {
                                    let expr_plan = get_remote_column_exp(
                                        &column,
                                        Arc::clone(&m),
                                        Arc::clone(&analyzed_wren_mdl));
                                    model_required_fields
                                        .entry(relation_ref.clone())
                                        .or_default()
                                        .insert(OrdExpr::new(expr_plan));
                                }
                            }
                            else {
                                panic!("Only support model as source dataset")
                            };
                        });
                } else {
                    let expr_plan = get_remote_column_exp(column, Arc::clone(&model), Arc::clone(&analyzed_wren_mdl));
                    model_required_fields
                        .entry(model_ref.clone())
                        .or_default()
                        .insert(OrdExpr::new(expr_plan.clone()));
                    let expr_plan = Expr::Column(Column::from_qualified_name(format!("{}.{}", model_ref.table(), column.name())));
                    required_exprs_buffer.insert(OrdExpr::new(expr_plan.clone()));
                }
                (
                    Some(TableReference::bare(model.name())),
                    Arc::new(Field::new(
                        column.name(),
                        map_data_type(&column.r#type),
                        column.no_null,
                    )),
                )
            })
            .collect();

        directed_graph.add_node(Dataset::Model(Arc::clone(&model)));
        if !is_dag(&directed_graph) {
            panic!("cyclic dependency detected: {}", model.name());
        }

        let schema_ref = DFSchemaRef::new(
            DFSchema::new_with_metadata(fields, HashMap::new())
                .expect("create schema failed"),
        );

        let mut iter = directed_graph.node_indices();
        let mut start = iter.next().unwrap();
        let source = directed_graph.node_weight(start).unwrap();
        let source_required_fields: Vec<Expr> = model_required_fields
            .get(&model_ref)
            .map(|c| c.iter().cloned().map(|c| c.expr).collect())
            .unwrap_or_else(Vec::new);

        let mut relation_chain = RelationChain::source(
            source,
            source_required_fields,
            Arc::clone(&analyzed_wren_mdl),
        );

        for next in iter {
            let target = directed_graph.node_weight(next).unwrap();
            let Some(link_index) = directed_graph.find_edge(start, next) else {
                break;
            };
            let link = directed_graph.edge_weight(link_index).unwrap();
            let target_ref = TableReference::full(
                analyzed_wren_mdl.wren_mdl().catalog(),
                analyzed_wren_mdl.wren_mdl().schema(),
                target.name(),
            );
            match target {
                Dataset::Model(target_model) => {
                    relation_chain = RelationChain::Chain(
                        LogicalPlan::Extension(Extension {
                            node: Arc::new(ModelSourceNode::new(
                                Arc::clone(target_model),
                                model_required_fields
                                    .get(&target_ref)
                                    .unwrap()
                                    .iter()
                                    .cloned()
                                    .map(|c| c.expr)
                                    .collect(),
                                Arc::clone(&analyzed_wren_mdl),
                                None,
                            )),
                        }),
                        link.join_type,
                        link.condition.clone(),
                        Box::new(relation_chain),
                    );
                }
                _ => {
                    unimplemented!("Only support model as source dataset")
                }
            }
            start = next;
        }

        Self {
            model_name: model.name.clone(),
            required_exprs: required_exprs_buffer
                .into_iter()
                .map(|oe| oe.expr)
                .collect(),
            relation_chain: Box::new(relation_chain),
            schema_ref,
            original_table_scan,
        }
    }
}

fn get_remote_column_exp(
    column: &mdl::manifest::Column,
    model: Arc<Model>,
    analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
) -> Expr {
    let expr = if let Some(expression) = &column.expression {
        create_remote_expr_for_model(expression, model, analyzed_wren_mdl)
    } else {
        create_remote_expr_for_model(&column.name, model, analyzed_wren_mdl)
    };
    expr.alias(column.name.clone())
}

#[derive(Eq, PartialEq, Debug, Hash, Clone)]
struct OrdExpr {
    expr: Expr,
}

impl OrdExpr {
    fn new(expr: Expr) -> Self {
        Self { expr }
    }
}

impl PartialOrd<Self> for OrdExpr {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdExpr {
    fn cmp(&self, other: &Self) -> Ordering {
        self.expr.to_string().cmp(&other.expr.to_string())
    }
}

impl From<OrdExpr> for Expr {
    fn from(val: OrdExpr) -> Self {
        val.expr
    }
}

fn merge_graph(
    graph: &mut Graph<Dataset, DatasetLink>,
    new_graph: &Graph<Dataset, DatasetLink>,
) {
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

/// RelationChain is a chain of models that are connected by the relationship.
/// The chain is used to generate the join plan for the model.
/// The physical layout will be looked like:
/// (((Model3, Model2), Model1), Nil)
#[derive(Eq, PartialEq, Debug, Hash, Clone)]
pub(crate) enum RelationChain {
    Chain(LogicalPlan, JoinType, String, Box<RelationChain>),
    Start(LogicalPlan),
}

impl RelationChain {
    pub(crate) fn source(
        dataset: &Dataset,
        required_fields: Vec<Expr>,
        analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
    ) -> Self {
        match dataset {
            Dataset::Model(source_model) => Start(LogicalPlan::Extension(Extension {
                node: Arc::new(ModelSourceNode::new(
                    Arc::clone(source_model),
                    required_fields,
                    analyzed_wren_mdl,
                    None,
                )),
            })),
            _ => {
                unimplemented!("Only support model as source dataset")
            }
        }
    }

    pub(crate) fn plan(&mut self, rule: ModelGenerationRule) -> Option<LogicalPlan> {
        match self {
            RelationChain::Chain(plan, _, condition, ref mut next) => {
                let left = rule
                    .generate_model_internal(plan.clone())
                    .expect("Failed to generate model plan")
                    .data;
                let join_keys: Vec<Expr> = mdl::utils::collect_identifiers(condition)
                    .iter()
                    .cloned()
                    .map(|c| col(c.flat_name()))
                    .collect();
                let join_condition = join_keys[0].clone().eq(join_keys[1].clone());
                let Some(right) = next.plan(rule) else {
                    panic!("Nil relation chain")
                };

                let mut required_exprs = BTreeSet::new();
                // collect the output calculated fields
                match plan {
                    LogicalPlan::Extension(plan) => {
                        if let Some(model_plan) =
                            plan.node.as_any().downcast_ref::<ModelPlanNode>()
                        {
                            UserDefinedLogicalNodeCore::schema(model_plan)
                                .fields()
                                .iter()
                                .map(|field| {
                                    col(format!(
                                        "{}.{}",
                                        model_plan.model_name,
                                        field.name()
                                    ))
                                })
                                .for_each(|c| {
                                    required_exprs.insert(OrdExpr::new(c));
                                });
                        } else if let Some(model_source_plan) =
                            plan.node.as_any().downcast_ref::<ModelSourceNode>()
                        {
                            UserDefinedLogicalNodeCore::schema(model_source_plan)
                                .fields()
                                .iter()
                                .map(|field| {
                                    col(format!(
                                        "{}.{}",
                                        model_source_plan.model_name,
                                        field.name()
                                    ))
                                })
                                .for_each(|c| {
                                    required_exprs.insert(OrdExpr::new(c));
                                });
                        } else {
                            panic!("Invalid extension plan node")
                        }
                    }
                    _ => panic!(""),
                };
                // collect the column of the left table
                for index in 0..left.schema().fields().len() {
                    let (Some(table_rf), f) = left.schema().qualified_field(index) else {
                        panic!("Field not found")
                    };
                    let qualified_name = format!("{}.{}", table_rf, f.name());
                    required_exprs.insert(OrdExpr::new(col(qualified_name)));
                }

                // collect the column of the right table
                for index in 0..right.schema().fields().len() {
                    let (Some(table_rf), f) = right.schema().qualified_field(index)
                    else {
                        panic!("Field not found")
                    };
                    let qualified_name = format!("{}.{}", table_rf, f.name());
                    required_exprs.insert(OrdExpr::new(col(qualified_name)));
                }

                let required_field: Vec<Expr> = required_exprs
                    .iter()
                    .map(|expr| expr.expr.clone())
                    .collect();

                Some(
                    LogicalPlanBuilder::from(left)
                        .join_on(
                            right,
                            datafusion::logical_expr::JoinType::Right,
                            vec![join_condition],
                        )
                        .unwrap()
                        .project(required_field)
                        .unwrap()
                        .build()
                        .unwrap(),
                )
            }
            Start(plan) => Some(
                rule.generate_model_internal(plan.clone())
                    .expect("Failed to generate model plan")
                    .data,
            ),
        }
    }
}

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

/// [ModelSourceNode] is a logical plan node that represents a model source. It contains the model name,
/// required fields, and the schema of the model. It responsible for generating the source plan to scan the
/// remote table. It will be used in the relation chain to generate the join or source plan.
#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub struct ModelSourceNode {
    pub model_name: String,
    pub required_exprs: Vec<Expr>,
    pub schema_ref: DFSchemaRef,
    pub original_table_scan: Option<LogicalPlan>,
}

impl ModelSourceNode {
    pub fn new(
        model: Arc<Model>,
        required_exprs: Vec<Expr>,
        analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
        original_table_scan: Option<LogicalPlan>,
    ) -> Self {
        let mut required_exprs_buffer = BTreeSet::new();
        let fields = required_exprs
            .iter()
            .map(|field| {
                let column = model
                    .get_physical_columns()
                    .into_iter()
                    .find(|column| match field {
                        Expr::Column(c) => c.name.as_str() == column.name(),
                        Expr::Alias(alias) => alias.name.as_str() == column.name(),
                        _ => panic!("Invalid field expression"),
                    })
                    .unwrap_or_else(|| panic!("Field not found {}", field));

                if column.is_calculated {
                    panic!("should not use calculated field in source plan")
                } else {
                    let expr_plan = get_remote_column_exp(
                        &column,
                        Arc::clone(&model),
                        Arc::clone(&analyzed_wren_mdl),
                    );
                    required_exprs_buffer.insert(OrdExpr::new(expr_plan.clone()));
                }
                (
                    Some(TableReference::bare(model.name())),
                    Arc::new(Field::new(
                        column.name(),
                        map_data_type(&column.r#type),
                        column.no_null,
                    )),
                )
            })
            .collect();

        let schema_ref = DFSchemaRef::new(
            DFSchema::new_with_metadata(fields, HashMap::new())
                .expect("create schema failed"),
        );

        ModelSourceNode {
            model_name: model.name().to_string(),
            required_exprs,
            schema_ref,
            original_table_scan,
        }
    }
}

impl UserDefinedLogicalNodeCore for ModelSourceNode {
    fn name(&self) -> &str {
        "ModelSource"
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

    fn fmt_for_explain(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "ModelSource: name={}", self.model_name)
    }

    fn from_template(&self, _: &[Expr], _: &[LogicalPlan]) -> Self {
        ModelSourceNode {
            model_name: self.model_name.clone(),
            required_exprs: self.required_exprs.clone(),
            schema_ref: self.schema_ref.clone(),
            original_table_scan: self.original_table_scan.clone(),
        }
    }
}
