use crate::logical_plan::analyze::plan::{
    CalculationPlanNode, ModelPlanNode, ModelSourceNode, OrdExpr, PartialModelPlanNode,
};
use crate::logical_plan::analyze::relation_chain::RelationChain::Start;
use crate::logical_plan::analyze::rule::ModelGenerationRule;
use crate::logical_plan::utils::create_schema;
use crate::mdl;
use crate::mdl::lineage::DatasetLink;
use crate::mdl::manifest::JoinType;
use crate::mdl::Dataset;
use crate::mdl::{AnalyzedWrenMDL, SessionStateRef};
use datafusion::common::TableReference;
use datafusion::common::{internal_err, not_impl_err, plan_err, DFSchema, DFSchemaRef};
use datafusion::logical_expr::{
    col, Expr, Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNodeCore,
};
use petgraph::graph::NodeIndex;
use petgraph::Graph;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

/// RelationChain is a chain of models that are connected by the relationship.
/// The chain is used to generate the join plan for the model.
/// The physical layout will be looked like:
/// (((Model3, Model2), Model1), Nil)
#[derive(Eq, PartialEq, Debug, Hash, Clone)]
pub enum RelationChain {
    Chain(LogicalPlan, JoinType, String, Box<RelationChain>),
    Start(LogicalPlan),
}

impl RelationChain {
    pub(crate) fn source(
        dataset: &Dataset,
        required_fields: Vec<Expr>,
        analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
        session_state_ref: SessionStateRef,
    ) -> datafusion::common::Result<Self> {
        match dataset {
            Dataset::Model(source_model) => {
                Ok(Start(LogicalPlan::Extension(Extension {
                    node: Arc::new(ModelSourceNode::new(
                        Arc::clone(source_model),
                        required_fields,
                        analyzed_wren_mdl,
                        session_state_ref,
                        None,
                    )?),
                })))
            }
            _ => {
                not_impl_err!("Only support model as source dataset")
            }
        }
    }

    pub fn with_chain(
        source: Self,
        mut start: NodeIndex,
        iter: impl Iterator<Item = NodeIndex>,
        directed_graph: Graph<Dataset, DatasetLink>,
        model_required_fields: &HashMap<TableReference, BTreeSet<OrdExpr>>,
        analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
        session_state_ref: SessionStateRef,
    ) -> datafusion::common::Result<Self> {
        let mut relation_chain = source;

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
            let Some(fields) = model_required_fields.get(&target_ref) else {
                return plan_err!("Required fields not found for {}", target_ref);
            };
            match target {
                Dataset::Model(target_model) => {
                    let node = if fields.iter().any(|e| {
                        e.column.is_some() && e.column.clone().unwrap().is_calculated
                    }) {
                        let schema = create_schema(
                            fields.iter().filter_map(|e| e.column.clone()).collect(),
                        )?;
                        let plan = ModelPlanNode::new(
                            Arc::clone(target_model),
                            fields.iter().cloned().map(|c| c.expr).collect(),
                            None,
                            Arc::clone(&analyzed_wren_mdl),
                            Arc::clone(&session_state_ref),
                        )?;

                        let df_schema =
                            DFSchemaRef::from(DFSchema::try_from(schema).unwrap());
                        LogicalPlan::Extension(Extension {
                            node: Arc::new(PartialModelPlanNode::new(plan, df_schema)),
                        })
                    } else {
                        LogicalPlan::Extension(Extension {
                            node: Arc::new(ModelSourceNode::new(
                                Arc::clone(target_model),
                                fields.iter().cloned().map(|c| c.expr).collect(),
                                Arc::clone(&analyzed_wren_mdl),
                                Arc::clone(&session_state_ref),
                                None,
                            )?),
                        })
                    };
                    relation_chain = RelationChain::Chain(
                        node,
                        link.join_type,
                        link.condition.clone(),
                        Box::new(relation_chain),
                    );
                }
                _ => return plan_err!("Only support model as source dataset"),
            }
            start = next;
        }
        Ok(relation_chain)
    }

    pub(crate) fn plan(
        &mut self,
        rule: ModelGenerationRule,
    ) -> datafusion::common::Result<Option<LogicalPlan>> {
        match self {
            RelationChain::Chain(plan, _, condition, ref mut next) => {
                let left = rule.generate_model_internal(plan.clone())?.data;
                let join_keys: Vec<Expr> = mdl::utils::collect_identifiers(condition)?
                    .iter()
                    .cloned()
                    .map(|c| col(c.flat_name()))
                    .collect();
                let join_condition = join_keys[0].clone().eq(join_keys[1].clone());
                let Some(right) = next.plan(rule)? else {
                    return plan_err!("Nil relation chain");
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
                                        model_plan.plan_name,
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
                        } else if let Some(calculation_plan) =
                            plan.node.as_any().downcast_ref::<CalculationPlanNode>()
                        {
                            UserDefinedLogicalNodeCore::schema(calculation_plan)
                                .fields()
                                .iter()
                                .map(|field| {
                                    col(format!(
                                        "{}.{}",
                                        calculation_plan.calculation.column.name(),
                                        field.name()
                                    ))
                                })
                                .for_each(|c| {
                                    required_exprs.insert(OrdExpr::new(c));
                                });
                        } else if let Some(partial_model_plan) =
                            plan.node.as_any().downcast_ref::<PartialModelPlanNode>()
                        {
                            UserDefinedLogicalNodeCore::schema(partial_model_plan)
                                .fields()
                                .iter()
                                .map(|field| {
                                    col(format!(
                                        "{}.{}",
                                        partial_model_plan.model_node.plan_name,
                                        field.name()
                                    ))
                                })
                                .for_each(|c| {
                                    required_exprs.insert(OrdExpr::new(c));
                                });
                        } else {
                            return plan_err!("Invalid extension plan node");
                        }
                    }
                    _ => return internal_err!("Invalid plan node"),
                };
                // collect the column of the left table
                for index in 0..left.schema().fields().len() {
                    let (Some(table_rf), f) = left.schema().qualified_field(index) else {
                        return plan_err!("Field not found");
                    };
                    let qualified_name = format!("{}.{}", table_rf, f.name());
                    required_exprs.insert(OrdExpr::new(col(qualified_name)));
                }

                // collect the column of the right table
                for index in 0..right.schema().fields().len() {
                    let (Some(table_rf), f) = right.schema().qualified_field(index)
                    else {
                        return plan_err!("Field not found");
                    };
                    let qualified_name = format!("{}.{}", table_rf, f.name());
                    required_exprs.insert(OrdExpr::new(col(qualified_name)));
                }

                let required_field: Vec<Expr> = required_exprs
                    .iter()
                    .map(|expr| expr.expr.clone())
                    .collect();

                Ok(Some(
                    LogicalPlanBuilder::from(left)
                        .join_on(
                            right,
                            datafusion::logical_expr::JoinType::Right,
                            vec![join_condition],
                        )?
                        .project(required_field)?
                        .build()?,
                ))
            }
            Start(plan) => Ok(Some(rule.generate_model_internal(plan.clone())?.data)),
        }
    }
}
