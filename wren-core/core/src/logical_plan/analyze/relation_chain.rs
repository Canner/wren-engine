use crate::logical_plan::analyze::model_generation::ModelGenerationRule;
use crate::logical_plan::analyze::plan::{
    CalculationPlanNode, ModelPlanNode, ModelSourceNode, OrdExpr, PartialModelPlanNode,
};
use crate::logical_plan::analyze::relation_chain::RelationChain::Start;
use crate::logical_plan::utils::{
    create_schema, eliminate_ambiguous_columns, rebase_column,
};
use crate::mdl::context::SessionPropertiesRef;
use crate::mdl::lineage::DatasetLink;
use crate::mdl::manifest::JoinType;
use crate::mdl::utils::{qualify_name_from_column_name, quoted};
use crate::mdl::Dataset;
use crate::mdl::{AnalyzedWrenMDL, SessionStateRef};
use crate::{mdl, DataFusionError};
use datafusion::common::alias::AliasGenerator;
use datafusion::common::{
    internal_err, not_impl_err, plan_err, DFSchema, DFSchemaRef, Result,
};
use datafusion::common::{plan_datafusion_err, TableReference};
use datafusion::logical_expr::{
    col, Expr, Extension, LogicalPlan, LogicalPlanBuilder, SubqueryAlias,
    UserDefinedLogicalNodeCore,
};
use petgraph::graph::NodeIndex;
use petgraph::Graph;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

const ALIAS: &str = "__relation_";

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
        session_properties: SessionPropertiesRef,
    ) -> Result<Self> {
        match dataset {
            Dataset::Model(source_model) => {
                Ok(Start(LogicalPlan::Extension(Extension {
                    node: Arc::new(ModelSourceNode::new(
                        Arc::clone(source_model),
                        required_fields,
                        analyzed_wren_mdl,
                        session_state_ref,
                        session_properties,
                        None,
                    )?),
                })))
            }
            _ => {
                not_impl_err!("Only support model as source dataset")
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_chain(
        source: Self,
        mut start: NodeIndex,
        iter: impl Iterator<Item = NodeIndex>,
        directed_graph: Graph<Dataset, DatasetLink>,
        model_required_fields: &HashMap<TableReference, BTreeSet<OrdExpr>>,
        analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
        session_state_ref: SessionStateRef,
        properties: SessionPropertiesRef,
    ) -> Result<Self> {
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
                    let schema = create_schema(
                        fields
                            .iter()
                            .map(|e| {
                                e.column.clone().ok_or_else(|| {
                                    plan_datafusion_err!(
                                        "Required field {:?} has no physical column",
                                        e.expr
                                    )
                                })
                            })
                            .collect::<Result<_>>()?,
                    )?;
                    let exprs = fields.iter().cloned().map(|c| c.expr).collect();
                    let plan = ModelPlanNode::new(
                        Arc::clone(target_model),
                        exprs,
                        None,
                        Arc::clone(&analyzed_wren_mdl),
                        Arc::clone(&session_state_ref),
                        Arc::clone(&properties),
                    )?;

                    let df_schema = DFSchemaRef::from(DFSchema::try_from(schema)?);
                    let node = LogicalPlan::Extension(Extension {
                        node: Arc::new(PartialModelPlanNode::new(plan, df_schema)),
                    });
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
        alias_generator: &AliasGenerator,
    ) -> Result<(Option<LogicalPlan>, Option<String>)> {
        match self {
            RelationChain::Chain(plan, _, condition, ref mut next) => {
                let left = rule.generate_model_internal(plan.clone())?.data;
                let left_alias = if let LogicalPlan::SubqueryAlias(SubqueryAlias {
                    alias,
                    ..
                }) = &left
                {
                    alias.table()
                } else {
                    return internal_err!(
                        "model plan should be wrapped in a subquery alias"
                    );
                };

                let (Some(right), right_alias) = next.plan(rule, alias_generator)? else {
                    return plan_err!("Nil relation chain");
                };

                let join_keys: Vec<Expr> = mdl::utils::collect_identifiers(condition)?
                    .iter()
                    .map(|c| col(qualify_name_from_column_name(c)))
                    .collect();

                // The right key should be rebased if the right table has a generated alias
                let join_keys = join_keys
                    .into_iter()
                    .map(|expr| match expr {
                        Expr::Column(c) => {
                            if c.relation
                                .clone()
                                .map(|r| r.table() != left_alias)
                                .unwrap_or(false)
                            {
                                if let Some(right_alias) = &right_alias {
                                    return rebase_column(&Expr::Column(c), right_alias);
                                }
                            }
                            Ok::<_, DataFusionError>(Expr::Column(c))
                        }
                        _ => Ok::<_, DataFusionError>(expr),
                    })
                    .collect::<Result<Vec<_>>>()?;
                let join_condition = join_keys[0].clone().eq(join_keys[1].clone());
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
                                        quoted(model_plan.plan_name()),
                                        quoted(field.name()),
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
                                        quoted(&model_source_plan.model_name),
                                        quoted(field.name()),
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
                                        quoted(
                                            calculation_plan.calculation.column.name()
                                        ),
                                        quoted(field.name()),
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
                                        quoted(partial_model_plan.model_node.plan_name()),
                                        quoted(field.name()),
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
                    let qualified_name =
                        format!("{}.{}", table_rf.to_quoted_string(), quoted(f.name()));
                    required_exprs.insert(OrdExpr::new(col(qualified_name)));
                }

                // collect the column of the right table
                for index in 0..right.schema().fields().len() {
                    let (Some(table_rf), f) = right.schema().qualified_field(index)
                    else {
                        return plan_err!("Field not found");
                    };
                    let qualified_name =
                        format!("{}.{}", table_rf.to_quoted_string(), quoted(f.name()));
                    required_exprs.insert(OrdExpr::new(col(qualified_name)));
                }

                let required_field: Vec<Expr> = required_exprs
                    .iter()
                    .map(|expr| expr.expr.clone())
                    .collect();
                let required_field = eliminate_ambiguous_columns(required_field);
                let alias = alias_generator.next(ALIAS);
                Ok((
                    Some(
                        LogicalPlanBuilder::from(left)
                            .join_on(
                                right,
                                datafusion::logical_expr::JoinType::Right,
                                vec![join_condition],
                            )?
                            .project(required_field)?
                            .alias(&alias)?
                            .build()?,
                    ),
                    Some(alias),
                ))
            }
            Start(plan) => {
                Ok((Some(rule.generate_model_internal(plan.clone())?.data), None))
            }
        }
    }
}
