use datafusion::arrow::datatypes::Field;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::sync::Arc;

use crate::logical_plan::analyze::RelationChain;
use crate::logical_plan::analyze::RelationChain::Start;
use crate::logical_plan::utils::{from_qualified_name, map_data_type};
use crate::mdl;
use crate::mdl::lineage::DatasetLink;
use crate::mdl::manifest::{JoinType, Model};
use crate::mdl::utils::{
    create_remote_expr_for_model, create_wren_calculated_field_expr,
    create_wren_expr_for_model, is_dag,
};
use crate::mdl::{AnalyzedWrenMDL, ColumnReference, Dataset};
use datafusion::common::{
    internal_err, plan_err, Column, DFSchema, DFSchemaRef, TableReference,
};
use datafusion::error::Result;
use datafusion::logical_expr::utils::find_aggregate_exprs;
use datafusion::logical_expr::{
    col, Expr, Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use petgraph::Graph;

pub(crate) enum WrenPlan {
    Source(Arc<ModelSourceNode>),
    Model(Arc<ModelPlanNode>),
    Calculation(Arc<CalculationPlanNode>),
}

impl WrenPlan {
    fn name(&self) -> &str {
        match self {
            WrenPlan::Source(node) => &node.model_name,
            WrenPlan::Model(node) => &node.model_name,
            WrenPlan::Calculation(node) => node.calculation.column.name(),
        }
    }

    fn as_ref(&self) -> Arc<dyn UserDefinedLogicalNode> {
        match self {
            WrenPlan::Source(source) => Arc::clone(source) as _,
            WrenPlan::Model(plan) => Arc::clone(plan) as _,
            WrenPlan::Calculation(calculation) => Arc::clone(calculation) as _,
        }
    }
}

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
    ) -> Result<Self> {
        ModelPlanNodeBuilder::new(analyzed_wren_mdl).build(
            model,
            required_fields,
            original_table_scan,
        )
    }
}

struct ModelPlanNodeBuilder {
    required_exprs_buffer: BTreeSet<OrdExpr>,
    directed_graph: Graph<Dataset, DatasetLink>,
    model_required_fields: HashMap<TableReference, BTreeSet<OrdExpr>>,
    required_calculation: Vec<WrenPlan>,
    fields: VecDeque<(Option<TableReference>, Arc<Field>)>,
    analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
}

impl ModelPlanNodeBuilder {
    fn new(analyzed_wren_mdl: Arc<AnalyzedWrenMDL>) -> Self {
        Self {
            required_exprs_buffer: BTreeSet::new(),
            directed_graph: Graph::new(),
            model_required_fields: HashMap::new(),
            required_calculation: vec![],
            fields: VecDeque::new(),
            analyzed_wren_mdl,
        }
    }

    fn build(
        mut self,
        model: Arc<Model>,
        required_fields: Vec<Expr>,
        original_table_scan: Option<LogicalPlan>,
    ) -> Result<ModelPlanNode> {
        let model_ref = TableReference::full(
            self.analyzed_wren_mdl.wren_mdl().catalog(),
            self.analyzed_wren_mdl.wren_mdl().schema(),
            model.name(),
        );

        let required_columns =
            model.get_physical_columns().into_iter().filter(|column| {
                required_fields.iter().any(|expr| {
                    if let Expr::Column(column_expr) = expr {
                        column_expr.name.as_str() == column.name()
                    } else {
                        false
                    }
                })
            });

        for column in required_columns {
            if column.is_calculated {
                let expr = if column.expression.is_some() {
                    let column_rf = self
                        .analyzed_wren_mdl
                        .wren_mdl()
                        .get_column_reference(&from_qualified_name(
                            &self.analyzed_wren_mdl.wren_mdl(),
                            model.name(),
                            column.name(),
                        ));
                    let Some(column_rf) = column_rf else {
                        return plan_err!("Column reference not found for {:?}", column);
                    };
                    let expr = create_wren_calculated_field_expr(
                        column_rf,
                        Arc::clone(&self.analyzed_wren_mdl),
                    )?;
                    let expr_plan = expr.alias(column.name());
                    expr_plan
                } else {
                    return plan_err!("Only support calculated field with expression");
                };

                let qualified_column = from_qualified_name(
                    &self.analyzed_wren_mdl.wren_mdl(),
                    model.name(),
                    column.name(),
                );

                let Some(column_graph) = self
                    .analyzed_wren_mdl
                    .lineage()
                    .required_dataset_topo
                    .get(&qualified_column)
                else {
                    return plan_err!(
                        "Required dataset not found for {}",
                        qualified_column
                    );
                };

                if !find_aggregate_exprs(&[expr.clone()]).is_empty() {
                    let calculation = self.create_partial_calculation(
                        model_ref.clone(),
                        Arc::clone(&column),
                        &qualified_column,
                        expr,
                    )?;
                    self.required_calculation.push(calculation);
                } else {
                    self.required_exprs_buffer
                        .insert(OrdExpr::new(expr.clone()));
                    merge_graph(&mut self.directed_graph, column_graph)?;
                    let _ = collect_model_required_fields(
                        &qualified_column,
                        Arc::clone(&self.analyzed_wren_mdl),
                        &mut self.model_required_fields,
                    );
                }
            } else {
                let expr_plan = get_remote_column_exp(
                    &column,
                    Arc::clone(&model),
                    Arc::clone(&self.analyzed_wren_mdl),
                )?;
                self.model_required_fields
                    .entry(model_ref.clone())
                    .or_default()
                    .insert(OrdExpr::new(expr_plan.clone()));
                let expr_plan = Expr::Column(Column::from_qualified_name(format!(
                    "{}.{}",
                    model_ref.table(),
                    column.name()
                )));
                self.required_exprs_buffer
                    .insert(OrdExpr::new(expr_plan.clone()));
            }
            self.fields.push_front((
                Some(TableReference::bare(model.name())),
                Arc::new(Field::new(
                    column.name(),
                    map_data_type(&column.r#type)?,
                    column.no_null,
                )),
            ));
        }

        self.directed_graph
            .add_node(Dataset::Model(Arc::clone(&model)));
        if !is_dag(&self.directed_graph) {
            return plan_err!("cyclic dependency detected: {}", model.name());
        }

        let schema_ref = DFSchemaRef::new(
            DFSchema::new_with_metadata(
                self.fields.into_iter().collect(),
                HashMap::new(),
            )
            .expect("create schema failed"),
        );

        let mut iter = self.directed_graph.node_indices();
        let Some(start) = iter.next() else {
            return internal_err!("Model not found");
        };
        let Some(source) = self.directed_graph.node_weight(start) else {
            return internal_err!("Dataset not found");
        };
        let mut source_required_fields: Vec<Expr> = self
            .model_required_fields
            .get(&model_ref)
            .map(|c| c.iter().cloned().map(|c| c.expr).collect())
            .unwrap_or_default();

        let mut calculate_iter = self.required_calculation.iter();

        let source_chain =
            if !source_required_fields.is_empty() || required_fields.is_empty() {
                if required_fields.is_empty() {
                    source_required_fields.insert(0, Expr::Wildcard { qualifier: None });
                }
                RelationChain::source(
                    source,
                    source_required_fields,
                    Arc::clone(&self.analyzed_wren_mdl),
                )?
            } else {
                let Some(first_calculation) = calculate_iter.next() else {
                    return plan_err!("Calculation not found and no any required field");
                };
                Start(LogicalPlan::Extension(Extension {
                    node: first_calculation.as_ref(),
                }))
            };

        let mut relation_chain = RelationChain::with_chain(
            source_chain,
            start,
            iter,
            self.directed_graph,
            &self.model_required_fields,
            Arc::clone(&self.analyzed_wren_mdl),
        )?;

        for calculation_plan in calculate_iter {
            let target_ref = TableReference::bare(calculation_plan.name());
            let Some(join_key) = model.primary_key() else {
                return plan_err!(
                    "Model {} should have primary key for calculation",
                    model.name()
                );
            };
            relation_chain = RelationChain::Chain(
                LogicalPlan::Extension(Extension {
                    node: calculation_plan.as_ref(),
                }),
                JoinType::OneToOne,
                format!(
                    "{}.{} = {}.{}",
                    model_ref.table(),
                    join_key,
                    target_ref.table(),
                    join_key,
                ),
                Box::new(relation_chain),
            );
        }
        Ok(ModelPlanNode {
            model_name: model.name.clone(),
            required_exprs: self
                .required_exprs_buffer
                .into_iter()
                .map(|oe| oe.expr)
                .collect(),
            relation_chain: Box::new(relation_chain),
            schema_ref,
            original_table_scan,
        })
    }

    fn create_partial_calculation(
        &mut self,
        model_ref: TableReference,
        column: Arc<mdl::manifest::Column>,
        qualified_column: &Column,
        col_expr: Expr,
    ) -> Result<WrenPlan> {
        let Some(column_graph) = self
            .analyzed_wren_mdl
            .lineage()
            .required_dataset_topo
            .get(qualified_column)
        else {
            return plan_err!("Required dataset not found for {}", qualified_column);
        };

        // The calculation column is provided by the CalculationPlanNode.
        let _ = &self.required_exprs_buffer.insert(OrdExpr::new(col(format!(
            "{}.{}",
            column.name(),
            column.name()
        ))));

        let mut partial_model_required_fields = HashMap::new();
        let _ = collect_model_required_fields(
            qualified_column,
            Arc::clone(&self.analyzed_wren_mdl),
            &mut partial_model_required_fields,
        );

        let mut iter = column_graph.node_indices();

        let start = iter.next().unwrap();
        let source_required_fields = partial_model_required_fields
            .get(&model_ref)
            .map(|c| c.iter().cloned().map(|c| c.expr).collect())
            .unwrap_or_default();
        let source = column_graph.node_weight(start).unwrap();

        let source_chain = RelationChain::source(
            source,
            source_required_fields,
            Arc::clone(&self.analyzed_wren_mdl),
        )?;

        let partial_chain = RelationChain::with_chain(
            source_chain,
            start,
            iter,
            column_graph.clone(),
            &partial_model_required_fields,
            Arc::clone(&self.analyzed_wren_mdl),
        )?;
        let Some(column_rf) = self
            .analyzed_wren_mdl
            .wren_mdl()
            .get_column_reference(qualified_column)
        else {
            return plan_err!("Column reference not found for {:?}", column);
        };
        Ok(WrenPlan::Calculation(Arc::new(CalculationPlanNode::new(
            column_rf,
            col_expr,
            partial_chain,
            Arc::clone(&self.analyzed_wren_mdl),
        )?)))
    }
}

fn collect_model_required_fields(
    qualified_column: &Column,
    analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
    model_required_fields: &mut HashMap<TableReference, BTreeSet<OrdExpr>>,
) -> Result<()> {
    let Some(set) = analyzed_wren_mdl
        .lineage()
        .required_fields_map
        .get(&qualified_column)
    else {
        return plan_err!("Required fields not found for {}", qualified_column);
    };

    for c in set {
        let Some(relation_ref) = &c.relation else {
            return plan_err!("Source dataset not found for {}", c);
        };
        let Some(ColumnReference { dataset, column }) =
            analyzed_wren_mdl.wren_mdl().get_column_reference(c)
        else {
            return plan_err!("Column reference not found for {}", c);
        };
        if let Dataset::Model(m) = dataset {
            if column.is_calculated {
                let expr_plan = if let Some(expression) = &column.expression {
                    create_wren_expr_for_model(
                        expression,
                        Arc::clone(&m),
                        Arc::clone(&analyzed_wren_mdl),
                    )?
                } else {
                    return plan_err!("Only support calculated field with expression");
                }
                .alias(column.name.clone());
                model_required_fields
                    .entry(relation_ref.clone())
                    .or_default()
                    .insert(OrdExpr::new(expr_plan));
            } else {
                let expr_plan = get_remote_column_exp(
                    &column,
                    Arc::clone(&m),
                    Arc::clone(&analyzed_wren_mdl),
                )?;
                model_required_fields
                    .entry(relation_ref.clone())
                    .or_default()
                    .insert(OrdExpr::new(expr_plan));
            }
        } else {
            return plan_err!("Only support model as source dataset");
        };
    }
    Ok(())
}

fn get_remote_column_exp(
    column: &mdl::manifest::Column,
    model: Arc<Model>,
    analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
) -> Result<Expr> {
    let expr = if let Some(expression) = &column.expression {
        create_remote_expr_for_model(expression, model, analyzed_wren_mdl)?
    } else {
        create_remote_expr_for_model(&column.name, model, analyzed_wren_mdl)?
    };
    Ok(expr.alias(column.name.clone()))
}

#[derive(Eq, PartialEq, Debug, Hash, Clone)]
pub struct OrdExpr {
    pub(crate) expr: Expr,
}

impl OrdExpr {
    pub(crate) fn new(expr: Expr) -> Self {
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
) -> Result<()> {
    let mut node_map = HashMap::new();
    for node in new_graph.node_indices() {
        let new_node = graph.add_node(new_graph[node].clone());
        node_map.insert(node, new_node);
    }

    for edge in new_graph.edge_indices() {
        let Some((source, target)) = new_graph.edge_endpoints(edge) else {
            return internal_err!("Edge not found");
        };
        let source = node_map.get(&source).unwrap();
        let target = node_map.get(&target).unwrap();
        graph.add_edge(*source, *target, new_graph[edge].clone());
    }
    Ok(())
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

    fn with_exprs_and_inputs(
        &self,
        _: Vec<Expr>,
        _: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        Ok(ModelPlanNode {
            model_name: self.model_name.clone(),
            required_exprs: self.required_exprs.clone(),
            relation_chain: self.relation_chain.clone(),
            schema_ref: self.schema_ref.clone(),
            original_table_scan: self.original_table_scan.clone(),
        })
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
    ) -> Result<Self> {
        let mut required_exprs_buffer = BTreeSet::new();
        let mut fields_buffer = BTreeSet::new();
        for expr in required_exprs.iter() {
            if let Expr::Wildcard { qualifier } = expr {
                let model = if let Some(model) = qualifier {
                    let Some(model) = analyzed_wren_mdl.wren_mdl.get_model(model) else {
                        return plan_err!("Model not found {}", &model);
                    };
                    model
                } else {
                    Arc::clone(&model)
                };
                for column in model.get_physical_columns().into_iter() {
                    // skip the calculated field
                    if column.is_calculated {
                        continue;
                    }
                    fields_buffer.insert((
                        Some(TableReference::bare(model.name())),
                        Arc::new(Field::new(
                            column.name(),
                            map_data_type(&column.r#type)?,
                            column.no_null,
                        )),
                    ));
                    required_exprs_buffer.insert(OrdExpr::new(get_remote_column_exp(
                        &column,
                        Arc::clone(&model),
                        Arc::clone(&analyzed_wren_mdl),
                    )?));
                }
            } else {
                let Some(column) =
                    model
                        .get_physical_columns()
                        .into_iter()
                        .find(|column| match expr {
                            Expr::Column(c) => c.name.as_str() == column.name(),
                            Expr::Alias(alias) => alias.name.as_str() == column.name(),
                            _ => false,
                        })
                else {
                    return plan_err!("Field not found {}", expr);
                };

                if column.is_calculated {
                    return plan_err!("should not use calculated field in source plan");
                } else {
                    let expr_plan = get_remote_column_exp(
                        &column,
                        Arc::clone(&model),
                        Arc::clone(&analyzed_wren_mdl),
                    )?;
                    required_exprs_buffer.insert(OrdExpr::new(expr_plan.clone()));
                }

                fields_buffer.insert((
                    Some(TableReference::bare(model.name())),
                    Arc::new(Field::new(
                        column.name(),
                        map_data_type(&column.r#type)?,
                        column.no_null,
                    )),
                ));
            }
        }

        let fields = fields_buffer.into_iter().collect::<Vec<_>>();
        let schema_ref = DFSchemaRef::new(
            DFSchema::new_with_metadata(fields, HashMap::new())
                .expect("create schema failed"),
        );
        let required_exprs = required_exprs_buffer
            .into_iter()
            .map(|e| e.expr)
            .collect::<Vec<_>>();
        Ok(ModelSourceNode {
            model_name: model.name().to_string(),
            required_exprs,
            schema_ref,
            original_table_scan,
        })
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

    fn with_exprs_and_inputs(&self, _: Vec<Expr>, _: Vec<LogicalPlan>) -> Result<Self> {
        Ok(ModelSourceNode {
            model_name: self.model_name.clone(),
            required_exprs: self.required_exprs.clone(),
            schema_ref: self.schema_ref.clone(),
            original_table_scan: self.original_table_scan.clone(),
        })
    }
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub struct CalculationPlanNode {
    pub calculation: ColumnReference,
    pub relation_chain: RelationChain,
    pub dimensions: Vec<Expr>,
    pub measures: Vec<Expr>,
    schema_ref: DFSchemaRef,
}

impl CalculationPlanNode {
    pub fn new(
        calculation: ColumnReference,
        calculation_expr: Expr,
        relation_chain: RelationChain,
        analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
    ) -> Result<Self> {
        let Some(model) = calculation.dataset.try_as_model() else {
            return plan_err!("Only support model as source dataset");
        };
        let Some(pk_column) = model.primary_key().and_then(|pk| model.get_column(pk))
        else {
            return plan_err!("Primary key not found");
        };

        // include calculation column and join key (pk)
        let output_field = vec![
            Arc::new(Field::new(
                calculation.column.name(),
                map_data_type(&calculation.column.r#type)?,
                calculation.column.no_null,
            )),
            Arc::new(Field::new(
                pk_column.name(),
                map_data_type(&pk_column.r#type)?,
                pk_column.no_null,
            )),
        ]
        .into_iter()
        .map(|f| (Some(TableReference::bare(model.name())), f))
        .collect();
        let dimensions = vec![create_wren_expr_for_model(
            &pk_column.name,
            Arc::clone(&model),
            Arc::clone(&analyzed_wren_mdl),
        )?
        .alias(pk_column.name())];
        let schema_ref = DFSchemaRef::new(
            DFSchema::new_with_metadata(output_field, HashMap::new())
                .expect("create schema failed"),
        );
        Ok(Self {
            calculation,
            relation_chain,
            dimensions,
            measures: vec![calculation_expr],
            schema_ref,
        })
    }
}

impl UserDefinedLogicalNodeCore for CalculationPlanNode {
    fn name(&self) -> &str {
        "Calculation"
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
        write!(f, "Calculation: name={}", self.calculation.column.name)
    }

    fn with_exprs_and_inputs(
        &self,
        _: Vec<Expr>,
        _: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        Ok(CalculationPlanNode {
            calculation: self.calculation.clone(),
            relation_chain: self.relation_chain.clone(),
            dimensions: self.dimensions.clone(),
            measures: self.measures.clone(),
            schema_ref: self.schema_ref.clone(),
        })
    }
}
