use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFField, DFSchema, DFSchemaRef, Result};
use datafusion::logical_expr::Extension;
use datafusion::logical_expr::{
    col, Expr, LogicalPlan, LogicalPlanBuilder, Projection, SubqueryAlias,
    UserDefinedLogicalNodeCore,
};
use datafusion::optimizer::analyzer::AnalyzerRule;
use std::collections::vec_deque;
use std::{collections::HashMap, fmt, fmt::Debug, sync::Arc};

use crate::mdl::manifest::Model;
use crate::mdl::WrenMDL;

use super::utils::{create_remote_table_source, map_data_type};

// Regonzied the model. Turn TableScan from a model to MdoelPlanNode
pub struct ModelAnalyzeRule {
    mdl: Arc<WrenMDL>,
}

impl ModelAnalyzeRule {
    pub fn new(mdl: Arc<WrenMDL>) -> Self {
        Self { mdl }
    }

    fn analyze_model_internal(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        if let LogicalPlan::Projection(Projection {
            input,
            expr,
            schema,
            ..
        }) = plan.clone()
        {
            if let LogicalPlan::TableScan(table_scan) = input.as_ref() {
                if let Some(model) = self
                    .mdl
                    .get_model(&table_scan.table_name.to_string().as_str())
                {
                    let model = LogicalPlan::Extension(Extension {
                        node: Arc::new(ModelPlanNode::new(
                            model,
                            expr.clone(),
                            // TODO: maybe we shouldn't clone the table_scan here
                            LogicalPlan::TableScan(table_scan.clone()),
                        )),
                    });
                    let result = Projection::new_from_schema(Arc::new(model), schema);
                    return Ok(Transformed::yes(LogicalPlan::Projection(result)));
                }
            }
        }
        return Ok(Transformed::no(plan));
    }
}

impl AnalyzerRule for ModelAnalyzeRule {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(&|plan| -> Result<Transformed<LogicalPlan>> {
            self.analyze_model_internal(plan)
        })
        .data()
    }

    fn name(&self) -> &str {
        "ModelAnalyzeRule"
    }
}

#[derive(PartialEq, Eq, Hash, Debug)]
struct ModelPlanNode {
    model_name: String,
    requried_fields: Vec<Expr>,
    schema_ref: DFSchemaRef,
    original_table_scan: LogicalPlan,
}

impl ModelPlanNode {
    pub fn new(
        model: Arc<Model>,
        requried_fields: Vec<Expr>,
        original_table_scan: LogicalPlan,
    ) -> Self {
        let schema_ref = create_df_schema(Arc::clone(&model));
        Self {
            model_name: model.name.clone(),
            requried_fields,
            schema_ref,
            original_table_scan,
        }
    }
}

fn create_df_schema(model: Arc<Model>) -> DFSchemaRef {
    let fields: Vec<DFField> = model
        .columns
        .iter()
        .map(|column| {
            let data_type = map_data_type(&column.r#type);
            DFField::new(
                Some(model.name.clone()),
                &column.name,
                data_type,
                column.no_null,
            )
        })
        .collect();
    DFSchemaRef::new(
        DFSchema::new_with_metadata(fields, HashMap::new()).expect("create schema failed"),
    )
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
        self.requried_fields.clone()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Model: name={}", self.model_name)
    }

    fn from_template(&self, _: &[Expr], _: &[LogicalPlan]) -> Self {
        ModelPlanNode {
            model_name: self.model_name.clone(),
            requried_fields: vec![],
            schema_ref: DFSchemaRef::new(DFSchema::empty()),
            original_table_scan: self.original_table_scan.clone(),
        }
    }
}

// Generate the query plan for the ModelPlanNode
pub struct ModelGenerationRule {
    mdl: Arc<WrenMDL>,
}

impl ModelGenerationRule {
    pub fn new(mdl: Arc<WrenMDL>) -> Self {
        Self { mdl }
    }

    fn generate_model_internal(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Extension(extension) => {
                if let Some(model_plan) = extension.node.as_any().downcast_ref::<ModelPlanNode>() {
                    let model: Arc<Model> = Arc::clone(
                        &self
                            .mdl
                            .get_model(&model_plan.model_name)
                            .expect("Model not found"),
                    );
                    // support table reference

                    let mut exprs = vec_deque::VecDeque::new();

                    model.columns.iter().for_each(|column| {
                        if let Some(expression) = &column.expression {
                            let expr_plan = col(expression).alias(column.name.clone());
                            exprs.push_back(expr_plan);
                        } else {
                            let expr_plan = col(column.name.clone());
                            exprs.push_back(expr_plan);
                        }
                    });

                    let table_scan = match &model_plan.original_table_scan {
                        LogicalPlan::TableScan(original_scan) => {
                            LogicalPlanBuilder::scan_with_filters(
                                model.name.clone(),
                                create_remote_table_source(&model, &self.mdl),
                                None,
                                original_scan.filters.clone(),
                            )
                            .unwrap()
                            .build()
                        }
                        _ => Err(datafusion::error::DataFusionError::Internal(
                            "ModelPlanNode should have a TableScan as original_table_scan"
                                .to_string(),
                        )),
                    };

                    let exprs_vec = exprs.into_iter().collect();
                    let result = Projection::try_new(exprs_vec, Arc::new(table_scan?)).unwrap();

                    let alias_name = model.name.clone();
                    let alias = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                        Arc::new(LogicalPlan::Projection(result)),
                        alias_name,
                    )?);
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
        plan.transform_up(&|plan| -> Result<Transformed<LogicalPlan>> {
            self.generate_model_internal(plan)
        })
        .data()
    }

    fn name(&self) -> &str {
        "ModelGenerationRule"
    }
}
