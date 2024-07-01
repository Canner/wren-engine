use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::schema::MemorySchemaProvider;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::common::Result;
use datafusion::datasource::{TableProvider, TableType, ViewTable};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

use crate::logical_plan::analyze::rule::{
    ModelAnalyzeRule, ModelGenerationRule, RemoveWrenPrefixRule,
};
use crate::logical_plan::utils::create_schema;
use crate::mdl::manifest::Model;
use crate::mdl::{AnalyzedWrenMDL, WrenMDL};

pub async fn create_ctx_with_mdl(
    ctx: &SessionContext,
    analyzed_mdl: Arc<AnalyzedWrenMDL>,
) -> Result<SessionContext> {
    let new_state = ctx
        .state()
        .with_analyzer_rules(vec![
            Arc::new(ModelAnalyzeRule::new(Arc::clone(&analyzed_mdl))),
            Arc::new(ModelGenerationRule::new(Arc::clone(&analyzed_mdl))),
            Arc::new(RemoveWrenPrefixRule::new(Arc::clone(&analyzed_mdl))),
        ])
        // TODO: there're some issues for the optimize rule.
        .with_optimizer_rules(vec![]);
    let ctx = SessionContext::new_with_state(new_state);
    register_table_with_mdl(&ctx, analyzed_mdl.wren_mdl()).await?;
    Ok(ctx)
}

pub async fn register_table_with_mdl(
    ctx: &SessionContext,
    wren_mdl: Arc<WrenMDL>,
) -> Result<()> {
    let catalog = MemoryCatalogProvider::new();
    let schema = MemorySchemaProvider::new();

    catalog.register_schema(&wren_mdl.manifest.schema, Arc::new(schema))?;
    ctx.register_catalog(&wren_mdl.manifest.catalog, Arc::new(catalog));

    for model in wren_mdl.manifest.models.iter() {
        let table = WrenDataSource::new(Arc::clone(model))?;
        ctx.register_table(
            format!(
                "{}.{}.{}",
                &wren_mdl.manifest.catalog, &wren_mdl.manifest.schema, &model.name
            ),
            Arc::new(table),
        )?;
    }
    for view in wren_mdl.manifest.views.iter() {
        let plan = ctx.state().create_logical_plan(&view.statement).await?;
        let view_table = ViewTable::try_new(plan, Some(view.statement.clone()))?;
        ctx.register_table(
            format!(
                "{}.{}.{}",
                &wren_mdl.manifest.catalog, &wren_mdl.manifest.schema, &view.name
            ),
            Arc::new(view_table),
        )?;
    }
    Ok(())
}

struct WrenDataSource {
    schema: SchemaRef,
}

impl WrenDataSource {
    pub fn new(model: Arc<Model>) -> Result<Self> {
        let schema = create_schema(model.get_physical_columns().clone())?;
        Ok(Self { schema })
    }
}

#[async_trait]
impl TableProvider for WrenDataSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unreachable!("WrenDataSource should be replaced before physical planning")
    }
}
