use std::any::Any;
use std::ops::Deref;
use std::sync::Arc;

use crate::logical_plan::analyze::expand_view::ExpandWrenViewRule;
use crate::logical_plan::analyze::model_anlayze::ModelAnalyzeRule;
use crate::logical_plan::analyze::model_generation::ModelGenerationRule;
use crate::logical_plan::utils::create_schema;
use crate::mdl::manifest::Model;
use crate::mdl::{AnalyzedWrenMDL, WrenMDL};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::catalog_common::memory::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::catalog_common::CatalogProvider;
use datafusion::common::Result;
use datafusion::datasource::{TableProvider, TableType, ViewTable};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use parking_lot::RwLock;

/// Apply Wren Rules to the context for sql generation.
/// TODO: There're some issue for unparsing the datafusion optimized plans.
///   Disable all the optimize rule for sql generation temporarily.
pub async fn create_ctx_with_mdl(
    ctx: &SessionContext,
    analyzed_mdl: Arc<AnalyzedWrenMDL>,
) -> Result<SessionContext> {
    let config = ctx
        .copied_config()
        .with_create_default_catalog_and_schema(false)
        .with_default_catalog_and_schema(
            analyzed_mdl.wren_mdl.catalog(),
            analyzed_mdl.wren_mdl.schema(),
        );
    let reset_default_catalog_schema = Arc::new(RwLock::new(
        SessionStateBuilder::new_from_existing(ctx.state())
            .with_config(config.clone())
            .build(),
    ));

    let new_state = SessionStateBuilder::new_from_existing(
        reset_default_catalog_schema.clone().read().deref().clone(),
    )
    .with_analyzer_rules(vec![
        // expand the view should be the first rule
        Arc::new(ExpandWrenViewRule::new(
            Arc::clone(&analyzed_mdl),
            Arc::clone(&reset_default_catalog_schema),
        )),
        Arc::new(ModelAnalyzeRule::new(
            Arc::clone(&analyzed_mdl),
            reset_default_catalog_schema,
        )),
        Arc::new(ModelGenerationRule::new(Arc::clone(&analyzed_mdl))),
    ])
    // TODO: there're some issues for the optimize rule.
    .with_optimizer_rules(vec![])
    .with_config(config)
    .build();
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
            TableReference::full(wren_mdl.catalog(), wren_mdl.schema(), model.name()),
            Arc::new(table),
        )?;
    }
    for view in wren_mdl.manifest.views.iter() {
        let plan = ctx.state().create_logical_plan(&view.statement).await?;
        let view_table = ViewTable::try_new(plan, Some(view.statement.clone()))?;
        ctx.register_table(
            TableReference::full(wren_mdl.catalog(), wren_mdl.schema(), view.name()),
            Arc::new(view_table),
        )?;
    }
    Ok(())
}

pub struct WrenDataSource {
    schema: SchemaRef,
}

impl WrenDataSource {
    pub fn new(model: Arc<Model>) -> Result<Self> {
        let schema = create_schema(model.get_physical_columns().clone())?;
        Ok(Self { schema })
    }

    pub fn new_with_schema(schema: SchemaRef) -> Self {
        Self { schema }
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
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unreachable!("WrenDataSource should be replaced before physical planning")
    }
}
