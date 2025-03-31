use std::any::Any;
use std::ops::Deref;
use std::sync::Arc;

use crate::logical_plan::analyze::expand_view::ExpandWrenViewRule;
use crate::logical_plan::analyze::model_anlayze::ModelAnalyzeRule;
use crate::logical_plan::analyze::model_generation::ModelGenerationRule;
use crate::logical_plan::optimize::simplify_timestamp::TimestampSimplify;
use crate::logical_plan::utils::create_schema;
use crate::mdl::manifest::Model;
use crate::mdl::{AnalyzedWrenMDL, SessionStateRef, WrenMDL};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::memory::MemoryCatalogProvider;
use datafusion::catalog::{MemorySchemaProvider, Session};
use datafusion::catalog_common::CatalogProvider;
use datafusion::common::Result;
use datafusion::datasource::{TableProvider, TableType, ViewTable};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::Expr;
use datafusion::optimizer::analyzer::expand_wildcard_rule::ExpandWildcardRule;
use datafusion::optimizer::analyzer::inline_table_scan::InlineTableScan;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
use datafusion::optimizer::decorrelate_predicate_subquery::DecorrelatePredicateSubquery;
use datafusion::optimizer::eliminate_cross_join::EliminateCrossJoin;
use datafusion::optimizer::eliminate_duplicated_expr::EliminateDuplicatedExpr;
use datafusion::optimizer::eliminate_filter::EliminateFilter;
use datafusion::optimizer::eliminate_group_by_constant::EliminateGroupByConstant;
use datafusion::optimizer::eliminate_join::EliminateJoin;
use datafusion::optimizer::eliminate_limit::EliminateLimit;
use datafusion::optimizer::eliminate_one_union::EliminateOneUnion;
use datafusion::optimizer::eliminate_outer_join::EliminateOuterJoin;
use datafusion::optimizer::extract_equijoin_predicate::ExtractEquijoinPredicate;
use datafusion::optimizer::filter_null_join_keys::FilterNullJoinKeys;
use datafusion::optimizer::propagate_empty_relation::PropagateEmptyRelation;
use datafusion::optimizer::replace_distinct_aggregate::ReplaceDistinctWithAggregate;
use datafusion::optimizer::unwrap_cast_in_comparison::UnwrapCastInComparison;
use datafusion::optimizer::{AnalyzerRule, OptimizerRule};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use parking_lot::RwLock;

/// Apply Wren Rules to the context for sql generation.
pub async fn create_ctx_with_mdl(
    ctx: &SessionContext,
    analyzed_mdl: Arc<AnalyzedWrenMDL>,
    is_local_runtime: bool,
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
    );

    let new_state = if is_local_runtime {
        new_state.with_analyzer_rules(analyze_rule_for_local_runtime(
            Arc::clone(&analyzed_mdl),
            reset_default_catalog_schema.clone(),
        ))
        //  The plan will be executed locally, so apply the default optimizer rules
    } else {
        new_state
            .with_analyzer_rules(analyze_rule_for_unparsing(
                Arc::clone(&analyzed_mdl),
                reset_default_catalog_schema.clone(),
            ))
            .with_optimizer_rules(optimize_rule_for_unparsing())
    };

    let new_state = new_state.with_config(config).build();
    let ctx = SessionContext::new_with_state(new_state);
    register_table_with_mdl(&ctx, analyzed_mdl.wren_mdl()).await?;
    Ok(ctx)
}

// Analyzer rules for local runtime
fn analyze_rule_for_local_runtime(
    analyzed_mdl: Arc<AnalyzedWrenMDL>,
    session_state_ref: SessionStateRef,
) -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    vec![
        // expand the view should be the first rule
        Arc::new(ExpandWrenViewRule::new(
            Arc::clone(&analyzed_mdl),
            Arc::clone(&session_state_ref),
        )),
        Arc::new(ModelAnalyzeRule::new(
            Arc::clone(&analyzed_mdl),
            Arc::clone(&session_state_ref),
        )),
        Arc::new(ModelGenerationRule::new(
            Arc::clone(&analyzed_mdl),
            session_state_ref,
        )),
        Arc::new(InlineTableScan::new()),
        // Every rule that will generate [Expr::Wildcard] should be placed in front of [ExpandWildcardRule].
        Arc::new(ExpandWildcardRule::new()),
        // [Expr::Wildcard] should be expanded before [TypeCoercion]
        Arc::new(TypeCoercion::new()),
    ]
}

// Analyze rules for local runtime
fn analyze_rule_for_unparsing(
    analyzed_mdl: Arc<AnalyzedWrenMDL>,
    session_state_ref: SessionStateRef,
) -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    vec![
        // expand the view should be the first rule
        Arc::new(ExpandWrenViewRule::new(
            Arc::clone(&analyzed_mdl),
            Arc::clone(&session_state_ref),
        )),
        Arc::new(ModelAnalyzeRule::new(
            Arc::clone(&analyzed_mdl),
            Arc::clone(&session_state_ref),
        )),
        Arc::new(ModelGenerationRule::new(
            Arc::clone(&analyzed_mdl),
            session_state_ref,
        )),
        Arc::new(InlineTableScan::new()),
        // Every rule that will generate [Expr::Wildcard] should be placed in front of [ExpandWildcardRule].
        Arc::new(ExpandWildcardRule::new()),
        // TimestampSimplify should be placed before TypeCoercion because the simplified timestamp should
        // be casted to the target type if needed
        Arc::new(TimestampSimplify::new()),
        // [Expr::Wildcard] should be expanded before [TypeCoercion]
        Arc::new(TypeCoercion::new()),
        // Disable it to avoid generate the alias name, `count(*)` because BigQuery doesn't allow
        // the special character `*` in the alias name
        // Arc::new(CountWildcardRule::new()),
    ]
}

/// Optimizer rules for unparse
fn optimize_rule_for_unparsing() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    vec![
        // Disable EliminateNestedUnion because unparser only support unparsing an union with two inputs
        // see https://github.com/apache/datafusion/issues/13621 for details
        // Arc::new(EliminateNestedUnion::new()),
        // Disable SimplifyExpressions to avoid apply some function locally
        // Arc::new(SimplifyExpressions::new()),
        Arc::new(UnwrapCastInComparison::new()),
        Arc::new(ReplaceDistinctWithAggregate::new()),
        Arc::new(EliminateJoin::new()),
        Arc::new(DecorrelatePredicateSubquery::new()),
        // Disable ScalarSubqueryToJoin to avoid generate invalid sql (join without condition)
        // Arc::new(ScalarSubqueryToJoin::new()),
        Arc::new(ExtractEquijoinPredicate::new()),
        // Disable SimplifyExpressions to avoid apply some function locally
        // Arc::new(SimplifyExpressions::new()),
        Arc::new(EliminateDuplicatedExpr::new()),
        Arc::new(EliminateFilter::new()),
        Arc::new(EliminateCrossJoin::new()),
        // Disable CommonSubexprEliminate to avoid generate invalid projection plan
        // Arc::new(CommonSubexprEliminate::new()),
        Arc::new(EliminateLimit::new()),
        Arc::new(PropagateEmptyRelation::new()),
        // Must be after PropagateEmptyRelation
        Arc::new(EliminateOneUnion::new()),
        Arc::new(FilterNullJoinKeys::default()),
        Arc::new(EliminateOuterJoin::new()),
        // Filters can't be pushed down past Limits, we should do PushDownFilter after PushDownLimit
        // TODO: Sort with pushdown-limit doesn't support to be unparse
        // Arc::new(PushDownLimit::new()),
        // Disable PushDownFilter to avoid the casting for bigquery (datetime/timestamp) column be removed
        // Arc::new(PushDownFilter::new()),
        // Disable SingleDistinctToGroupBy to avoid generate invalid aggregation plan
        // Arc::new(SingleDistinctToGroupBy::new()),
        // Disable SimplifyExpressions to avoid apply some function locally
        // Arc::new(SimplifyExpressions::new()),
        Arc::new(UnwrapCastInComparison::new()),
        // Disable CommonSubexprEliminate to avoid generate invalid projection plan
        // Arc::new(CommonSubexprEliminate::new()),
        Arc::new(EliminateGroupByConstant::new()),
        // TODO: This rule would generate a plan that is not supported by the current unparser
        // Arc::new(OptimizeProjections::new()),
    ]
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

#[derive(Debug)]
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
