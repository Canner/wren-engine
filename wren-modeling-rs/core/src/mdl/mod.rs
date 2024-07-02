use std::fmt::Display;
use std::{collections::HashMap, sync::Arc};

use datafusion::prelude::SessionContext;
use datafusion::{error::Result, sql::unparser::plan_to_sql};
use log::{debug, info};

use manifest::Relationship;

use crate::logical_plan::utils::from_qualified_name_str;
use crate::mdl::context::create_ctx_with_mdl;
use crate::mdl::manifest::{Column, Manifest, Metric, Model};

pub mod builder;
pub mod context;
pub mod lineage;
pub mod manifest;
pub mod utils;

pub struct AnalyzedWrenMDL {
    pub wren_mdl: Arc<WrenMDL>,
    pub lineage: Arc<lineage::Lineage>,
}

impl AnalyzedWrenMDL {
    pub fn analyze(manifest: Manifest) -> Result<Self> {
        let wren_mdl = Arc::new(WrenMDL::new(manifest));
        let lineage = Arc::new(lineage::Lineage::new(&wren_mdl)?);
        Ok(AnalyzedWrenMDL { wren_mdl, lineage })
    }

    pub fn analyze_with_tables(
        manifest: Manifest,
        register_tables: HashMap<String, Arc<dyn datafusion::datasource::TableProvider>>,
    ) -> Result<Self> {
        let mut wren_mdl = WrenMDL::new(manifest);
        for (name, table) in register_tables {
            wren_mdl.register_table(name, table);
        }
        let lineage = lineage::Lineage::new(&wren_mdl)?;
        Ok(AnalyzedWrenMDL {
            wren_mdl: Arc::new(wren_mdl),
            lineage: Arc::new(lineage),
        })
    }

    pub fn wren_mdl(&self) -> Arc<WrenMDL> {
        Arc::clone(&self.wren_mdl)
    }

    pub fn lineage(&self) -> &lineage::Lineage {
        &self.lineage
    }
}

// This is the main struct that holds the manifest and provides methods to access the models
pub struct WrenMDL {
    pub manifest: Manifest,
    pub qualified_references: HashMap<datafusion::common::Column, ColumnReference>,
    pub register_tables: HashMap<String, Arc<dyn datafusion::datasource::TableProvider>>,
}

impl WrenMDL {
    pub fn new(manifest: Manifest) -> Self {
        let mut qualifed_references = HashMap::new();
        manifest.models.iter().for_each(|model| {
            model.columns.iter().for_each(|column| {
                qualifed_references.insert(
                    from_qualified_name_str(
                        &manifest.catalog,
                        &manifest.schema,
                        model.name(),
                        column.name(),
                    ),
                    ColumnReference::new(
                        Dataset::Model(Arc::clone(model)),
                        Arc::clone(column),
                    ),
                );
            });
        });
        manifest.metrics.iter().for_each(|metric| {
            metric.dimension.iter().for_each(|dimension| {
                qualifed_references.insert(
                    from_qualified_name_str(
                        &manifest.catalog,
                        &manifest.schema,
                        metric.name(),
                        dimension.name(),
                    ),
                    ColumnReference::new(
                        Dataset::Metric(Arc::clone(metric)),
                        Arc::clone(dimension),
                    ),
                );
            });
            metric.measure.iter().for_each(|measure| {
                qualifed_references.insert(
                    from_qualified_name_str(
                        &manifest.catalog,
                        &manifest.schema,
                        metric.name(),
                        measure.name(),
                    ),
                    ColumnReference::new(
                        Dataset::Metric(Arc::clone(metric)),
                        Arc::clone(measure),
                    ),
                );
            });
        });

        WrenMDL {
            manifest,
            qualified_references: qualifed_references,
            register_tables: HashMap::new(),
        }
    }

    pub fn new_ref(manifest: Manifest) -> Arc<Self> {
        Arc::new(WrenMDL::new(manifest))
    }

    pub fn register_table(
        &mut self,
        name: String,
        table: Arc<dyn datafusion::datasource::TableProvider>,
    ) {
        self.register_tables.insert(name, table);
    }

    pub fn get_table(
        &self,
        name: &str,
    ) -> Option<Arc<dyn datafusion::datasource::TableProvider>> {
        self.register_tables.get(name).cloned()
    }

    pub fn get_register_tables(
        &self,
    ) -> &HashMap<String, Arc<dyn datafusion::datasource::TableProvider>> {
        &self.register_tables
    }

    pub fn catalog(&self) -> &str {
        &self.manifest.catalog
    }

    pub fn schema(&self) -> &str {
        &self.manifest.schema
    }

    pub fn get_model(&self, name: &str) -> Option<Arc<Model>> {
        self.manifest
            .models
            .iter()
            .find(|model| model.name == name)
            .cloned()
    }

    pub fn get_relationship(&self, name: &str) -> Option<Arc<Relationship>> {
        self.manifest
            .relationships
            .iter()
            .find(|relationship| relationship.name == name)
            .cloned()
    }

    pub fn get_column_reference(
        &self,
        column: &datafusion::common::Column,
    ) -> Option<ColumnReference> {
        self.qualified_references.get(column).cloned()
    }
}

/// Transform the SQL based on the MDL
pub fn transform_sql(analyzed_mdl: Arc<AnalyzedWrenMDL>, sql: &str) -> Result<String> {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(transform_sql_with_ctx(
        &SessionContext::new(),
        analyzed_mdl,
        sql,
    ))
}

/// Transform the SQL based on the MDL with the SessionContext
pub async fn transform_sql_with_ctx(
    ctx: &SessionContext,
    analyzed_mdl: Arc<AnalyzedWrenMDL>,
    sql: &str,
) -> Result<String> {
    info!("wren-core received SQL: {}", sql);
    let ctx = create_ctx_with_mdl(ctx, analyzed_mdl).await?;
    let plan = ctx.state().create_logical_plan(sql).await?;
    debug!("wren-core original plan:\n {plan:?}");
    let analyzed = ctx.state().optimize(&plan)?;
    debug!("wren-core final planned:\n {analyzed:?}");

    // show the planned sql
    match plan_to_sql(&analyzed) {
        Ok(sql) => {
            info!("wren-core planned SQL: {}", sql.to_string());
            Ok(sql.to_string())
        }
        Err(e) => Err(e),
    }
}

/// Analyze the decision point. It's same as the /v1/analysis/sql API in wren engine
pub fn decision_point_analyze(_wren_mdl: Arc<WrenMDL>, _sql: &str) {}

/// Cheap clone of the ColumnReference
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ColumnReference {
    pub dataset: Dataset,
    pub column: Arc<Column>,
}

impl ColumnReference {
    fn new(dataset: Dataset, column: Arc<Column>) -> Self {
        ColumnReference { dataset, column }
    }

    pub fn get_qualified_name(&self) -> String {
        format!("{}.{}", self.dataset.name(), self.column.name)
    }
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub enum Dataset {
    Model(Arc<Model>),
    Metric(Arc<Metric>),
}

impl Dataset {
    pub fn name(&self) -> &str {
        match self {
            Dataset::Model(model) => model.name(),
            Dataset::Metric(metric) => metric.name(),
        }
    }

    pub fn try_as_model(&self) -> Option<Arc<Model>> {
        match self {
            Dataset::Model(model) => Some(Arc::clone(model)),
            _ => None,
        }
    }
}

impl Display for Dataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Dataset::Model(model) => write!(f, "{}", model.name()),
            Dataset::Metric(metric) => write!(f, "{}", metric.name()),
        }
    }
}

#[cfg(test)]
mod test {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;

    use datafusion::common::not_impl_err;
    use datafusion::error::Result;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::planner::SqlToRel;
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;
    use datafusion::sql::unparser::plan_to_sql;

    use crate::logical_plan::context_provider::RemoteContextProvider;
    use crate::mdl::manifest::Manifest;
    use crate::mdl::{self, AnalyzedWrenMDL};

    #[test]
    fn test_sync_transform() -> Result<()> {
        let test_data: PathBuf =
            [env!("CARGO_MANIFEST_DIR"), "tests", "data", "mdl.json"]
                .iter()
                .collect();
        let mdl_json = fs::read_to_string(test_data.as_path())?;
        let mdl = match serde_json::from_str::<Manifest>(&mdl_json) {
            Ok(mdl) => mdl,
            Err(e) => return not_impl_err!("Failed to parse mdl json: {}", e),
        };
        let analyzed_mdl = Arc::new(AnalyzedWrenMDL::analyze(mdl)?);
        let actual = mdl::transform_sql(
            Arc::clone(&analyzed_mdl),
            "select orderkey + orderkey from test.test.orders",
        )?;
        plan_sql(&actual, Arc::clone(&analyzed_mdl))?;
        Ok(())
    }

    #[tokio::test]
    async fn test_access_model() -> Result<()> {
        let test_data: PathBuf =
            [env!("CARGO_MANIFEST_DIR"), "tests", "data", "mdl.json"]
                .iter()
                .collect();
        let mdl_json = fs::read_to_string(test_data.as_path())?;
        let mdl = match serde_json::from_str::<Manifest>(&mdl_json) {
            Ok(mdl) => mdl,
            Err(e) => return not_impl_err!("Failed to parse mdl json: {}", e),
        };
        let analyzed_mdl = Arc::new(AnalyzedWrenMDL::analyze(mdl)?);

        let tests: Vec<&str> = vec![
                "select orderkey + orderkey from test.test.orders",
                "select orderkey from test.test.orders where orders.totalprice > 10",
                "select orders.orderkey from test.test.orders left join test.test.customer on (orders.custkey = customer.custkey) where orders.totalprice > 10",
                "select orderkey, sum(totalprice) from test.test.orders group by 1",
                "select orderkey, count(*) from test.test.orders where orders.totalprice > 10 group by 1",
                "select totalcost from test.test.profile",
        // TODO: support calculated without relationship
        //     "select orderkey_plus_custkey from orders",
        ];

        for sql in tests {
            println!("Original: {}", sql);
            let actual = mdl::transform_sql_with_ctx(
                &SessionContext::new(),
                Arc::clone(&analyzed_mdl),
                sql,
            )
            .await?;
            let after_roundtrip = plan_sql(&actual, Arc::clone(&analyzed_mdl))?;
            println!("After roundtrip: {}", after_roundtrip);
        }

        Ok(())
    }

    fn plan_sql(sql: &str, analyzed_mdl: Arc<AnalyzedWrenMDL>) -> Result<String> {
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql).unwrap();
        let statement = &ast[0];

        let context_provider = RemoteContextProvider::new(&analyzed_mdl.wren_mdl())?;
        let sql_to_rel = SqlToRel::new(&context_provider);
        let rels = sql_to_rel.sql_statement_to_plan(statement.clone())?;
        // show the planned sql
        match plan_to_sql(&rels) {
            Ok(sql) => Ok(sql.to_string()),
            Err(e) => Err(e),
        }
    }

    #[tokio::test]
    async fn test_access_view() -> Result<()> {
        let test_data: PathBuf =
            [env!("CARGO_MANIFEST_DIR"), "tests", "data", "mdl.json"]
                .iter()
                .collect();
        let mdl_json = fs::read_to_string(test_data.as_path())?;
        let mdl = match serde_json::from_str::<Manifest>(&mdl_json) {
            Ok(mdl) => mdl,
            Err(e) => return not_impl_err!("Failed to parse mdl json: {}", e),
        };
        let analyzed_mdl = Arc::new(AnalyzedWrenMDL::analyze(mdl)?);
        let sql = "select * from test.test.customer_view";
        println!("Original: {}", sql);
        let actual = mdl::transform_sql_with_ctx(
            &SessionContext::new(),
            Arc::clone(&analyzed_mdl),
            sql,
        )
        .await?;
        let after_roundtrip = plan_sql(&actual, Arc::clone(&analyzed_mdl))?;
        println!("After roundtrip: {}", after_roundtrip);
        Ok(())
    }
}
