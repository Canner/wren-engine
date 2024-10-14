use crate::logical_plan::analyze::expand_view::ExpandWrenViewRule;
use crate::logical_plan::analyze::model_anlayze::ModelAnalyzeRule;
use crate::logical_plan::analyze::model_generation::ModelGenerationRule;
use crate::logical_plan::utils::from_qualified_name_str;
use crate::mdl::context::{create_ctx_with_mdl, register_table_with_mdl, WrenDataSource};
use crate::mdl::manifest::{Column, Manifest, Model, View};
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::sqlparser::keywords::ALL_KEYWORDS;
use datafusion::prelude::SessionContext;
use datafusion::sql::unparser::dialect::{Dialect, IntervalStyle};
use datafusion::sql::unparser::Unparser;
use datafusion::sql::TableReference;
pub use dataset::Dataset;
use log::{debug, info};
use manifest::Relationship;
use parking_lot::RwLock;
use regex::Regex;
use std::{collections::HashMap, sync::Arc};

pub mod builder;
pub mod context;
pub(crate) mod dataset;
pub mod function;
pub mod lineage;
pub mod manifest;
pub mod utils;

pub type SessionStateRef = Arc<RwLock<SessionState>>;

pub struct AnalyzedWrenMDL {
    pub wren_mdl: Arc<WrenMDL>,
    pub lineage: Arc<lineage::Lineage>,
}

impl AnalyzedWrenMDL {
    pub fn analyze(manifest: Manifest) -> Result<Self> {
        let wren_mdl = Arc::new(WrenMDL::new_and_register_table_ref(manifest));
        let lineage = Arc::new(lineage::Lineage::new(&wren_mdl)?);
        Ok(AnalyzedWrenMDL { wren_mdl, lineage })
    }

    pub fn analyze_with_tables(
        manifest: Manifest,
        register_tables: HashMap<String, Arc<dyn TableProvider>>,
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

pub type RegisterTables = HashMap<String, Arc<dyn TableProvider>>;
// This is the main struct that holds the manifest and provides methods to access the models
pub struct WrenMDL {
    pub manifest: Manifest,
    pub qualified_references: HashMap<datafusion::common::Column, ColumnReference>,
    pub register_tables: RegisterTables,
    pub catalog_schema_prefix: String,
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
            catalog_schema_prefix: format!("{}.{}.", &manifest.catalog, &manifest.schema),
            manifest,
            qualified_references: qualifed_references,
            register_tables: HashMap::new(),
        }
    }

    pub fn new_ref(manifest: Manifest) -> Arc<Self> {
        Arc::new(WrenMDL::new(manifest))
    }

    /// Create a WrenMDL from a manifest and register the table reference of the model as a remote table.
    /// All the column without expression will be considered a column
    pub fn new_and_register_table_ref(manifest: Manifest) -> Self {
        let mut mdl = WrenMDL::new(manifest);
        let sources: Vec<_> = mdl
            .models()
            .iter()
            .map(|model| {
                let name = TableReference::from(&model.table_reference);
                let fields: Vec<_> = model
                    .columns
                    .iter()
                    .filter(|column| {
                        !column.is_calculated
                            && column.expression.is_none()
                            && column.relationship.is_none()
                    })
                    .map(|column| column.to_field())
                    .collect();
                let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(fields));
                let datasource = WrenDataSource::new_with_schema(schema);
                (name.to_quoted_string(), Arc::new(datasource))
            })
            .collect();
        sources
            .into_iter()
            .for_each(|(name, ds_ref)| mdl.register_table(name, ds_ref));
        mdl
    }

    pub fn register_table(&mut self, name: String, table: Arc<dyn TableProvider>) {
        self.register_tables.insert(name, table);
    }

    pub fn get_table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.register_tables.get(name).cloned()
    }

    pub fn get_register_tables(&self) -> &RegisterTables {
        &self.register_tables
    }

    pub fn catalog(&self) -> &str {
        &self.manifest.catalog
    }

    pub fn schema(&self) -> &str {
        &self.manifest.schema
    }

    pub fn models(&self) -> &[Arc<Model>] {
        &self.manifest.models
    }

    pub fn get_model(&self, name: &str) -> Option<Arc<Model>> {
        self.manifest
            .models
            .iter()
            .find(|model| model.name == name)
            .cloned()
    }

    pub fn get_view(&self, name: &str) -> Option<Arc<View>> {
        self.manifest
            .views
            .iter()
            .find(|view| view.name == name)
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

    pub fn catalog_schema_prefix(&self) -> &str {
        &self.catalog_schema_prefix
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
/// Wren engine will normalize the SQL to the lower case to solve the case sensitive
/// issue for the Wren view
pub async fn transform_sql_with_ctx(
    ctx: &SessionContext,
    analyzed_mdl: Arc<AnalyzedWrenMDL>,
    sql: &str,
) -> Result<String> {
    info!("wren-core received SQL: {}", sql);
    let ctx = create_ctx_with_mdl(ctx, Arc::clone(&analyzed_mdl), false).await?;
    let plan = ctx.state().create_logical_plan(sql).await?;
    debug!("wren-core original plan:\n {plan}");
    let analyzed = ctx.state().optimize(&plan)?;
    debug!("wren-core final planned:\n {analyzed}");

    let unparser = Unparser::new(&WrenDialect {}).with_pretty(true);
    // show the planned sql
    match unparser.plan_to_sql(&analyzed) {
        Ok(sql) => {
            // TODO: workaround to remove unnecessary catalog and schema of mdl
            let replaced = sql
                .to_string()
                .replace(analyzed_mdl.wren_mdl().catalog_schema_prefix(), "");
            info!("wren-core planned SQL: {}", replaced);
            Ok(replaced)
        }
        Err(e) => Err(e),
    }
}

/// WrenDialect is a dialect for Wren engine. Handle the identifier quote style based on the
/// original Datafusion Dialect implementation but with more strict rules.
/// If the identifier isn't lowercase, it will be quoted.
pub struct WrenDialect {}

impl Dialect for WrenDialect {
    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        let identifier_regex = Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").unwrap();
        if ALL_KEYWORDS.contains(&identifier.to_uppercase().as_str())
            || !identifier_regex.is_match(identifier)
            || non_lowercase(identifier)
        {
            Some('"')
        } else {
            None
        }
    }

    fn interval_style(&self) -> IntervalStyle {
        IntervalStyle::SQLStandard
    }
}

fn non_lowercase(sql: &str) -> bool {
    let lowercase = sql.to_lowercase();
    lowercase != sql
}

/// Apply Wren Rules to a given session context with a WrenMDL
///
/// TODO: There're some issue about apply the rule with the native optimize rules of datafusion
/// Recommend to use [transform_sql_with_ctx] generated the SQL text instead.
pub async fn apply_wren_rules(
    ctx: &SessionContext,
    analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
) -> Result<()> {
    // expand the view should be the first rule
    ctx.add_analyzer_rule(Arc::new(ExpandWrenViewRule::new(
        Arc::clone(&analyzed_wren_mdl),
        ctx.state_ref(),
    )));
    ctx.add_analyzer_rule(Arc::new(ModelAnalyzeRule::new(
        Arc::clone(&analyzed_wren_mdl),
        ctx.state_ref(),
    )));
    ctx.add_analyzer_rule(Arc::new(ModelGenerationRule::new(Arc::clone(
        &analyzed_wren_mdl,
    ))));
    register_table_with_mdl(ctx, analyzed_wren_mdl.wren_mdl()).await
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

#[cfg(test)]
mod test {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;

    use crate::mdl::builder::{ColumnBuilder, ManifestBuilder, ModelBuilder};
    use crate::mdl::manifest::Manifest;
    use crate::mdl::{self, AnalyzedWrenMDL};
    use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use datafusion::common::not_impl_err;
    use datafusion::common::Result;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::unparser::plan_to_sql;

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
        let _ = mdl::transform_sql(
            Arc::clone(&analyzed_mdl),
            "select o_orderkey + o_orderkey from test.test.orders",
        )?;
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
                "select o_orderkey + o_orderkey from test.test.orders",
                "select o_orderkey from test.test.orders where orders.o_totalprice > 10",
                "select orders.o_orderkey from test.test.orders left join test.test.customer on (orders.o_custkey = customer.c_custkey) where orders.o_totalprice > 10",
                "select o_orderkey, sum(o_totalprice) from test.test.orders group by 1",
                "select o_orderkey, count(*) from test.test.orders where orders.o_totalprice > 10 group by 1",
                "select totalcost from test.test.profile",
                "select totalcost from profile",
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
            println!("After transform: {}", actual);
            assert_sql_valid_executable(&actual).await?;
        }

        Ok(())
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
        assert_sql_valid_executable(&actual).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_uppercase_catalog_schema() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_batch("customer", customer())?;
        let manifest = ManifestBuilder::new()
            .catalog("CTest")
            .schema("STest")
            .model(
                ModelBuilder::new("Customer")
                    .table_reference("datafusion.public.customer")
                    .column(ColumnBuilder::new("Custkey", "int").build())
                    .column(ColumnBuilder::new("Name", "string").build())
                    .build(),
            )
            .build();
        let analyzed_mdl = Arc::new(AnalyzedWrenMDL::analyze(manifest)?);
        let sql = r#"select * from "CTest"."STest"."Customer""#;
        let actual = mdl::transform_sql_with_ctx(
            &SessionContext::new(),
            Arc::clone(&analyzed_mdl),
            sql,
        )
        .await?;
        assert_eq!(actual,
            "SELECT \"Customer\".\"Custkey\", \"Customer\".\"Name\" FROM \
            (SELECT datafusion.public.customer.\"Custkey\" AS \"Custkey\", \
            datafusion.public.customer.\"Name\" AS \"Name\" FROM datafusion.public.customer) AS \"Customer\"");
        Ok(())
    }

    async fn assert_sql_valid_executable(sql: &str) -> Result<()> {
        let ctx = SessionContext::new();
        // To roundtrip testing, we should register the mock table for the planned sql.
        ctx.register_batch("orders", orders())?;
        ctx.register_batch("customer", customer())?;
        ctx.register_batch("profile", profile())?;

        // show the planned sql
        let df = ctx.sql(sql).await?;
        let plan = df.into_optimized_plan()?;
        let after_roundtrip = plan_to_sql(&plan).map(|sql| sql.to_string())?;
        println!("After roundtrip: {}", after_roundtrip);
        match ctx.sql(sql).await?.collect().await {
            Ok(_) => Ok(()),
            Err(e) => {
                eprintln!("Error: {e}");
                Err(e)
            }
        }
    }

    /// Return a RecordBatch with made up data about customer
    fn customer() -> RecordBatch {
        let custkey: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let name: ArrayRef =
            Arc::new(StringArray::from_iter_values(["Gura", "Azki", "Ina"]));
        RecordBatch::try_from_iter(vec![("c_custkey", custkey), ("c_name", name)])
            .unwrap()
    }

    /// Return a RecordBatch with made up data about profile
    fn profile() -> RecordBatch {
        let custkey: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let phone: ArrayRef = Arc::new(StringArray::from_iter_values([
            "123456", "234567", "345678",
        ]));
        let sex: ArrayRef = Arc::new(StringArray::from_iter_values(["M", "M", "F"]));
        RecordBatch::try_from_iter(vec![
            ("p_custkey", custkey),
            ("p_phone", phone),
            ("p_sex", sex),
        ])
        .unwrap()
    }

    /// Return a RecordBatch with made up data about orders
    fn orders() -> RecordBatch {
        let orderkey: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let custkey: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let totalprice: ArrayRef = Arc::new(Int64Array::from(vec![100, 200, 300]));
        RecordBatch::try_from_iter(vec![
            ("o_orderkey", orderkey),
            ("o_custkey", custkey),
            ("o_totalprice", totalprice),
        ])
        .unwrap()
    }
}
