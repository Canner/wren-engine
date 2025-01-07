use crate::logical_plan::utils::{from_qualified_name_str, map_data_type};
use crate::mdl::builder::ManifestBuilder;
use crate::mdl::context::{create_ctx_with_mdl, WrenDataSource};
use crate::mdl::dialect::WrenDialect;
use crate::mdl::function::{
    ByPassAggregateUDF, ByPassScalarUDF, ByPassWindowFunction, FunctionType,
    RemoteFunction,
};
use crate::mdl::manifest::{Column, Manifest, Metric, Model, View};
use crate::mdl::utils::to_field;
use crate::DataFusionError;
use datafusion::arrow::datatypes::Field;
use datafusion::common::internal_datafusion_err;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser::ast::{Expr, ExprWithAlias, Ident};
use datafusion::sql::sqlparser::dialect::dialect_from_str;
use datafusion::sql::unparser::Unparser;
use datafusion::sql::TableReference;
pub use dataset::Dataset;
use log::{debug, info};
use manifest::Relationship;
use parking_lot::RwLock;
use std::hash::Hash;
use std::{collections::HashMap, sync::Arc};
use wren_core_base::mdl::DataSource;

pub mod builder {
    pub use wren_core_base::mdl::builder::*;
}
pub mod context;
pub(crate) mod dataset;
mod dialect;
pub mod function;
pub mod lineage;
pub mod manifest {
    pub use wren_core_base::mdl::manifest::*;
}
pub mod utils;

pub type SessionStateRef = Arc<RwLock<SessionState>>;

pub struct AnalyzedWrenMDL {
    pub wren_mdl: Arc<WrenMDL>,
    pub lineage: Arc<lineage::Lineage>,
}

impl Hash for AnalyzedWrenMDL {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.wren_mdl.hash(state);
    }
}

impl Default for AnalyzedWrenMDL {
    fn default() -> Self {
        let manifest = ManifestBuilder::default().build();
        let wren_mdl = WrenMDL::new(manifest);
        let lineage = lineage::Lineage::new(&wren_mdl).unwrap();
        AnalyzedWrenMDL {
            wren_mdl: Arc::new(wren_mdl),
            lineage: Arc::new(lineage),
        }
    }
}

impl AnalyzedWrenMDL {
    pub fn analyze(manifest: Manifest) -> Result<Self> {
        let wren_mdl = Arc::new(WrenMDL::infer_and_register_remote_table(manifest)?);
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

impl Hash for WrenMDL {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.manifest.hash(state);
    }
}

impl WrenMDL {
    pub fn new(manifest: Manifest) -> Self {
        let mut qualifed_references = HashMap::new();
        manifest.models.iter().for_each(|model| {
            model.get_visible_columns().for_each(|column| {
                qualifed_references.insert(
                    from_qualified_name_str(
                        &manifest.catalog,
                        &manifest.schema,
                        model.name(),
                        column.name(),
                    ),
                    ColumnReference::new(
                        Dataset::Model(Arc::clone(model)),
                        Arc::clone(&column),
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
    pub fn infer_and_register_remote_table(manifest: Manifest) -> Result<Self> {
        let mut mdl = WrenMDL::new(manifest);
        let sources: Vec<_> = mdl
            .models()
            .iter()
            .map(|model| {
                let name = TableReference::from(model.table_reference());
                let fields: Vec<_> = model
                    .columns
                    .iter()
                    .filter_map(|column| Self::infer_source_column(column).ok().flatten())
                    .collect();
                let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(fields));
                let datasource = WrenDataSource::new_with_schema(schema);
                (name.to_quoted_string(), Arc::new(datasource))
            })
            .collect();
        sources
            .into_iter()
            .for_each(|(name, ds_ref)| mdl.register_table(name, ds_ref));
        Ok(mdl)
    }

    /// Infer the source column from the column expression.
    ///
    /// If the column is calculated or has a relationship, it's not a source column.
    /// If the column without expression, it's a source column.
    /// If the column has an expression, it will try to infer the source column from the expression.
    /// If the expression is a simple column reference, it's the source column name.
    /// If the expression is a complex expression, it can't be inferred.
    ///
    fn infer_source_column(column: &Column) -> Result<Option<Field>> {
        if column.is_calculated || column.relationship.is_some() {
            return Ok(None);
        }

        if let Some(expression) = column.expression() {
            let ExprWithAlias { expr, alias } = WrenMDL::sql_to_expr(expression)?;
            // if the column is a simple column reference, we can infer the column name
            if let Some(name) = Self::collect_one_column(&expr) {
                Ok(Some(Field::new(
                    alias.map(|a| a.value).unwrap_or_else(|| name.value.clone()),
                    map_data_type(&column.r#type)?,
                    column.not_null,
                )))
            } else {
                Ok(None)
            }
        } else {
            Ok(Some(to_field(column)?))
        }
    }

    fn sql_to_expr(sql: &str) -> Result<ExprWithAlias> {
        let dialect = dialect_from_str("generic").ok_or_else(|| {
            internal_datafusion_err!("Failed to create dialect from generic")
        })?;

        let expr = DFParser::parse_sql_into_expr_with_dialect(sql, dialect.as_ref())?;
        Ok(expr)
    }

    /// Collect the last identifier of the expression
    /// e.g. "a"."b"."c" -> c
    /// e.g. "a" -> a
    /// others -> None
    fn collect_one_column(expr: &Expr) -> Option<&Ident> {
        match expr {
            Expr::CompoundIdentifier(idents) => idents.last(),
            Expr::Identifier(ident) => Some(ident),
            _ => None,
        }
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

    pub fn views(&self) -> &[Arc<View>] {
        &self.manifest.views
    }

    pub fn relationships(&self) -> &[Arc<Relationship>] {
        &self.manifest.relationships
    }

    pub fn metrics(&self) -> &[Arc<Metric>] {
        &self.manifest.metrics
    }

    pub fn data_source(&self) -> Option<DataSource> {
        self.manifest.data_source
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
pub fn transform_sql(
    analyzed_mdl: Arc<AnalyzedWrenMDL>,
    remote_functions: &[RemoteFunction],
    sql: &str,
) -> Result<String> {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(transform_sql_with_ctx(
        &SessionContext::new(),
        analyzed_mdl,
        remote_functions,
        sql,
    ))
}

/// Transform the SQL based on the MDL with the SessionContext
/// Wren engine will normalize the SQL to the lower case to solve the case-sensitive
/// issue for the Wren view
pub async fn transform_sql_with_ctx(
    ctx: &SessionContext,
    analyzed_mdl: Arc<AnalyzedWrenMDL>,
    remote_functions: &[RemoteFunction],
    sql: &str,
) -> Result<String> {
    info!("wren-core received SQL: {}", sql);
    remote_functions.iter().try_for_each(|remote_function| {
        debug!("Registering remote function: {:?}", remote_function);
        register_remote_function(ctx, remote_function)?;
        Ok::<_, DataFusionError>(())
    })?;
    let ctx = create_ctx_with_mdl(ctx, Arc::clone(&analyzed_mdl), false).await?;
    let plan = ctx.state().create_logical_plan(sql).await?;
    debug!("wren-core original plan:\n {plan}");
    let analyzed = ctx.state().optimize(&plan)?;
    debug!("wren-core final planned:\n {analyzed}");

    let data_source = analyzed_mdl.wren_mdl().data_source().unwrap_or_default();
    let wren_dialect = WrenDialect::new(&data_source);
    let unparser = Unparser::new(&wren_dialect).with_pretty(true);
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

fn register_remote_function(
    ctx: &SessionContext,
    remote_function: &RemoteFunction,
) -> Result<()> {
    match &remote_function.function_type {
        FunctionType::Scalar => {
            ctx.register_udf(ScalarUDF::new_from_impl(ByPassScalarUDF::new(
                &remote_function.name,
                map_data_type(&remote_function.return_type)?,
            )))
        }
        FunctionType::Aggregate => {
            ctx.register_udaf(AggregateUDF::new_from_impl(ByPassAggregateUDF::new(
                &remote_function.name,
                map_data_type(&remote_function.return_type)?,
            )))
        }
        FunctionType::Window => {
            ctx.register_udwf(WindowUDF::new_from_impl(ByPassWindowFunction::new(
                &remote_function.name,
                map_data_type(&remote_function.return_type)?,
            )))
        }
    };
    Ok(())
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
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;

    use crate::mdl::builder::{ColumnBuilder, ManifestBuilder, ModelBuilder};
    use crate::mdl::context::create_ctx_with_mdl;
    use crate::mdl::function::RemoteFunction;
    use crate::mdl::manifest::DataSource::MySQL;
    use crate::mdl::manifest::Manifest;
    use crate::mdl::{self, transform_sql_with_ctx, AnalyzedWrenMDL};
    use datafusion::arrow::array::{
        ArrayRef, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray,
    };
    use datafusion::assert_batches_eq;
    use datafusion::common::not_impl_err;
    use datafusion::common::Result;
    use datafusion::config::ConfigOptions;
    use datafusion::prelude::{SessionConfig, SessionContext};
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
            &[],
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
                "select sum(c_custkey) over (order by c_name) from test.test.customer limit 1",
        // TODO: support calculated without relationship
        //     "select orderkey_plus_custkey from orders",
        ];

        for sql in tests {
            println!("Original: {}", sql);
            let actual = mdl::transform_sql_with_ctx(
                &SessionContext::new(),
                Arc::clone(&analyzed_mdl),
                &[],
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
        let _ = transform_sql_with_ctx(
            &SessionContext::new(),
            Arc::clone(&analyzed_mdl),
            &[],
            sql,
        )
        .await?;
        // TODO: There are some issues for round trip of the view plan
        // Disable the roundtrip testing before fixed.
        // see https://github.com/apache/datafusion/issues/13272
        // assert_sql_valid_executable(&actual).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_plan_calculation_without_unnamed_subquery() -> Result<()> {
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
        let sql = "select totalcost from profile";
        let result = transform_sql_with_ctx(
            &SessionContext::new(),
            Arc::clone(&analyzed_mdl),
            &[],
            sql,
        )
        .await?;
        let expected = "SELECT profile.totalcost FROM (SELECT totalcost.totalcost FROM \
        (SELECT __relation__2.p_custkey AS p_custkey, sum(CAST(__relation__2.o_totalprice AS BIGINT)) AS totalcost FROM \
        (SELECT __relation__1.c_custkey, orders.o_custkey, orders.o_totalprice, __relation__1.p_custkey FROM \
        (SELECT __source.o_custkey AS o_custkey, __source.o_totalprice AS o_totalprice FROM orders AS __source) AS orders RIGHT JOIN \
        (SELECT customer.c_custkey, profile.p_custkey FROM (SELECT __source.c_custkey AS c_custkey FROM customer AS __source) AS customer RIGHT JOIN \
        (SELECT __source.p_custkey AS p_custkey FROM profile AS __source) AS profile ON customer.c_custkey = profile.p_custkey) AS __relation__1 \
        ON orders.o_custkey = __relation__1.c_custkey) AS __relation__2 GROUP BY __relation__2.p_custkey) AS totalcost) AS profile";
        assert_eq!(result, expected);

        let sql = "select totalcost from profile where p_sex = 'M'";
        let result = transform_sql_with_ctx(
            &SessionContext::new(),
            Arc::clone(&analyzed_mdl),
            &[],
            sql,
        )
        .await?;
        assert_eq!(result,
          "SELECT profile.totalcost FROM (SELECT __relation__1.p_sex, __relation__1.totalcost FROM \
          (SELECT totalcost.p_custkey, profile.p_sex, totalcost.totalcost FROM (SELECT __relation__2.p_custkey AS p_custkey, \
          sum(CAST(__relation__2.o_totalprice AS BIGINT)) AS totalcost FROM (SELECT __relation__1.c_custkey, orders.o_custkey, \
          orders.o_totalprice, __relation__1.p_custkey FROM (SELECT __source.o_custkey AS o_custkey, __source.o_totalprice AS o_totalprice \
          FROM orders AS __source) AS orders RIGHT JOIN (SELECT customer.c_custkey, profile.p_custkey FROM \
          (SELECT __source.c_custkey AS c_custkey FROM customer AS __source) AS customer RIGHT JOIN \
          (SELECT __source.p_custkey AS p_custkey FROM profile AS __source) AS profile ON customer.c_custkey = profile.p_custkey) AS __relation__1 \
          ON orders.o_custkey = __relation__1.c_custkey) AS __relation__2 GROUP BY __relation__2.p_custkey) AS totalcost RIGHT JOIN \
          (SELECT __source.p_custkey AS p_custkey, __source.p_sex AS p_sex FROM profile AS __source) AS profile \
          ON totalcost.p_custkey = profile.p_custkey) AS __relation__1) AS profile WHERE profile.p_sex = 'M'");
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
            &[],
            sql,
        )
        .await?;
        assert_eq!(actual,
            "SELECT \"Customer\".\"Custkey\", \"Customer\".\"Name\" FROM \
            (SELECT __source.\"Custkey\" AS \"Custkey\", __source.\"Name\" AS \"Name\" FROM \
            datafusion.public.customer AS __source) AS \"Customer\"");
        Ok(())
    }

    #[tokio::test]
    async fn test_remote_function() -> Result<()> {
        env_logger::init();
        let test_data: PathBuf =
            [env!("CARGO_MANIFEST_DIR"), "tests", "data", "functions.csv"]
                .iter()
                .collect();
        let ctx = SessionContext::new();
        let functions = csv::Reader::from_path(test_data)
            .unwrap()
            .into_deserialize::<RemoteFunction>()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();
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
        let actual = transform_sql_with_ctx(
            &ctx,
            Arc::clone(&analyzed_mdl),
            &functions,
            r#"select add_two("Custkey") from "Customer""#,
        )
        .await?;
        assert_eq!(actual, "SELECT add_two(\"Customer\".\"Custkey\") FROM (SELECT \"Customer\".\"Custkey\" \
        FROM (SELECT __source.\"Custkey\" AS \"Custkey\" FROM datafusion.public.customer AS __source) AS \"Customer\") AS \"Customer\"");

        let actual = transform_sql_with_ctx(
            &ctx,
            Arc::clone(&analyzed_mdl),
            &functions,
            r#"select median("Custkey") from "CTest"."STest"."Customer" group by "Name""#,
        )
        .await?;
        assert_eq!(actual, "SELECT median(\"Customer\".\"Custkey\") FROM (SELECT \"Customer\".\"Custkey\", \"Customer\".\"Name\" \
        FROM (SELECT __source.\"Custkey\" AS \"Custkey\", __source.\"Name\" AS \"Name\" FROM datafusion.public.customer AS __source) AS \"Customer\") AS \"Customer\" \
        GROUP BY \"Customer\".\"Name\"");

        // TODO: support window functions analysis
        // let actual = transform_sql_with_ctx(
        //     &ctx,
        //     Arc::clone(&analyzed_mdl),
        //     &functions,
        //     r#"select max_if("Custkey") OVER (PARTITION BY "Name") from "Customer""#,
        // ).await?;
        // assert_eq!(actual, "");

        Ok(())
    }

    #[tokio::test]
    async fn test_unicode_remote_column_name() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_batch("artist", artist())?;
        let manifest = ManifestBuilder::new()
            .catalog("wren")
            .schema("test")
            .model(
                ModelBuilder::new("artist")
                    .table_reference("artist")
                    .column(ColumnBuilder::new("名字", "string").build())
                    .column(
                        ColumnBuilder::new("name_append", "string")
                            .expression(r#""名字" || "名字""#)
                            .build(),
                    )
                    .column(
                        ColumnBuilder::new("group", "string")
                            .expression(r#""組別""#)
                            .build(),
                    )
                    .column(
                        ColumnBuilder::new("subscribe", "int")
                            .expression(r#""訂閱數""#)
                            .build(),
                    )
                    .column(
                        ColumnBuilder::new("subscribe_plus", "int")
                            .expression(r#""訂閱數" + 1"#)
                            .build(),
                    )
                    .build(),
            )
            .build();
        let analyzed_mdl = Arc::new(AnalyzedWrenMDL::analyze(manifest)?);
        let sql = r#"select * from wren.test.artist"#;
        let actual = transform_sql_with_ctx(
            &SessionContext::new(),
            Arc::clone(&analyzed_mdl),
            &[],
            sql,
        )
        .await?;
        assert_eq!(actual,
                   "SELECT artist.\"名字\", artist.name_append, artist.\"group\", artist.subscribe_plus, artist.subscribe \
                   FROM (SELECT __source.\"名字\" AS \"名字\", __source.\"名字\" || __source.\"名字\" AS name_append, __source.\"組別\" AS \"group\", \
                   CAST(__source.\"訂閱數\" AS BIGINT) + 1 AS subscribe_plus, __source.\"訂閱數\" AS subscribe FROM artist AS __source) AS artist"
);
        ctx.sql(&actual).await?.show().await?;

        let sql = r#"select group from wren.test.artist"#;
        let actual = transform_sql_with_ctx(
            &SessionContext::new(),
            Arc::clone(&analyzed_mdl),
            &[],
            sql,
        )
        .await?;
        assert_eq!(actual,
                   "SELECT artist.\"group\" FROM (SELECT artist.\"group\" FROM (SELECT __source.\"組別\" AS \"group\" FROM artist AS __source) AS artist) AS artist");
        ctx.sql(&actual).await?.show().await?;

        let sql = r#"select subscribe_plus from wren.test.artist"#;
        let actual = mdl::transform_sql_with_ctx(
            &SessionContext::new(),
            Arc::clone(&analyzed_mdl),
            &[],
            sql,
        )
        .await?;
        assert_eq!(actual,
                   "SELECT artist.subscribe_plus FROM (SELECT artist.subscribe_plus FROM (SELECT CAST(__source.\"訂閱數\" AS BIGINT) + 1 AS subscribe_plus FROM artist AS __source) AS artist) AS artist");
        ctx.sql(&actual).await?.show().await
    }

    #[tokio::test]
    async fn test_invalid_infer_remote_table() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_batch("artist", artist())?;
        let manifest = ManifestBuilder::new()
            .catalog("wren")
            .schema("test")
            .model(
                ModelBuilder::new("artist")
                    .table_reference("artist")
                    .column(
                        ColumnBuilder::new("name_append", "string")
                            .expression(r#""名字" || "名字""#)
                            .build(),
                    )
                    .column(
                        ColumnBuilder::new("lower_name", "string")
                            .expression(r#"lower("名字")"#)
                            .build(),
                    )
                    .build(),
            )
            .build();

        let analyzed_mdl = Arc::new(AnalyzedWrenMDL::analyze(manifest)?);
        let sql = r#"select name_append from wren.test.artist"#;
        let _ = transform_sql_with_ctx(
            &SessionContext::new(),
            Arc::clone(&analyzed_mdl),
            &[],
            sql,
        )
        .await
        .map_err(|e| {
            assert_eq!(
                e.to_string(),
                "ModelAnalyzeRule\ncaused by\nSchema error: No field named \"名字\"."
            )
        });

        let sql = r#"select lower_name from wren.test.artist"#;
        let _ = transform_sql_with_ctx(
            &SessionContext::new(),
            Arc::clone(&analyzed_mdl),
            &[],
            sql,
        )
        .await
        .map_err(|e| {
            assert_eq!(
                e.to_string(),
                "ModelAnalyzeRule\ncaused by\nSchema error: No field named \"名字\"."
            )
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_query_hidden_column() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_batch("artist", artist())?;
        let manifest = ManifestBuilder::new()
            .catalog("wren")
            .schema("test")
            .model(
                ModelBuilder::new("artist")
                    .table_reference("artist")
                    .column(ColumnBuilder::new("名字", "string").hidden(true).build())
                    .column(
                        ColumnBuilder::new("串接名字", "string")
                            .expression(r#""名字" || "名字""#)
                            .build(),
                    )
                    .build(),
            )
            .build();

        let analyzed_mdl = Arc::new(AnalyzedWrenMDL::analyze(manifest)?);
        let sql = r#"select "串接名字" from wren.test.artist"#;
        let actual = transform_sql_with_ctx(
            &SessionContext::new(),
            Arc::clone(&analyzed_mdl),
            &[],
            sql,
        )
        .await?;
        assert_eq!(actual,
                   "SELECT artist.\"串接名字\" FROM (SELECT artist.\"串接名字\" FROM \
                   (SELECT __source.\"名字\" || __source.\"名字\" AS \"串接名字\" FROM artist AS __source) AS artist) AS artist");

        let sql = r#"select * from wren.test.artist"#;
        let actual = transform_sql_with_ctx(
            &SessionContext::new(),
            Arc::clone(&analyzed_mdl),
            &[],
            sql,
        )
        .await?;
        assert_eq!(actual,
                   "SELECT artist.\"串接名字\" FROM (SELECT __source.\"名字\" || __source.\"名字\" AS \"串接名字\" FROM artist AS __source) AS artist");

        let sql = r#"select "名字" from wren.test.artist"#;
        let _ = transform_sql_with_ctx(
            &SessionContext::new(),
            Arc::clone(&analyzed_mdl),
            &[],
            sql,
        )
            .await.map_err(|e| {
                assert_eq!(
                    e.to_string(),
                    "Schema error: No field named \"名字\". Valid fields are wren.test.artist.\"串接名字\"."
                )
            });
        Ok(())
    }

    #[tokio::test]
    async fn test_disable_simplify_expression() -> Result<()> {
        let sql = "select current_date";
        let actual = transform_sql_with_ctx(
            &SessionContext::new(),
            Arc::new(AnalyzedWrenMDL::default()),
            &[],
            sql,
        )
        .await?;
        assert_eq!(actual, "SELECT current_date()");
        Ok(())
    }

    /// This test will be failed if the `出道時間` is not inferred as a timestamp column correctly.
    #[tokio::test]
    async fn test_infer_timestamp_column() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_batch("artist", artist())?;
        let manifest = ManifestBuilder::new()
            .catalog("wren")
            .schema("test")
            .model(
                ModelBuilder::new("artist")
                    .table_reference("artist")
                    .column(ColumnBuilder::new("出道時間", "timestamp").build())
                    .build(),
            )
            .build();

        let analyzed_mdl = Arc::new(AnalyzedWrenMDL::analyze(manifest)?);
        let sql = r#"select current_date > "出道時間" from wren.test.artist"#;
        let actual = transform_sql_with_ctx(
            &SessionContext::new(),
            Arc::clone(&analyzed_mdl),
            &[],
            sql,
        )
        .await?;
        assert_eq!(actual,
                   "SELECT CAST(current_date() AS TIMESTAMP) > artist.\"出道時間\" FROM \
                   (SELECT artist.\"出道時間\" FROM (SELECT __source.\"出道時間\" AS \"出道時間\" FROM artist AS __source) AS artist) AS artist");
        Ok(())
    }

    #[tokio::test]
    async fn test_disable_count_wildcard_rule() -> Result<()> {
        let ctx = SessionContext::new();

        let analyzed_mdl = Arc::new(AnalyzedWrenMDL::default());
        let sql = "select count(*) from (select 1)";
        let actual =
            transform_sql_with_ctx(&ctx, Arc::clone(&analyzed_mdl), &[], sql).await?;
        assert_eq!(actual, "SELECT count(*) FROM (SELECT 1)");
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

    #[tokio::test]
    async fn test_mysql_style_interval() -> Result<()> {
        let ctx = SessionContext::new();
        let analyzed_mdl = Arc::new(AnalyzedWrenMDL::default());
        let sql = "select interval 1 day";
        let actual =
            transform_sql_with_ctx(&ctx, Arc::clone(&analyzed_mdl), &[], sql).await?;
        assert_eq!(actual, "SELECT INTERVAL 1 DAY");

        let sql = "SELECT INTERVAL '1 YEAR 1 MONTH'";
        let actual =
            transform_sql_with_ctx(&ctx, Arc::clone(&analyzed_mdl), &[], sql).await?;
        assert_eq!(actual, "SELECT INTERVAL 13 MONTH");

        let sql = "SELECT INTERVAL '1' YEAR + INTERVAL '2' MONTH + INTERVAL '3' DAY";
        let actual =
            transform_sql_with_ctx(&ctx, Arc::clone(&analyzed_mdl), &[], sql).await?;
        assert_eq!(
            actual,
            "SELECT INTERVAL 12 MONTH + INTERVAL 2 MONTH + INTERVAL 3 DAY"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_simplify_timestamp() -> Result<()> {
        let ctx = SessionContext::new();
        let analyzed_mdl = Arc::new(AnalyzedWrenMDL::default());
        let sql = "select timestamp '2011-01-01 18:00:00 +08:00'";
        let actual =
            transform_sql_with_ctx(&ctx, Arc::clone(&analyzed_mdl), &[], sql).await?;
        assert_eq!(actual, "SELECT CAST('2011-01-01 10:00:00' AS TIMESTAMP) AS \"Utf8(\"\"2011-01-01 18:00:00 +08:00\"\")\"");

        let sql = "select timestamp '2011-01-01 18:00:00 Asia/Taipei'";
        let actual =
            transform_sql_with_ctx(&ctx, Arc::clone(&analyzed_mdl), &[], sql).await?;
        assert_eq!(actual, "SELECT CAST('2011-01-01 10:00:00' AS TIMESTAMP) AS \"Utf8(\"\"2011-01-01 18:00:00 Asia/Taipei\"\")\"");
        Ok(())
    }

    #[tokio::test]
    async fn test_eval_timestamp_with_session_timezone() -> Result<()> {
        let mut config = ConfigOptions::new();
        config.execution.time_zone = Some("+08:00".to_string());
        let session_config = SessionConfig::from(config);
        let ctx = SessionContext::new_with_config(session_config);
        let analyzed_mdl = Arc::new(AnalyzedWrenMDL::default());
        let sql = "select timestamp '2011-01-01 18:00:00'";
        let actual =
            transform_sql_with_ctx(&ctx, Arc::clone(&analyzed_mdl), &[], sql).await?;
        // TIMESTAMP doesn't have timezone, so the timezone will be ignored
        assert_eq!(actual, "SELECT CAST('2011-01-01 18:00:00' AS TIMESTAMP) AS \"Utf8(\"\"2011-01-01 18:00:00\"\")\"");

        let sql = "select timestamp with time zone '2011-01-01 18:00:00'";
        let actual =
            transform_sql_with_ctx(&ctx, Arc::clone(&analyzed_mdl), &[], sql).await?;
        // TIMESTAMP WITH TIME ZONE will be converted to the session timezone
        assert_eq!(actual, "SELECT CAST('2011-01-01 10:00:00' AS TIMESTAMP) AS \"Utf8(\"\"2011-01-01 18:00:00\"\")\"");

        let mut config = ConfigOptions::new();
        config.execution.time_zone = Some("America/New_York".to_string());
        let session_config = SessionConfig::from(config);
        let ctx = SessionContext::new_with_config(session_config);
        let analyzed_mdl = Arc::new(AnalyzedWrenMDL::default());
        // TIMESTAMP WITH TIME ZONE will be converted to the session timezone with daylight saving (UTC -5)
        let sql = "select timestamp with time zone '2024-01-15 18:00:00'";
        let actual =
            transform_sql_with_ctx(&ctx, Arc::clone(&analyzed_mdl), &[], sql).await?;
        assert_eq!(actual, "SELECT CAST('2024-01-15 23:00:00' AS TIMESTAMP) AS \"Utf8(\"\"2024-01-15 18:00:00\"\")\"");

        // TIMESTAMP WITH TIME ZONE will be converted to the session timezone without daylight saving (UTC -4)
        let sql = "select timestamp with time zone '2024-07-15 18:00:00'";
        let actual =
            transform_sql_with_ctx(&ctx, Arc::clone(&analyzed_mdl), &[], sql).await?;
        assert_eq!(actual, "SELECT CAST('2024-07-15 22:00:00' AS TIMESTAMP) AS \"Utf8(\"\"2024-07-15 18:00:00\"\")\"");
        Ok(())
    }

    #[tokio::test]
    async fn test_disable_pushdown_filter() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_batch("artist", artist())?;
        let manifest = ManifestBuilder::new()
            .catalog("wren")
            .schema("test")
            .model(
                ModelBuilder::new("artist")
                    .table_reference("artist")
                    .column(
                        ColumnBuilder::new("出道時間", "timestamp")
                            .hidden(true)
                            .build(),
                    )
                    .column(
                        ColumnBuilder::new("cast_timestamptz", "timestamptz")
                            .expression(r#"cast("出道時間" as timestamp with time zone)"#)
                            .build(),
                    )
                    .build(),
            )
            .build();

        let analyzed_mdl = Arc::new(AnalyzedWrenMDL::analyze(manifest)?);
        let sql = r#"select count(*) from wren.test.artist where cast(cast_timestamptz as timestamp) > timestamp '2011-01-01 21:00:00'"#;
        let actual = transform_sql_with_ctx(
            &SessionContext::new(),
            Arc::clone(&analyzed_mdl),
            &[],
            sql,
        )
        .await?;
        assert_eq!(actual,
                   "SELECT count(*) FROM (SELECT artist.cast_timestamptz FROM \
                   (SELECT CAST(__source.\"出道時間\" AS TIMESTAMP WITH TIME ZONE) AS cast_timestamptz \
                   FROM artist AS __source) AS artist) AS artist WHERE CAST(artist.cast_timestamptz AS TIMESTAMP) > CAST('2011-01-01 21:00:00' AS TIMESTAMP)");
        Ok(())
    }

    #[tokio::test]
    async fn test_register_timestamptz() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_batch("timestamp_table", timestamp_table())?;
        let provider = ctx
            .catalog("datafusion")
            .unwrap()
            .schema("public")
            .unwrap()
            .table("timestamp_table")
            .await?
            .unwrap();
        let mut registers = HashMap::new();
        registers.insert(
            "datafusion.public.timestamp_table".to_string(),
            Arc::clone(&provider),
        );
        let manifest = ManifestBuilder::new()
            .catalog("wren")
            .schema("test")
            .model(
                ModelBuilder::new("timestamp_table")
                    .table_reference("datafusion.public.timestamp_table")
                    .column(ColumnBuilder::new("timestamp_col", "timestamp").build())
                    .column(ColumnBuilder::new("timestamptz_col", "timestamptz").build())
                    .build(),
            )
            .build();

        let analyzed_mdl =
            Arc::new(AnalyzedWrenMDL::analyze_with_tables(manifest, registers)?);
        let ctx = create_ctx_with_mdl(&ctx, Arc::clone(&analyzed_mdl), true).await?;
        let sql = r#"select arrow_typeof(timestamp_col), arrow_typeof(timestamptz_col) from wren.test.timestamp_table limit 1"#;
        let result = ctx.sql(sql).await?.collect().await?;
        let expected = vec![
            "+---------------------------------------------+-----------------------------------------------+",
            "| arrow_typeof(timestamp_table.timestamp_col) | arrow_typeof(timestamp_table.timestamptz_col) |",
            "+---------------------------------------------+-----------------------------------------------+",
            "| Timestamp(Nanosecond, None)                 | Timestamp(Nanosecond, Some(\"UTC\"))            |",
            "+---------------------------------------------+-----------------------------------------------+",
        ];
        assert_batches_eq!(&expected, &result);
        Ok(())
    }

    #[tokio::test]
    async fn test_coercion_timestamptz() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_batch("timestamp_table", timestamp_table())?;
        for timezone_type in [
            "timestamptz",
            "timestamp_with_timezone",
            "timestamp_with_time_zone",
        ] {
            let manifest = ManifestBuilder::new()
                .catalog("wren")
                .schema("test")
                .model(
                    ModelBuilder::new("timestamp_table")
                        .table_reference("datafusion.public.timestamp_table")
                        .column(ColumnBuilder::new("timestamp_col", "timestamp").build())
                        .column(
                            ColumnBuilder::new("timestamptz_col", timezone_type).build(),
                        )
                        .build(),
                )
                .build();
            let analyzed_mdl = Arc::new(AnalyzedWrenMDL::analyze(manifest)?);
            let sql = r#"select timestamp_col = timestamptz_col from wren.test.timestamp_table"#;
            let actual = transform_sql_with_ctx(
                &SessionContext::new(),
                Arc::clone(&analyzed_mdl),
                &[],
                sql,
            )
            .await?;
            assert_eq!(actual,
                       "SELECT CAST(timestamp_table.timestamp_col AS TIMESTAMP WITH TIME ZONE) = timestamp_table.timestamptz_col \
                       FROM (SELECT timestamp_table.timestamp_col, timestamp_table.timestamptz_col FROM \
                       (SELECT __source.timestamp_col AS timestamp_col, __source.timestamptz_col AS timestamptz_col \
                       FROM datafusion.public.timestamp_table AS __source) AS timestamp_table) AS timestamp_table");

            let sql = r#"select timestamptz_col > cast('2011-01-01 18:00:00' as TIMESTAMP WITH TIME ZONE) from wren.test.timestamp_table"#;
            let actual = transform_sql_with_ctx(
                &SessionContext::new(),
                Arc::clone(&analyzed_mdl),
                &[],
                sql,
            )
            .await?;
            // assert the simplified literal will be casted to the timestamp tz
            assert_eq!(actual,
              "SELECT timestamp_table.timestamptz_col > CAST(CAST('2011-01-01 18:00:00' AS TIMESTAMP) AS TIMESTAMP WITH TIME ZONE) FROM (SELECT timestamp_table.timestamptz_col FROM (SELECT __source.timestamptz_col AS timestamptz_col FROM datafusion.public.timestamp_table AS __source) AS timestamp_table) AS timestamp_table"
);

            let sql = r#"select timestamptz_col > '2011-01-01 18:00:00' from wren.test.timestamp_table"#;
            let actual = transform_sql_with_ctx(
                &SessionContext::new(),
                Arc::clone(&analyzed_mdl),
                &[],
                sql,
            )
            .await?;
            // assert the string literal will be casted to the timestamp tz
            assert_eq!(actual,
                       "SELECT timestamp_table.timestamptz_col > CAST('2011-01-01 18:00:00' AS TIMESTAMP WITH TIME ZONE) \
                       FROM (SELECT timestamp_table.timestamptz_col FROM (SELECT __source.timestamptz_col AS timestamptz_col \
                       FROM datafusion.public.timestamp_table AS __source) AS timestamp_table) AS timestamp_table");

            let sql = r#"select timestamp_col > cast('2011-01-01 18:00:00' as TIMESTAMP WITH TIME ZONE) from wren.test.timestamp_table"#;
            let actual = transform_sql_with_ctx(
                &SessionContext::new(),
                Arc::clone(&analyzed_mdl),
                &[],
                sql,
            )
            .await?;
            // assert the simplified literal won't be casted to the timestamp tz
            assert_eq!(actual,
                "SELECT timestamp_table.timestamp_col > CAST('2011-01-01 18:00:00' AS TIMESTAMP) \
                FROM (SELECT timestamp_table.timestamp_col FROM (SELECT __source.timestamp_col AS timestamp_col \
                FROM datafusion.public.timestamp_table AS __source) AS timestamp_table) AS timestamp_table");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_list() -> Result<()> {
        let ctx = SessionContext::new();
        let manifest = ManifestBuilder::new()
            .catalog("wren")
            .schema("test")
            .model(
                ModelBuilder::new("list_table")
                    .table_reference("list_table")
                    .column(ColumnBuilder::new("list_col", "array<int>").build())
                    .build(),
            )
            .build();
        let analyzed_mdl = Arc::new(AnalyzedWrenMDL::analyze(manifest)?);
        let sql = "select list_col[1] from wren.test.list_table";
        let actual =
            transform_sql_with_ctx(&ctx, Arc::clone(&analyzed_mdl), &[], sql).await?;
        assert_eq!(actual, "SELECT list_table.list_col[1] FROM (SELECT list_table.list_col FROM \
        (SELECT __source.list_col AS list_col FROM list_table AS __source) AS list_table) AS list_table");
        Ok(())
    }

    #[tokio::test]
    async fn test_struct() -> Result<()> {
        let ctx = SessionContext::new();
        let manifest = ManifestBuilder::new()
            .catalog("wren")
            .schema("test")
            .model(
                ModelBuilder::new("struct_table")
                    .table_reference("struct_table")
                    .column(
                        ColumnBuilder::new(
                            "struct_col",
                            "struct<float_field float,time_field timestamp>",
                        )
                        .build(),
                    )
                    .column(
                        ColumnBuilder::new(
                            "struct_array_col",
                            "array<struct<float_field float,time_field timestamp>>",
                        )
                        .build(),
                    )
                    .build(),
            )
            .build();
        let analyzed_mdl = Arc::new(AnalyzedWrenMDL::analyze(manifest)?);
        let sql = "select struct_col.float_field from wren.test.struct_table";
        let actual =
            transform_sql_with_ctx(&ctx, Arc::clone(&analyzed_mdl), &[], sql).await?;
        assert_eq!(
            actual,
            "SELECT struct_table.struct_col.float_field FROM \
        (SELECT struct_table.struct_col FROM (SELECT __source.struct_col AS struct_col \
        FROM struct_table AS __source) AS struct_table) AS struct_table"
        );

        let sql = "select struct_array_col[1].float_field from wren.test.struct_table";
        let actual =
            transform_sql_with_ctx(&ctx, Arc::clone(&analyzed_mdl), &[], sql).await?;
        assert_eq!(actual, "SELECT struct_table.struct_array_col[1].float_field FROM \
        (SELECT struct_table.struct_array_col FROM (SELECT __source.struct_array_col AS struct_array_col \
        FROM struct_table AS __source) AS struct_table) AS struct_table");

        let sql =
            "select {float_field: 1.0, time_field: timestamp '2021-01-01 00:00:00'}";
        let actual =
            transform_sql_with_ctx(&ctx, Arc::clone(&analyzed_mdl), &[], sql).await?;
        assert_eq!(actual, "SELECT {float_field: 1.0, time_field: CAST('2021-01-01 00:00:00' AS TIMESTAMP)}");

        let manifest = ManifestBuilder::new()
            .catalog("wren")
            .schema("test")
            .model(
                ModelBuilder::new("struct_table")
                    .table_reference("struct_table")
                    .column(ColumnBuilder::new("struct_col", "struct<>").build())
                    .build(),
            )
            .build();
        let analyzed_mdl = Arc::new(AnalyzedWrenMDL::analyze(manifest)?);
        let sql = "select struct_col.float_field from wren.test.struct_table";
        let _ = transform_sql_with_ctx(&ctx, Arc::clone(&analyzed_mdl), &[], sql)
            .await
            .map_err(|e| {
                assert_eq!(
                    e.to_string(),
                    "Error during planning: struct must have at least one field"
                )
            });
        Ok(())
    }

    #[tokio::test]
    async fn test_disable_common_expression_eliminate() -> Result<()> {
        let ctx = SessionContext::new();
        let sql =
            "SELECT CAST(TIMESTAMP '2021-01-01 00:00:00' as TIMESTAMP WITH TIME ZONE) = \
        CAST(TIMESTAMP '2021-01-01 00:00:00' as TIMESTAMP WITH TIME ZONE)";
        let result =
            transform_sql_with_ctx(&ctx, Arc::new(AnalyzedWrenMDL::default()), &[], sql)
                .await?;
        assert_eq!(result, "SELECT CAST(CAST('2021-01-01 00:00:00' AS TIMESTAMP) AS TIMESTAMP WITH TIME ZONE) = \
        CAST(CAST('2021-01-01 00:00:00' AS TIMESTAMP) AS TIMESTAMP WITH TIME ZONE)");
        Ok(())
    }

    #[tokio::test]
    async fn test_disable_eliminate_nested_union() -> Result<()> {
        let ctx = SessionContext::new();
        let sql = r#"SELECT * FROM (SELECT 1 x, 'a' y UNION ALL
    SELECT 1 x, 'b' y UNION ALL
    SELECT 2 x, 'a' y UNION ALL
    SELECT 2 x, 'c' y)"#;
        let result =
            transform_sql_with_ctx(&ctx, Arc::new(AnalyzedWrenMDL::default()), &[], sql)
                .await?;
        assert_eq!(
            result,
            "SELECT x, y FROM (SELECT 1 AS x, 'a' AS y \
        UNION ALL SELECT 1 AS x, 'b' AS y \
        UNION ALL SELECT 2 AS x, 'a' AS y \
        UNION ALL SELECT 2 AS x, 'c' AS y)"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_dialect_specific_function_rewrite() -> Result<()> {
        let manifest = ManifestBuilder::default().data_source(MySQL).build();
        let mdl = Arc::new(AnalyzedWrenMDL::analyze(manifest)?);
        let ctx = SessionContext::new();
        let expected = "SELECT trim(' abc')";
        let actual =
            transform_sql_with_ctx(&ctx, Arc::clone(&mdl), &[], expected).await?;
        assert_eq!(actual, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_disable_single_distinct_to_group_by() -> Result<()> {
        let ctx = SessionContext::new();
        let manifest = ManifestBuilder::new()
            .catalog("wren")
            .schema("test")
            .model(
                ModelBuilder::new("customer")
                    .table_reference("customer")
                    .column(ColumnBuilder::new("c_custkey", "int").build())
                    .column(ColumnBuilder::new("c_name", "string").build())
                    .build(),
            )
            .build();
        let sql = r#"SELECT c_custkey, count(distinct c_name) FROM customer GROUP BY c_custkey"#;
        let analyzed_mdl = Arc::new(AnalyzedWrenMDL::analyze(manifest)?);
        let result =
            transform_sql_with_ctx(&ctx, Arc::clone(&analyzed_mdl), &[], sql).await?;
        assert_eq!(
            result,
            "SELECT customer.c_custkey, count(DISTINCT customer.c_name) FROM \
            (SELECT customer.c_custkey, customer.c_name FROM \
            (SELECT __source.c_custkey AS c_custkey, __source.c_name AS c_name FROM customer AS __source) AS customer) AS customer \
            GROUP BY customer.c_custkey"
        );
        Ok(())
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

    fn artist() -> RecordBatch {
        let name: ArrayRef =
            Arc::new(StringArray::from_iter_values(["Ina", "Azki", "Kaela"]));
        let group: ArrayRef = Arc::new(StringArray::from_iter_values(["EN", "JP", "ID"]));
        let subscribe: ArrayRef = Arc::new(Int64Array::from(vec![100, 200, 300]));
        let debut_time: ArrayRef =
            Arc::new(TimestampNanosecondArray::from(vec![1, 2, 3]));
        RecordBatch::try_from_iter(vec![
            ("名字", name),
            ("組別", group),
            ("訂閱數", subscribe),
            ("出道時間", debut_time),
        ])
        .unwrap()
    }

    fn timestamp_table() -> RecordBatch {
        let timestamp: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![1, 2, 3]));
        let timestamptz: ArrayRef =
            Arc::new(TimestampNanosecondArray::from(vec![1, 2, 3]).with_timezone("UTC"));
        RecordBatch::try_from_iter(vec![
            ("timestamp_col", timestamp),
            ("timestamptz_col", timestamptz),
        ])
        .unwrap()
    }
}
