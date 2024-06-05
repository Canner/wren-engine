use std::{collections::HashMap, sync::Arc};

use datafusion::{
    config::ConfigOptions,
    error::DataFusionError,
    optimizer::analyzer::Analyzer,
    sql::{
        planner::SqlToRel,
        sqlparser::{dialect::GenericDialect, parser::Parser},
        unparser::plan_to_sql,
    },
};

use manifest::Relationship;

use crate::{
    logical_plan::{
        context_provider::WrenContextProvider,
        rule::{ModelAnalyzeRule, ModelGenerationRule},
    },
    mdl::manifest::{Column, Manifest, Metric, Model},
};

pub mod lineage;
pub mod manifest;
pub mod utils;

pub struct AnalyzedWrenMDL {
    pub wren_mdl: Arc<WrenMDL>,
    pub lineage: lineage::Lineage,
}

impl AnalyzedWrenMDL {
    pub fn analyze(manifest: Manifest) -> Self {
        let wren_mdl = Arc::new(WrenMDL::new(manifest));
        let lineage = lineage::Lineage::new(&wren_mdl);
        AnalyzedWrenMDL { wren_mdl, lineage }
    }
}

// This is the main struct that holds the manifest and provides methods to access the models
pub struct WrenMDL {
    pub manifest: Manifest,
    pub qualified_references: HashMap<String, ColumnReference>,
}

impl WrenMDL {
    pub fn new(manifest: Manifest) -> Self {
        let mut qualifed_references = HashMap::new();
        manifest.models.iter().for_each(|model| {
            model.columns.iter().for_each(|column| {
                qualifed_references.insert(
                    format!("{}.{}", model.name, column.name),
                    ColumnReference::new(Dataset::Model(Arc::clone(model)), Arc::clone(column)),
                );
            });
        });
        manifest.metrics.iter().for_each(|metric| {
            metric.dimension.iter().for_each(|dimension| {
                qualifed_references.insert(
                    format!("{}.{}", metric.name, dimension.name),
                    ColumnReference::new(
                        Dataset::Metric(Arc::clone(metric)),
                        Arc::clone(dimension),
                    ),
                );
            });
            metric.measure.iter().for_each(|measure| {
                qualifed_references.insert(
                    format!("{}.{}", metric.name, measure.name),
                    ColumnReference::new(Dataset::Metric(Arc::clone(metric)), Arc::clone(measure)),
                );
            });
        });

        WrenMDL {
            manifest,
            qualified_references: qualifed_references,
        }
    }

    pub fn new_ref(manifest: Manifest) -> Arc<Self> {
        Arc::new(WrenMDL::new(manifest))
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

    pub fn get_column_reference(&self, dataset: &str, column: &str) -> ColumnReference {
        let name = format!("{}.{}", dataset, column);
        self.qualified_references
            .get(&name)
            .unwrap_or_else(|| panic!("column {} not found", name))
            .clone()
    }
}
/// Transform the SQL based on the MDL
pub fn transform_sql(
    analyzed_mdl: Arc<AnalyzedWrenMDL>,
    sql: &str,
) -> Result<String, DataFusionError> {
    println!("SQL: {}", sql);
    println!("********");

    // parse the SQL
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    let statement = &ast[0];

    // create a logical query plan
    let context_provider = WrenContextProvider::new(&analyzed_mdl.wren_mdl);
    let sql_to_rel = SqlToRel::new(&context_provider);
    let plan = match sql_to_rel.sql_statement_to_plan(statement.clone()) {
        Ok(plan) => plan,
        Err(e) => {
            println!("Error: {:?}", e);
            return Err(e);
        }
    };
    println!("Original LogicalPlan:\n {plan:?}");
    println!("********");

    let analyzer = Analyzer::with_rules(vec![
        Arc::new(ModelAnalyzeRule::new(Arc::clone(&analyzed_mdl))),
        Arc::new(ModelGenerationRule::new(Arc::clone(&analyzed_mdl))),
    ]);

    let config = ConfigOptions::default();

    let analyzed = match analyzer.execute_and_check(plan, &config, |_, _| {}) {
        Ok(analyzed) => analyzed,
        Err(e) => {
            println!("Error: {:?}", e);
            return Err(e);
        }
    };
    println!("Do some modeling:\n {analyzed:?}");
    println!("********");

    // show the planned sql
    match plan_to_sql(&analyzed) {
        Ok(sql) => Ok(sql.to_string()),
        Err(e) => Err(e),
    }
}

/// Analyze the decision point. It's same as the /v1/analysis/sql API in wren engine
pub fn decision_point_analyze(_wren_mdl: Arc<WrenMDL>, _sql: &str) {}

/// Cheap clone of the ColumnReference
#[derive(Clone, Debug)]
pub struct ColumnReference {
    pub dataset: Dataset,
    pub column: Arc<Column>,
}

impl ColumnReference {
    fn new(dataset: Dataset, column: Arc<Column>) -> Self {
        ColumnReference { dataset, column }
    }

    pub fn get_column(&self) -> Arc<Column> {
        Arc::clone(&self.column)
    }

    pub fn get_qualified_name(&self) -> String {
        match &self.dataset {
            Dataset::Model(model) => format!("{}.{}", model.name, self.column.name),
            Dataset::Metric(metric) => format!("{}.{}", metric.name, self.column.name),
        }
    }
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub enum Dataset {
    Model(Arc<Model>),
    Metric(Arc<Metric>),
}

impl Dataset {
    pub fn get_name(&self) -> String {
        match self {
            Dataset::Model(model) => model.name.clone(),
            Dataset::Metric(metric) => metric.name.clone(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::error::Error;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;

    use datafusion::error::Result;
    use datafusion::sql::planner::SqlToRel;
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;
    use datafusion::sql::unparser::plan_to_sql;

    use crate::logical_plan::context_provider::RemoteContextProvider;
    use crate::mdl::manifest::Manifest;
    use crate::mdl::{self, AnalyzedWrenMDL};

    #[test]
    fn test_access_model() -> Result<(), Box<dyn Error>> {
        let test_data: PathBuf = [env!("CARGO_MANIFEST_DIR"), "tests", "data", "mdl.json"]
            .iter()
            .collect();
        let mdl_json = fs::read_to_string(test_data.as_path())?;
        let mdl = serde_json::from_str::<Manifest>(&mdl_json)?;
        let analyzed_mdl = Arc::new(AnalyzedWrenMDL::analyze(mdl));

        // TODO: instead of assert string value, assert the query plan or result
        let tests: Vec<(&str, &str)> = vec![
            (
                "select orderkey + orderkey from orders",
                r#"SELECT ("orders"."orderkey" + "orders"."orderkey") FROM (SELECT "orders"."o_orderkey" AS "orderkey" FROM "orders") AS "orders""#,
            ),
            (
                "select orderkey from orders where orders.totalprice > 10",
                r#"SELECT "orders"."orderkey" FROM (SELECT "o_orderkey" AS "orderkey", "o_totalprice" AS "totalprice" FROM "orders") AS "orders" WHERE ("orders"."totalprice" > 10)"#,
            ),
            (
                "select orders.orderkey from orders left join customer on (orders.custkey = customer.custkey) where orders.totalprice > 10",
                r#"SELECT "orders"."orderkey" FROM (SELECT "orders"."o_custkey" AS "custkey", "orders"."o_orderkey" AS "orderkey", "orders"."o_totalprice" AS "totalprice" FROM "orders") AS "orders" LEFT JOIN (SELECT "customer"."c_orderkey" AS "custkey" FROM "customer") AS "customer" ON ("orders"."custkey" = "customer"."custkey") WHERE ("orders"."totalprice" > 10)"#,
            ),
            (
                "select orderkey, sum(totalprice) from orders group by 1",
                r#"SELECT "orders"."orderkey", SUM("orders"."totalprice") FROM (SELECT "o_orderkey" AS "orderkey", "o_totalprice" AS "totalprice" FROM "orders") AS "orders" GROUP BY "orders"."orderkey""#,
            ),
            (
                "select orderkey, count(*) from orders where orders.totalprice > 10 group by 1",
                r#"SELECT "orders"."orderkey", COUNT(*) FROM (SELECT "o_orderkey" AS "orderkey", "o_totalprice" AS "totalprice" FROM "orders") AS "orders" WHERE ("orders"."totalprice" > 10) GROUP BY "orders"."orderkey""#,
            ),
            (
                "select count(*) from orders",
                r#"SELECT COUNT(*) FROM "orders""#,
            ),
            (
                "select customer_name from orders",
                r#"SELECT "orders"."customer_name" FROM (SELECT "customer"."name" AS "customer_name" FROM (SELECT "customer"."c_name" AS "name", "customer"."c_orderkey" AS "custkey" FROM "customer") AS "customer" LEFT JOIN (SELECT "orders"."o_custkey" AS "custkey" FROM "orders") AS "orders" ON ("customer"."custkey" = "orders"."custkey")) AS "orders""#
            ),
            // TODO: support calculated witout relationship
            // (
            //     "select orderkey_plus_custkey from orders",
            //     "select * from orders;"
            // )
        ];

        for (sql, expected) in tests {
            println!("{}", sql);
            let actual = mdl::transform_sql(Arc::clone(&analyzed_mdl), sql)?;
            assert_eq!(
                plan_sql(&actual, Arc::clone(&analyzed_mdl))?,
                plan_sql(expected, Arc::clone(&analyzed_mdl))?
            );
        }

        Ok(())
    }

    fn plan_sql(sql: &str, analyzed_mdl: Arc<AnalyzedWrenMDL>) -> Result<String> {
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql).unwrap();
        let statement = &ast[0];

        let context_provider = RemoteContextProvider::new(&analyzed_mdl.wren_mdl);
        let sql_to_rel = SqlToRel::new(&context_provider);
        let rels = sql_to_rel.sql_statement_to_plan(statement.clone())?;
        // show the planned sql
        match plan_to_sql(&rels) {
            Ok(sql) => Ok(sql.to_string()),
            Err(e) => Err(e),
        }
    }
}
