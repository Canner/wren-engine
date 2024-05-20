pub mod manifest;

use std::sync::Arc;

use datafusion::{config::ConfigOptions, error::DataFusionError, optimizer::analyzer::Analyzer, sql::{planner::SqlToRel, sqlparser::{dialect::GenericDialect, parser::Parser}, unparser::plan_to_sql}};

use crate::{logical_plan::{rule::{ModelAnalyzeRule, ModelGenerationRule}, wren_context_provider::WrenContextProvider}, mdl::manifest::{Manifest, Model}};


// This is the main struct that holds the manifest and provides methods to access the models
pub struct WrenMDL {
    pub manifest: Manifest,
}

impl WrenMDL {
    pub fn new(manifest: Manifest) -> Self {
        WrenMDL { manifest }
    }

    pub fn new_ref(manifest: Manifest) -> Arc<Self> {
        Arc::new(WrenMDL::new(manifest))
    }

    pub fn get_model(&self, name: &str) -> Option<Arc<Model>> {
        self.manifest
            .models
            .iter()
            .find(|model| model.name == name)
            .map(|model| Arc::clone(model))
    }
}

pub fn transform_sql(wren_mdl: Arc<WrenMDL>, sql: &str) -> Result<String, DataFusionError>{
    println!("SQL: {}", sql);
    println!("********");

    // parse the SQL
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    let statement = &ast[0];

    // create a logical query plan
    let context_provider = WrenContextProvider::new(&wren_mdl);
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
        Arc::new(ModelAnalyzeRule::new(Arc::clone(&wren_mdl))),
        Arc::new(ModelGenerationRule::new(Arc::clone(&wren_mdl))),
    ]);

    let config = ConfigOptions::default();

    let analyzed = analyzer
        .execute_and_check(&plan, &config, |_, _| {})
        .unwrap();
    println!("Do some modeling:\n {analyzed:?}");
    println!("********");

    // show the planned sql
    match plan_to_sql(&analyzed) {
        Ok(sql) => {
            Ok(sql.to_string())
        }
        Err(e) => {
            Err(e)
        }
    }
}

#[cfg(test)]
mod test {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use crate::mdl::manifest::Manifest;
    use crate::mdl::{self, WrenMDL};

    #[test]
    fn test_projection_scan() {
        let test_data: PathBuf = [env!("CARGO_MANIFEST_DIR"), "tests", "data", "mdl.json"]
        .iter()
        .collect();
        let mdl_json = fs::read_to_string(test_data.as_path()).unwrap();
        let mdl = serde_json::from_str::<Manifest>(&mdl_json).unwrap();
        let wren_mdl = Arc::new(WrenMDL::new(mdl));

        let sql: &str = "select orderkey from orders";
        let planned = mdl::transform_sql(Arc::clone(&wren_mdl), sql).unwrap();
        assert_eq!(planned, r#"SELECT "orders"."orderkey" FROM (SELECT "o_orderkey" AS "orderkey", "o_custkey" AS "custkey", "o_totalprice" AS "totalprice" FROM "orders") AS "orders""#);
    }

    #[test]
    fn test_projection_filter_scan() {
        let test_data: PathBuf = [env!("CARGO_MANIFEST_DIR"), "tests", "data", "mdl.json"]
        .iter()
        .collect();
        let mdl_json = fs::read_to_string(test_data.as_path()).unwrap();
        let mdl = serde_json::from_str::<Manifest>(&mdl_json).unwrap();
        let wren_mdl = Arc::new(WrenMDL::new(mdl));

        let sql: &str = "select orderkey from orders where orders.totalprice > 10";
        let planned = mdl::transform_sql(Arc::clone(&wren_mdl), sql).unwrap();
        assert_eq!(planned, r#"SELECT "orders"."orderkey" FROM (SELECT "o_orderkey" AS "orderkey", "o_custkey" AS "custkey", "o_totalprice" AS "totalprice" FROM "orders") AS "orders" WHERE ("orders"."totalprice" > 10)"#);
    }
}
