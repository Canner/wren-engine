use datafusion::common::config::ConfigOptions;
use wren_modeling_rs::logical_plan::wren_context_provider::WrenContextProvider;

use datafusion::optimizer::analyzer::Analyzer;
use datafusion::sql::{
    planner::SqlToRel,
    sqlparser::{dialect::GenericDialect, parser::Parser},
    unparser::plan_to_sql,
};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use wren_modeling_rs::logical_plan::rule::{ModelAnalyzeRule, ModelGenerationRule};
use wren_modeling_rs::mdl::manifest::Manifest;
use wren_modeling_rs::mdl::WrenMDL;

fn main() {
    let test_data: PathBuf = [env!("CARGO_MANIFEST_DIR"), "tests", "data", "mdl.json"]
        .iter()
        .collect();
    let mdl_json = fs::read_to_string(test_data.as_path()).unwrap();
    let mdl = serde_json::from_str::<Manifest>(&mdl_json).unwrap();
    let wren_mdl = Arc::new(WrenMDL::new(mdl));

    let sql = "select * from orders";

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
            return;
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
    let planned = match plan_to_sql(&analyzed) {
        Ok(sql) => sql,
        Err(e) => {
            println!("Error: {:?}", e);
            return;
        }
    };

    println!("unparse to SQL:\n {}", planned);
}
