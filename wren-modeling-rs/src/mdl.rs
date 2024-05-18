pub mod manifest;

use std::sync::Arc;

use datafusion::{config::ConfigOptions, error::DataFusionError, optimizer::analyzer::Analyzer, sql::{planner::SqlToRel, sqlparser::{dialect::GenericDialect, parser::Parser}, unparser::plan_to_sql}};

use crate::{logical_plan::{rule::{ModelAnalyzeRule, ModelGenerationRule}, wren_context_provider::WrenContextProvider}, mdl::manifest::{Manifest, Model}};

pub struct WrenMDL {
    pub manifest: Manifest,
}

impl WrenMDL {
    pub fn new(manifest: Manifest) -> Self {
        WrenMDL { manifest }
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
