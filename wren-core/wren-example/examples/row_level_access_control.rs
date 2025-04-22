use std::collections::HashMap;
use std::sync::Arc;

use datafusion::prelude::{CsvReadOptions, SessionContext};
use wren_core::mdl::builder::{ColumnBuilder, ManifestBuilder, ModelBuilder};
use wren_core::mdl::manifest::{Manifest, SessionProperty};
use wren_core::mdl::{transform_sql_with_ctx, AnalyzedWrenMDL};

#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    let manifest = init_manifest();
    let ctx = SessionContext::new();

    ctx.register_csv(
        "customers",
        "sqllogictest/tests/resources/ecommerce/customers.csv",
        CsvReadOptions::new(),
    )
    .await?;
    let customers_provider = ctx
        .catalog("datafusion")
        .unwrap()
        .schema("public")
        .unwrap()
        .table("customers")
        .await?
        .unwrap();
    let register = HashMap::from([(
        "datafusion.public.customers".to_string(),
        customers_provider,
    )]);
    let analyzed_mdl =
        Arc::new(AnalyzedWrenMDL::analyze_with_tables(manifest, register)?);
    let sql = "SELECT * FROM customers";

    // carry the seesion property
    let mut properties = HashMap::new();
    properties.insert("session_city".to_string(), Some("'Santa Ana'".to_string()));

    let sql = transform_sql_with_ctx(&ctx, analyzed_mdl, &[], properties, sql).await?;
    println!("Wren engine generated SQL: \n{}", sql);
    let df = match ctx.sql(&sql).await {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error: {}", e);
            return Err(e);
        }
    };
    match df.show().await {
        Ok(_) => {}
        Err(e) => eprintln!("Error: {}", e),
    }

    Ok(())
}

fn init_manifest() -> Manifest {
    ManifestBuilder::new()
        .model(
            ModelBuilder::new("customers")
                .table_reference("datafusion.public.customers")
                .column(ColumnBuilder::new("city", "varchar").build())
                .column(ColumnBuilder::new("id", "varchar").build())
                .column(ColumnBuilder::new("state", "varchar").build())
                .add_row_level_access_control(
                    "city rule",
                    vec![SessionProperty::new_required("session_city")],
                    "city = @session_city",
                )
                .primary_key("id")
                .build(),
        )
        .build()
}
