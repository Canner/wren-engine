// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::engine::utils::read_dir_recursive;
use datafusion::error::Result;
use datafusion::prelude::SessionConfig;
use datafusion::prelude::{CsvReadOptions, SessionContext};
use log::info;
use tempfile::TempDir;
use wren_core::mdl::builder::{
    ColumnBuilder, ManifestBuilder, ModelBuilder, RelationshipBuilder, ViewBuilder,
};
use wren_core::mdl::manifest::JoinType;
use wren_core::mdl::AnalyzedWrenMDL;

const TEST_RESOURCES: &str = "tests/resources";

/// Context for running tests
pub struct TestContext {
    /// Context for running queries
    ctx: SessionContext,
    analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
    /// Temporary directory created and cleared at the end of the test
    test_dir: Option<TempDir>,
}

impl TestContext {
    pub fn new(ctx: SessionContext, analyzed_wren_mdl: Arc<AnalyzedWrenMDL>) -> Self {
        Self {
            ctx,
            analyzed_wren_mdl,
            test_dir: None,
        }
    }

    /// Create a SessionContext, configured for the specific sqllogictest
    /// test(.slt file) , if possible.
    ///
    /// If `None` is returned (e.g. because some needed feature is not
    /// enabled), the file should be skipped
    pub async fn try_new_for_test_file(relative_path: &Path) -> Option<Self> {
        let config = SessionConfig::new()
            // hardcode target partitions so plans are deterministic
            .with_target_partitions(4);

        let ctx = SessionContext::new_with_config(config);

        let file_name = relative_path.file_name().unwrap().to_str().unwrap();
        match file_name {
            "view.slt" | "model.slt" => {
                info!("Registering local temporary table");
                Some(register_ecommerce_table(&ctx).await.unwrap())
            }
            _ => {
                info!("Using default SessionContext");
                None
            }
        }
    }

    /// Enables the test directory feature. If not enabled,
    /// calling `testdir_path` will result in a panic.
    pub fn enable_testdir(&mut self) {
        if self.test_dir.is_none() {
            self.test_dir = Some(TempDir::new().expect("failed to create testdir"));
        }
    }

    /// Returns the path to the test directory. Panics if the test
    /// directory feature is not enabled via `enable_testdir`.
    pub fn testdir_path(&self) -> &Path {
        self.test_dir.as_ref().expect("testdir not enabled").path()
    }

    /// Returns a reference to the internal SessionContext
    pub fn session_ctx(&self) -> &SessionContext {
        &self.ctx
    }

    pub fn analyzed_wren_mdl(&self) -> &Arc<AnalyzedWrenMDL> {
        &self.analyzed_wren_mdl
    }
}

pub async fn register_ecommerce_table(ctx: &SessionContext) -> Result<TestContext> {
    let path = PathBuf::from(TEST_RESOURCES).join("ecommerce");
    let data = read_dir_recursive(&path).unwrap();

    // register csv file with the execution context
    for file in data.iter() {
        let table_name = file.file_stem().unwrap().to_str().unwrap();
        ctx.register_csv(table_name, file.to_str().unwrap(), CsvReadOptions::new())
            .await
            .unwrap();
    }
    let (ctx, mdl) = register_ecommerce_mdl(ctx).await?;
    Ok(TestContext::new(ctx, mdl))
}

async fn register_ecommerce_mdl(
    ctx: &SessionContext,
) -> Result<(SessionContext, Arc<AnalyzedWrenMDL>)> {
    let manifest = ManifestBuilder::new()
        .model(
            ModelBuilder::new("Customers")
                .table_reference("datafusion.public.customers")
                .column(ColumnBuilder::new("City", "varchar").expression("city").build())
                .column(ColumnBuilder::new("Id", "varchar").expression("id").build())
                .column(ColumnBuilder::new("State", "varchar").expression("state").build())
                // .column(
                //     ColumnBuilder::new_calculated("City_state", "varchar")
                //         .expression(r#""City" || ' ' || "State""#)
                //         .build(),
                // )
                .primary_key("Id")
                .build(),
        )
        .model(
            ModelBuilder::new("Order_items")
                .table_reference("datafusion.public.order_items")
                .column(ColumnBuilder::new("Freight_value", "double").expression("freight_value").build())
                .column(ColumnBuilder::new("Id", "bigint").expression("id").build())
                .column(ColumnBuilder::new("Item_number", "bigint").expression("item_number").build())
                .column(ColumnBuilder::new("Order_id", "varchar").expression("order_id").build())
                .column(ColumnBuilder::new("Price", "double").expression("price").build())
                .column(ColumnBuilder::new("Product_id", "varchar").expression("product_id").build())
                .column(ColumnBuilder::new("Shipping_limit_date", "varchar").expression("shipping_limit_date").build())
                .column(
                    ColumnBuilder::new_relationship(
                        "Orders",
                        "Orders",
                        "Orders_order_items",
                    )
                    .build(),
                )
                .column(
                    ColumnBuilder::new_calculated("Customer_state", "varchar")
                        .expression(r#""Orders"."Customers"."State""#)
                        .build(),
                )
                // TODO: it cause a stack overflow issue.
                // .column(
                //     ColumnBuilder::new_calculated("Customer_state_cf", "varchar")
                //         .expression(r#""Orders"."Customer_state""#)
                //         .build(),
                // )
                // TODO: duplicate `orders.customer_state`
                // .column(
                //     ColumnBuilder::new_calculated("Customer_state_cf_concat", "varchar")
                //         .expression(r#""Orders"."Customer_state" || '-test'"#)
                //         .build(),
                // )
                // TODO: allow multiple calculation in an expression
                // .column(
                //     ColumnBuilder::new("Customer_location", "varchar")
                //         .calculated(true)
                //         .expression(r#""Orders"."Customer_state" || '-' || "Orders."Customer_city""#)
                //         .build(),
                // )
                // .column(
                //     ColumnBuilder::new("Customer_location", "varchar")
                //         .calculated(true)
                //         .expression(r#""Orders"."Customers"."State" || '-' || "Orders"."Customers"."City""#)
                //         .build(),
                // )
                .primary_key("Id")
                .build(),
        )
        .model(
            ModelBuilder::new("Orders")
                .table_reference("datafusion.public.orders")
                .column(ColumnBuilder::new("Approved_timestamp", "varchar").expression("approved_timestamp").build())
                .column(ColumnBuilder::new("Customer_id", "varchar").expression("customer_id").build())
                .column(ColumnBuilder::new("Delivered_carrier_date", "varchar").expression("delivered_carrier_date").build())
                .column(ColumnBuilder::new("Estimated_delivery_date", "varchar").expression("estimated_delivery_date").build())
                .column(ColumnBuilder::new("Order_id", "varchar").expression("order_id").build())
                .column(ColumnBuilder::new("Purchase_timestamp", "varchar").expression("purchase_timestamp").build())
                .column(
                    ColumnBuilder::new_relationship(
                        "Customers",
                        "Customers",
                        "Orders_customer",
                    )
                    .build(),
                )
                .column(
                    ColumnBuilder::new_calculated("Customer_state", "varchar")
                        .expression(r#""Customers"."State""#)
                        .build(),
                )
                // TODO: fix calcaultion with non-relationship column
                // .column(
                //     ColumnBuilder::new_calculated("Customer_state_order_id", "varchar")
                //         .expression(r#""Customers"."State" || ' ' || "Order_id""#)
                //         .build(),
                // )
                .column(
                    ColumnBuilder::new_relationship(
                        "Order_items",
                        "Order_items",
                        "Orders_order_items",
                    )
                    .build(),
                )
                .column(
                    ColumnBuilder::new_calculated("Totalprice", "double")
                        .expression(r#"sum("Order_items"."Price")"#)
                        .build(),
                )
                .column(
                    ColumnBuilder::new_calculated("Customer_city", "varchar")
                        .expression(r#""Customers"."City""#)
                        .build(),
                )
                .primary_key("Order_id")
                .build(),
        )
        .relationship(
            RelationshipBuilder::new("Orders_customer")
                .model("Orders")
                .model("Customers")
                .join_type(JoinType::ManyToOne)
                .condition(r#""Orders"."Customer_id" = "Customers"."Id""#)
                .build(),
        )
        .relationship(
            RelationshipBuilder::new("Orders_order_items")
                .model("Orders")
                .model("Order_items")
                .join_type(JoinType::ManyToOne)
                .condition(r#""Orders"."Order_id" = "Order_items"."Order_id""#)
                .build(),
        )
        .view(
            ViewBuilder::new("Customer_view")
                .statement(r#"select * from wrenai.public."Customers""#)
                .build(),
        )
        .view(ViewBuilder::new("Revenue_orders").statement(r#"select "Order_id", sum("Price") from wrenai.public."Order_items" group by "Order_id""#).build())
        .view(
            ViewBuilder::new("Revenue_orders_alias")
                .statement(r#"select "Order_id" as "Order_id", sum("Price") as "Totalprice" from wrenai.public."Order_items" group by "Order_id""#)
                .build())
        .build();
    let mut register_tables = HashMap::new();
    register_tables.insert(
        "datafusion.public.orders".to_string(),
        ctx.catalog("datafusion")
            .unwrap()
            .schema("public")
            .unwrap()
            .table("orders")
            .await?
            .unwrap(),
    );
    register_tables.insert(
        "datafusion.public.order_items".to_string(),
        ctx.catalog("datafusion")
            .unwrap()
            .schema("public")
            .unwrap()
            .table("order_items")
            .await?
            .unwrap(),
    );
    register_tables.insert(
        "datafusion.public.customers".to_string(),
        ctx.catalog("datafusion")
            .unwrap()
            .schema("public")
            .unwrap()
            .table("customers")
            .await?
            .unwrap(),
    );
    let analyzed_mdl = Arc::new(AnalyzedWrenMDL::analyze_with_tables(
        manifest,
        register_tables,
    )?);
    // TODO: there're some conflicts for datafusion optimization rules.
    // let ctx = create_ctx_with_mdl(ctx, Arc::clone(&analyzed_mdl)).await?;
    Ok((ctx.to_owned(), analyzed_mdl))
}
