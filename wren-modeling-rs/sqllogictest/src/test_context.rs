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

use crate::engine::utils::read_dir_recursive;

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
                Some(register_ecommerce_table(&ctx).await.ok()?)
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
            ModelBuilder::new("customers")
                .table_reference("datafusion.public.customers")
                .column(ColumnBuilder::new("city", "varchar").build())
                .column(ColumnBuilder::new("id", "varchar").build())
                .column(ColumnBuilder::new("state", "varchar").build())
                .column(
                    ColumnBuilder::new_calculated("city_state", "varchar")
                        .expression("city || ' ' || state")
                        .build(),
                )
                .primary_key("id")
                .build(),
        )
        .model(
            ModelBuilder::new("order_items")
                .table_reference("datafusion.public.order_items")
                .column(ColumnBuilder::new("freight_value", "double").build())
                .column(ColumnBuilder::new("id", "bigint").build())
                .column(ColumnBuilder::new("item_number", "bigint").build())
                .column(ColumnBuilder::new("order_id", "varchar").build())
                .column(ColumnBuilder::new("price", "double").build())
                .column(ColumnBuilder::new("product_id", "varchar").build())
                .column(ColumnBuilder::new("shipping_limit_date", "varchar").build())
                .column(
                    ColumnBuilder::new_relationship(
                        "orders",
                        "orders",
                        "orders_order_items",
                    )
                    .build(),
                )
                .column(
                    ColumnBuilder::new_calculated("customer_state", "varchar")
                        .expression("orders.customers.state")
                        .build(),
                )
                .primary_key("id")
                .build(),
        )
        .model(
            ModelBuilder::new("orders")
                .table_reference("datafusion.public.orders")
                .column(ColumnBuilder::new("approved_timestamp", "varchar").build())
                .column(ColumnBuilder::new("customer_id", "varchar").build())
                .column(ColumnBuilder::new("delivered_carrier_date", "varchar").build())
                .column(ColumnBuilder::new("estimated_delivery_date", "varchar").build())
                .column(ColumnBuilder::new("order_id", "varchar").build())
                .column(ColumnBuilder::new("purchase_timestamp", "varchar").build())
                .column(
                    ColumnBuilder::new_relationship(
                        "customers",
                        "customers",
                        "orders_customer",
                    )
                    .build(),
                )
                .column(
                    ColumnBuilder::new_calculated("customer_state", "varchar")
                        .expression("customers.state")
                        .build(),
                )
                .column(
                    ColumnBuilder::new_calculated("customer_state_order_id", "varchar")
                        .expression("customers.state || ' ' || order_id")
                        .build(),
                )
                .column(
                    ColumnBuilder::new_relationship(
                        "order_items",
                        "order_items",
                        "orders_order_items",
                    )
                    .build(),
                )
                .column(
                    ColumnBuilder::new_calculated("totalprice", "double")
                        .expression("sum(order_items.price)")
                        .build(),
                )
                .primary_key("order_id")
                .build(),
        )
        .relationship(
            RelationshipBuilder::new("orders_customer")
                .model("orders")
                .model("customers")
                .join_type(JoinType::ManyToOne)
                .condition("orders.customer_id = customers.id")
                .build(),
        )
        .relationship(
            RelationshipBuilder::new("orders_order_items")
                .model("orders")
                .model("order_items")
                .join_type(JoinType::ManyToOne)
                .condition("orders.order_id = order_items.order_id")
                .build(),
        )
        .view(ViewBuilder::new("orders_view")
            .statement("select * from wrenai.public.orders")
            .build())
        // TODO: support expression without alias inside view
        // .view(ViewBuilder::new("revenue_orders").statement("select order_id, sum(price) from wrenai.public.order_items group by order_id").build())
        .view(ViewBuilder::new("revenue_orders").statement("select order_id, sum(price) as totalprice from wrenai.public.order_items group by order_id").build())
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
    // let new_state = ctx
    //     .state()
    //     .add_analyzer_rule(Arc::new(ModelAnalyzeRule::
    //
    // new(Arc::clone(&analyzed_mdl))))
    //     .add_analyzer_rule(Arc::new(ModelGenerationRule::new(Arc::clone(
    //         &analyzed_mdl,
    //     ))))
    //     // TODO: disable optimize_projections rule
    //     // There are some conflict with the optimize rule, [datafusion::optimizer::optimize_projections::OptimizeProjections]
    //     .with_optimizer_rules(vec![]);
    // let ctx = SessionContext::new_with_state(new_state);
    Ok((ctx.to_owned(), analyzed_mdl))
}
