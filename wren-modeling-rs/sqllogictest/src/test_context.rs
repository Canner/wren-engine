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

use std::any::Any;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion::{
    catalog::{schema::MemorySchemaProvider, CatalogProvider, MemoryCatalogProvider},
    datasource::{TableProvider, TableType},
    prelude::{CsvReadOptions, SessionContext},
};
use log::info;
use tempfile::TempDir;

use wren_core::logical_plan::utils::create_schema;
use wren_core::mdl::builder::{
    ColumnBuilder, ManifestBuilder, ModelBuilder, RelationshipBuilder,
};
use wren_core::mdl::manifest::{JoinType, Model};
use wren_core::mdl::{AnalyzedWrenMDL, WrenMDL};

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
            "model.slt" => {
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

async fn register_ecommerce_mdl(ctx: &SessionContext) -> Result<(SessionContext, Arc<AnalyzedWrenMDL>)> {
    let manifest = ManifestBuilder::new()
        .model(
            ModelBuilder::new("customers")
                .table_reference("datafusion.public.customers")
                .column(ColumnBuilder::new("city", "varchar").build())
                .column(ColumnBuilder::new("id", "varchar").build())
                .column(ColumnBuilder::new("state", "varchar").build())
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
                    ColumnBuilder::new("orders", "orders")
                        .relationship("orders_order_items")
                        .build(),
                )
                .column(
                    ColumnBuilder::new("customer_state", "varchar")
                        .calculated(true)
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
                    ColumnBuilder::new("customers", "customers")
                        .relationship("orders_customer")
                        .build(),
                )
                .column(
                    ColumnBuilder::new("customer_state", "varchar")
                        .calculated(true)
                        .expression("customers.state")
                        .build(),
                )
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
    let _ = register_table_with_mdl(&ctx, Arc::clone(&analyzed_mdl.wren_mdl)).await;
    Ok((ctx.to_owned(), analyzed_mdl))
}

pub async fn register_table_with_mdl(ctx: &SessionContext, wren_mdl: Arc<WrenMDL>) -> Result<()> {
    let catalog = MemoryCatalogProvider::new();
    let schema = MemorySchemaProvider::new();

    catalog
        .register_schema(&wren_mdl.manifest.schema, Arc::new(schema))
        .unwrap();
    ctx.register_catalog(&wren_mdl.manifest.catalog, Arc::new(catalog));

    for model in wren_mdl.manifest.models.iter() {
        let table = WrenDataSource::new(Arc::clone(model))?;
        ctx.register_table(
            format!(
                "{}.{}.{}",
                &wren_mdl.manifest.catalog, &wren_mdl.manifest.schema, &model.name
            ),
            Arc::new(table),
        )?;
    }
    Ok(())
}

struct WrenDataSource {
    schema: SchemaRef,
}

impl WrenDataSource {
    pub fn new(model: Arc<Model>) -> Result<Self> {
        let schema = create_schema(model.get_physical_columns().clone())?;
        Ok(Self { schema })
    }
}

#[async_trait]
impl TableProvider for WrenDataSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unreachable!("WrenDataSource should be replaced before physical planning")
    }
}
