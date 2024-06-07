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

use wren_core::logical_plan::rule::{ModelAnalyzeRule, ModelGenerationRule};
use wren_core::logical_plan::utils::create_schema;
use wren_core::mdl::builder::{ColumnBuilder, ManifestBuilder, ModelBuilder};
use wren_core::mdl::manifest::Model;
use wren_core::mdl::{AnalyzedWrenMDL, WrenMDL};

use crate::engine::utils::read_dir_recursive;

const TEST_RESOURCES: &str = "tests/resources";

/// Context for running tests
pub struct TestContext {
    /// Context for running queries
    ctx: SessionContext,
    /// Temporary directory created and cleared at the end of the test
    test_dir: Option<TempDir>,
}

impl TestContext {
    pub fn new(ctx: SessionContext) -> Self {
        Self {
            ctx,
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
        let test_ctx = TestContext::new(ctx);

        let file_name = relative_path.file_name().unwrap().to_str().unwrap();
        match file_name {
            "model.slt" => {
                info!("Registering local temporary table");
                register_ecommerce_table(&test_ctx).await;
            }
            _ => {
                info!("Using default SessionContext");
            }
        };
        Some(test_ctx)
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
}

pub async fn register_ecommerce_table(test_ctx: &TestContext) {
    let path = PathBuf::from(TEST_RESOURCES).join("ecommerce");
    let data = read_dir_recursive(&path).unwrap();

    // register csv file with the execution context
    for file in data.iter() {
        let table_name = file.file_stem().unwrap().to_str().unwrap();
        test_ctx
            .ctx
            .register_csv(table_name, file.to_str().unwrap(), CsvReadOptions::new())
            .await
            .unwrap();
    }
    register_ecommerce_mdl(&test_ctx.ctx).await;
}

async fn register_ecommerce_mdl(ctx: &SessionContext) {
    let manifest = ManifestBuilder::new()
        .model(
            ModelBuilder::new("customers")
                .column(ColumnBuilder::new("city", "varchar").build())
                .column(ColumnBuilder::new("id", "varchar").build())
                .column(ColumnBuilder::new("state", "varchar").build())
                .primary_key("id")
                .build(),
        )
        .model(
            ModelBuilder::new("order_items")
                .column(ColumnBuilder::new("freight_value", "double").build())
                .column(ColumnBuilder::new("id", "varchar").build())
                .column(ColumnBuilder::new("item_number", "integer").build())
                .column(ColumnBuilder::new("order_id", "varchar").build())
                .column(ColumnBuilder::new("price", "double").build())
                .column(ColumnBuilder::new("product_id", "varchar").build())
                .column(ColumnBuilder::new("shipping_limit_date", "varchar").build())
                .primary_key("id")
                .build(),
        )
        .model(
            ModelBuilder::new("orders")
                .column(ColumnBuilder::new("approved_timestamp", "timestamp").build())
                .column(ColumnBuilder::new("customer_id", "varchar").build())
                .column(ColumnBuilder::new("delivered_carrier_date", "date").build())
                .column(ColumnBuilder::new("estimated_deliver_date", "date").build())
                .column(ColumnBuilder::new("order_id", "varchar").build())
                .column(ColumnBuilder::new("purchase_timestamp", "timestamp").build())
                .build(),
        )
        .build();
    let analyzed_mdl = Arc::new(AnalyzedWrenMDL::analyze(manifest));
    ctx.state().with_analyzer_rules(vec![
        Arc::new(ModelAnalyzeRule::new(Arc::clone(&analyzed_mdl))),
        Arc::new(ModelGenerationRule::new(Arc::clone(&analyzed_mdl))),
    ]);
    register_table_with_mdl(ctx, Arc::clone(&analyzed_mdl.wren_mdl)).await;
}

pub async fn register_table_with_mdl(ctx: &SessionContext, wren_mdl: Arc<WrenMDL>) {
    let catalog = MemoryCatalogProvider::new();
    let schema = MemorySchemaProvider::new();

    catalog
        .register_schema(&wren_mdl.manifest.schema, Arc::new(schema))
        .unwrap();
    ctx.register_catalog(&wren_mdl.manifest.catalog, Arc::new(catalog));

    for model in wren_mdl.manifest.models.iter() {
        let table = WrenDataSource::new(Arc::clone(model));
        ctx.register_table(
            format!(
                "{}.{}.{}",
                &wren_mdl.manifest.catalog, &wren_mdl.manifest.schema, &model.name
            ),
            Arc::new(table),
        )
        .unwrap();
    }
}

struct WrenDataSource {
    schema: SchemaRef,
}

impl WrenDataSource {
    pub fn new(model: Arc<Model>) -> Self {
        let schema = create_schema(model.columns.clone());
        Self { schema }
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
