use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::schema::MemorySchemaProvider;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{CsvReadOptions, SessionContext};

use wren_core::logical_plan::utils::create_schema;
use wren_core::mdl::builder::{
    ColumnBuilder, ManifestBuilder, ModelBuilder, RelationshipBuilder,
};
use wren_core::mdl::manifest::{JoinType, Manifest, Model};
use wren_core::mdl::{transform_sql, AnalyzedWrenMDL, WrenMDL};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let manifest = init_manifest();

    // register the table
    let ctx = SessionContext::new();
    ctx.register_csv(
        "orders",
        "sqllogictest/tests/resources/ecommerce/orders.csv",
        CsvReadOptions::new(),
    )
    .await?;
    let provider = ctx
        .catalog("datafusion")
        .unwrap()
        .schema("public")
        .unwrap()
        .table("orders")
        .await?
        .unwrap();

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

    ctx.register_csv(
        "order_items",
        "sqllogictest/tests/resources/ecommerce/order_items.csv",
        CsvReadOptions::new(),
    )
    .await?;
    let order_items_provider = ctx
        .catalog("datafusion")
        .unwrap()
        .schema("public")
        .unwrap()
        .table("order_items")
        .await?
        .unwrap();

    let register = HashMap::from([
        ("datafusion.public.orders".to_string(), provider),
        (
            "datafusion.public.customers".to_string(),
            customers_provider,
        ),
        (
            "datafusion.public.order_items".to_string(),
            order_items_provider,
        ),
    ]);
    let analyzed_mdl =
        Arc::new(AnalyzedWrenMDL::analyze_with_tables(manifest, register)?);

    let transformed = transform_sql(
        &ctx,
        Arc::clone(&analyzed_mdl),
        "select totalprice from wrenai.public.orders",
    )
    .await?;
    register_table_with_mdl(&ctx, Arc::clone(&analyzed_mdl.wren_mdl)).await?;
    let df = ctx.sql(&transformed).await?;
    df.show().await?;
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
                    ColumnBuilder::new("order_items", "order_items")
                        .relationship("orders_order_items")
                        .build(),
                )
                .column(
                    ColumnBuilder::new("totalprice", "double")
                        .expression("sum(order_items.price)")
                        .calculated(true)
                        .build(),
                )
                .primary_key("order_id")
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
        .build()
}

pub async fn register_table_with_mdl(
    ctx: &SessionContext,
    wren_mdl: Arc<WrenMDL>,
) -> Result<()> {
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
        let schema = create_schema(model.get_physical_columns())?;
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
