use datafusion::prelude::SessionContext;
use std::sync::Arc;
use wren_core::mdl::builder::{
    ColumnBuilder, ManifestBuilder, ModelBuilder, RelationshipBuilder, ViewBuilder,
};
use wren_core::mdl::manifest::{JoinType, Manifest};
use wren_core::mdl::{transform_sql_with_ctx, AnalyzedWrenMDL};

#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    let manifest = init_manifest();
    let analyzed_mdl = Arc::new(AnalyzedWrenMDL::analyze(manifest)?);

    let sql = r#"
        select count(*) from wrenai.public."Order_items";
    "#;
    println!("Original SQL: \n{}", sql);
    let sql = match transform_sql_with_ctx(&SessionContext::new(), analyzed_mdl, sql).await {
        Ok(sql) => sql,
        Err(e) => {
            eprintln!("Error: {}", e);
            return Ok(());
        }
    };
    println!("Wren engine generated SQL: \n{}", sql);
    Ok(())
}

fn init_manifest() -> Manifest {
    ManifestBuilder::new()
        .model(
            ModelBuilder::new("Customers")
                .table_reference("datafusion.public.customers")
                .column(ColumnBuilder::new("City", "varchar").expression("city").build())
                .column(ColumnBuilder::new("Id", "varchar").expression("id").build())
                .column(ColumnBuilder::new("State", "varchar").expression("state").build())
                .column(
                    ColumnBuilder::new_calculated("City_state", "varchar")
                        .expression(r#""City" || ' ' || "State""#)
                        .build(),
                )
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
        .build()
}
