use crate::mdl::lineage::DatasetLink;
use crate::mdl::utils::quoted;
use crate::mdl::{
    manifest::{Column, Model},
    WrenMDL,
};
use crate::mdl::{Dataset, SessionStateRef};
use datafusion::arrow::datatypes::{
    DataType, Field, IntervalUnit, Schema, SchemaBuilder, SchemaRef, TimeUnit,
};
use datafusion::catalog_common::TableReference;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::DefaultTableSource;
use datafusion::error::Result;
use datafusion::logical_expr::{builder::LogicalTableSource, Expr, TableSource};
use log::debug;
use petgraph::dot::{Config, Dot};
use petgraph::Graph;
use std::collections::HashSet;
use std::{collections::HashMap, sync::Arc};

fn create_mock_list_type() -> DataType {
    let string_filed = Arc::new(Field::new("string", DataType::Utf8, false));
    DataType::List(string_filed)
}

fn create_mock_struct_type() -> DataType {
    let mut builder = SchemaBuilder::new();
    builder.push(Field::new("a", DataType::Boolean, false));
    let fields = builder.finish().fields;
    DataType::Struct(fields)
}

pub fn map_data_type(data_type: &str) -> DataType {
    let lower = data_type.to_lowercase();
    let data_type = lower.as_str();
    // Currently, we don't care about the element type of the array or struct.
    // We only care about the array or struct itself.
    if data_type.starts_with("array") {
        return create_mock_list_type();
    }
    if data_type.starts_with("struct") {
        return create_mock_struct_type();
    }
    match data_type {
        // Wren Definition Types
        "bool" => DataType::Boolean,
        "tinyint" => DataType::Int8,
        "int2" => DataType::Int16,
        "smallint" => DataType::Int16,
        "int4" => DataType::Int32,
        "int" => DataType::Int32,
        "integer" => DataType::Int32,
        "int8" => DataType::Int64,
        "bigint" => DataType::Int64,
        "numeric" => DataType::Decimal128(38, 10), // set the default precision and scale
        "decimal" => DataType::Decimal128(38, 10),
        "varchar" => DataType::Utf8,
        "char" => DataType::Utf8,
        "bpchar" => DataType::Utf8, // we don't have a BPCHAR type, so we map it to Utf8
        "text" => DataType::Utf8,
        "string" => DataType::Utf8,
        "name" => DataType::Utf8,
        "float4" => DataType::Float32,
        "real" => DataType::Float32,
        "float" => DataType::Float32,
        "float8" => DataType::Float64,
        "double" => DataType::Float64,
        "timestamp" | "datetime" => DataType::Timestamp(TimeUnit::Nanosecond, None), // chose the smallest time unit
        "timestamptz" | "timestamp_with_timezone" | "timestamp_with_time_zone" => {
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        }
        "date" => DataType::Date32,
        "interval" => DataType::Interval(IntervalUnit::DayTime),
        "json" => DataType::Utf8, // we don't have a JSON type, so we map it to Utf8
        "oid" => DataType::Int32,
        "bytea" => DataType::Binary,
        "uuid" => DataType::Utf8, // we don't have a UUID type, so we map it to Utf8
        "inet" => DataType::Utf8, // we don't have a INET type, so we map it to Utf8
        "unknown" => DataType::Utf8, // we don't have a UNKNOWN type, so we map it to Utf8
        // BigQuery Compatible Types
        "bignumeric" => DataType::Decimal128(38, 10), // set the default precision and scale
        "bytes" => DataType::Binary,
        "float64" => DataType::Float64,
        "int64" => DataType::Int64,
        "time" => DataType::Time32(TimeUnit::Nanosecond), // chose the smallest time unit
        "null" => DataType::Null,
        _ => {
            // default to string
            debug!("map unknown type {} to Utf8", data_type);
            DataType::Utf8
        }
    }
}

pub fn create_table_source(model: &Model) -> Result<Arc<dyn TableSource>> {
    let schema = create_schema(model.get_physical_columns())?;
    Ok(Arc::new(LogicalTableSource::new(schema)))
}

pub fn create_schema(columns: Vec<Arc<Column>>) -> Result<SchemaRef> {
    let fields: Vec<Field> = columns
        .iter()
        .map(|column| {
            let data_type = map_data_type(&column.r#type);
            Ok(Field::new(&column.name, data_type, column.not_null))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(SchemaRef::new(Schema::new_with_metadata(
        fields,
        HashMap::new(),
    )))
}

pub fn create_remote_table_source(
    model: Arc<Model>,
    mdl: &WrenMDL,
    session_state_ref: SessionStateRef,
) -> Result<Arc<dyn TableSource>> {
    if let Some(table_provider) = mdl.get_table(model.table_reference()) {
        Ok(Arc::new(DefaultTableSource::new(table_provider)))
    } else {
        let dataset = Dataset::Model(model);
        let schema = dataset
            .to_remote_schema(Some(mdl.get_register_tables()), session_state_ref)?;
        Ok(Arc::new(LogicalTableSource::new(Arc::new(
            schema.as_arrow().clone(),
        ))))
    }
}

pub fn format_qualified_name(
    catalog: &str,
    schema: &str,
    dataset: &str,
    column: &str,
) -> String {
    format!(
        "{}.{}.{}.{}",
        quoted(catalog),
        quoted(schema),
        quoted(dataset),
        quoted(column)
    )
}

pub fn from_qualified_name(
    wren_mdl: &WrenMDL,
    dataset: &str,
    column: &str,
) -> datafusion::common::Column {
    from_qualified_name_str(wren_mdl.catalog(), wren_mdl.schema(), dataset, column)
}

pub fn from_qualified_name_str(
    catalog: &str,
    schema: &str,
    dataset: &str,
    column: &str,
) -> datafusion::common::Column {
    datafusion::common::Column::from_qualified_name(format_qualified_name(
        catalog, schema, dataset, column,
    ))
}

/// Use to print the graph for debugging purposes
pub fn print_graph(graph: &Graph<Dataset, DatasetLink>) {
    let dot = Dot::with_config(graph, &[Config::EdgeNoLabel]);
    println!("graph: {:?}", dot);
}

/// Check if the table reference belongs to the mdl
pub fn belong_to_mdl(
    mdl: &WrenMDL,
    table_reference: TableReference,
    session: SessionStateRef,
) -> bool {
    let session = session.read();
    let catalog = table_reference
        .catalog()
        .unwrap_or(&session.config_options().catalog.default_catalog);
    let catalog_match = catalog == mdl.catalog();

    let schema = table_reference
        .schema()
        .unwrap_or(&session.config_options().catalog.default_schema);
    let schema_match = schema == mdl.schema();

    catalog_match && schema_match
}

/// Collect all the Columns and OuterReferenceColumns in the expression
pub fn expr_to_columns(
    expr: &Expr,
    accum: &mut HashSet<datafusion::common::Column>,
) -> Result<()> {
    expr.apply(|expr| {
        match expr {
            Expr::Column(qc) => {
                accum.insert(qc.clone());
            }
            Expr::OuterReferenceColumn(_, column) => {
                accum.insert(column.clone());
            }
            // Use explicit pattern match instead of a default
            // implementation, so that in the future if someone adds
            // new Expr types, they will check here as well
            Expr::Unnest(_)
            | Expr::ScalarVariable(_, _)
            | Expr::Alias(_)
            | Expr::Literal(_)
            | Expr::BinaryExpr { .. }
            | Expr::Like { .. }
            | Expr::SimilarTo { .. }
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_)
            | Expr::Negative(_)
            | Expr::Between { .. }
            | Expr::Case { .. }
            | Expr::Cast { .. }
            | Expr::TryCast { .. }
            | Expr::ScalarFunction(..)
            | Expr::WindowFunction { .. }
            | Expr::AggregateFunction { .. }
            | Expr::GroupingSet(_)
            | Expr::InList { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery(_)
            | Expr::ScalarSubquery(_)
            | Expr::Wildcard { .. }
            | Expr::Placeholder(_) => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .map(|_| ())
}

#[cfg(test)]
mod test {
    use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
    use datafusion::common::Result;

    use crate::logical_plan::utils::{create_mock_list_type, create_mock_struct_type};

    #[test]
    pub fn test_map_data_type() -> Result<()> {
        let test_cases = vec![
            ("bool", DataType::Boolean),
            ("tinyint", DataType::Int8),
            ("int2", DataType::Int16),
            ("smallint", DataType::Int16),
            ("int4", DataType::Int32),
            ("integer", DataType::Int32),
            ("int8", DataType::Int64),
            ("bigint", DataType::Int64),
            ("numeric", DataType::Decimal128(38, 10)),
            ("decimal", DataType::Decimal128(38, 10)),
            ("varchar", DataType::Utf8),
            ("char", DataType::Utf8),
            ("bpchar", DataType::Utf8),
            ("text", DataType::Utf8),
            ("string", DataType::Utf8),
            ("name", DataType::Utf8),
            ("float4", DataType::Float32),
            ("real", DataType::Float32),
            ("float8", DataType::Float64),
            ("double", DataType::Float64),
            ("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None)),
            (
                "timestamptz",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            ("date", DataType::Date32),
            ("interval", DataType::Interval(IntervalUnit::DayTime)),
            ("json", DataType::Utf8),
            ("oid", DataType::Int32),
            ("bytea", DataType::Binary),
            ("uuid", DataType::Utf8),
            ("inet", DataType::Utf8),
            ("unknown", DataType::Utf8),
            ("bignumeric", DataType::Decimal128(38, 10)),
            ("bytes", DataType::Binary),
            ("datetime", DataType::Timestamp(TimeUnit::Nanosecond, None)),
            ("float64", DataType::Float64),
            ("int64", DataType::Int64),
            ("time", DataType::Time32(TimeUnit::Nanosecond)),
            ("null", DataType::Null),
            ("geography", DataType::Utf8),
            ("range", DataType::Utf8),
            ("array<int64>", create_mock_list_type()),
            ("struct<name string, age int>", create_mock_struct_type()),
        ];
        for (data_type, expected) in test_cases {
            let result = super::map_data_type(data_type);
            assert_eq!(result, expected);
            // test case insensitivity
            let result = super::map_data_type(&data_type.to_uppercase());
            assert_eq!(result, expected);
        }
        Ok(())
    }
}
