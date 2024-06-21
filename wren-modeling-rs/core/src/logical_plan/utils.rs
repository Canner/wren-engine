use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::common::not_impl_err;
use std::{collections::HashMap, sync::Arc};

use datafusion::datasource::DefaultTableSource;
use datafusion::error::Result;
use datafusion::logical_expr::{builder::LogicalTableSource, TableSource};
use petgraph::dot::{Config, Dot};
use petgraph::Graph;

use crate::mdl::lineage::DatasetLink;
use crate::mdl::{
    manifest::{Column, Model},
    Dataset, WrenMDL,
};

pub fn map_data_type(data_type: &str) -> Result<DataType> {
    let result = match data_type {
        "integer" => DataType::Int32,
        "bigint" => DataType::Int64,
        "varchar" => DataType::Utf8,
        "double" => DataType::Float64,
        "timestamp" => DataType::Timestamp(TimeUnit::Nanosecond, None),
        "date" => DataType::Date32,
        _ => return not_impl_err!("Unsupported data type: {}", &data_type),
    };
    Ok(result)
}

pub fn create_table_source(model: &Model) -> Result<Arc<dyn TableSource>> {
    let schema = create_schema(model.get_physical_columns())?;
    Ok(Arc::new(LogicalTableSource::new(schema)))
}

pub fn create_schema(columns: Vec<Arc<Column>>) -> Result<SchemaRef> {
    let fields: Vec<Field> = columns
        .iter()
        .map(|column| {
            let data_type = map_data_type(&column.r#type)?;
            Ok(Field::new(&column.name, data_type, column.no_null))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(SchemaRef::new(Schema::new_with_metadata(
        fields,
        HashMap::new(),
    )))
}

pub fn create_remote_table_source(model: &Model, mdl: &WrenMDL) -> Arc<dyn TableSource> {
    if let Some(table_provider) = mdl.get_table(&model.table_reference) {
        Arc::new(DefaultTableSource::new(table_provider))
    } else {
        let fields: Vec<Field> = model
            .get_physical_columns()
            .iter()
            .map(|column| {
                let column = Arc::clone(column);
                let name = if let Some(ref expression) = column.expression {
                    expression.clone()
                } else {
                    column.name.clone()
                };
                // We don't know the data type of the remote table, so we just mock a Int32 type here
                Field::new(name, DataType::Int8, column.no_null)
            })
            .collect();

        let schema = SchemaRef::new(Schema::new_with_metadata(fields, HashMap::new()));
        Arc::new(LogicalTableSource::new(schema))
    }
}

pub fn format_qualified_name(
    catalog: &str,
    schema: &str,
    dataset: &str,
    column: &str,
) -> String {
    format!("{}.{}.{}.{}", catalog, schema, dataset, column)
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
