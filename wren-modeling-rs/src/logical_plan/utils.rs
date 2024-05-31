use std::{collections::HashMap, sync::Arc};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::logical_expr::{builder::LogicalTableSource, TableSource};

use crate::mdl::{
    manifest::{Column, Model},
    WrenMDL,
};

pub fn map_data_type(data_type: &str) -> DataType {
    match data_type {
        "integer" => DataType::Int32,
        "varchar" => DataType::Utf8,
        _ => unimplemented!("{}", &data_type),
    }
}

pub fn create_table_source(model: &Model) -> Arc<dyn TableSource> {
    let schema = create_schema(model.columns.clone());
    Arc::new(LogicalTableSource::new(schema))
}

pub fn create_schema(columns: Vec<Arc<Column>>) -> SchemaRef {
    let fields: Vec<Field> = columns
        .iter()
        .map(|column| {
            let data_type = map_data_type(&column.r#type);
            Field::new(&column.name, data_type, column.no_null)
        })
        .collect();
    SchemaRef::new(Schema::new_with_metadata(fields, HashMap::new()))
}

pub fn create_remote_table_source(model: &Model, _: &WrenMDL) -> Arc<dyn TableSource> {
    let fields: Vec<Field> = model
        .columns
        .iter()
        .map(|column| {
            let column = Arc::clone(column);
            let name = if let Some(ref expression) = column.expression {
                expression.clone()
            } else {
                column.name.clone()
            };
            // We don't know the data type of the remote table, so we just mock a Int32 type here
            Field::new(name, DataType::Int32, column.no_null)
        })
        .collect();
    let schema = SchemaRef::new(Schema::new_with_metadata(fields, HashMap::new()));
    Arc::new(LogicalTableSource::new(schema))
}
