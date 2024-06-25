use std::{collections::HashMap, sync::Arc};

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::builder::LogicalTableSource;
use datafusion::{
    common::{plan_err, Result},
    config::ConfigOptions,
    logical_expr::{AggregateUDF, ScalarUDF, TableSource, WindowUDF},
    sql::{planner::ContextProvider, TableReference},
};

use crate::mdl::manifest::{Column, Model};
use crate::mdl::{utils, WrenMDL};

use super::utils::{create_table_source, map_data_type};

/// WrenContextProvider is a ContextProvider implementation that uses the WrenMDL
/// to provide table sources and other metadata.
pub struct WrenContextProvider {
    options: ConfigOptions,
    tables: HashMap<String, Arc<dyn TableSource>>,
}

impl WrenContextProvider {
    pub fn new(mdl: &WrenMDL) -> Result<Self> {
        let mut tables = HashMap::new();
        // register model table
        for model in mdl.manifest.models.iter() {
            tables.insert(
                format!("{}.{}.{}", mdl.catalog(), mdl.schema(), model.name()),
                create_table_source(model)?,
            );
        }
        // register physical table
        for (name, table) in mdl.register_tables.iter() {
            tables.insert(
                name.clone(),
                Arc::new(DefaultTableSource::new(table.clone())),
            );
        }
        Ok(Self {
            tables,
            options: Default::default(),
        })
    }

    pub fn new_bare(mdl: &WrenMDL) -> Result<Self> {
        let mut tables = HashMap::new();
        for model in mdl.manifest.models.iter() {
            tables.insert(model.name().to_string(), create_table_source(model)?);
        }
        Ok(Self {
            tables,
            options: Default::default(),
        })
    }
}

impl ContextProvider for WrenContextProvider {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        let table_name = name.to_string();
        match self.tables.get(&table_name) {
            Some(table) => Ok(table.clone()),
            _ => plan_err!("Table not found: {}", &table_name),
        }
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }

    fn udf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udaf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }
}

/// RemoteContextProvider is a ContextProvider implementation that is used to provide
/// the schema for the remote column in the column expression
pub struct RemoteContextProvider {
    options: ConfigOptions,
    tables: HashMap<String, Arc<dyn TableSource>>,
}

impl RemoteContextProvider {
    pub fn new(mdl: &WrenMDL) -> Result<Self> {
        let tables = mdl
            .manifest
            .models
            .iter()
            .map(|model| {
                let remove_provider = mdl.get_table(&model.table_reference);
                let datasource = if let Some(table_provider) = remove_provider {
                    Arc::new(DefaultTableSource::new(table_provider))
                } else {
                    create_remote_table_source(model, mdl)?
                };
                Ok((model.table_reference.clone(), datasource))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        Ok(Self {
            tables,
            options: Default::default(),
        })
    }
}

impl ContextProvider for RemoteContextProvider {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        match self.tables.get(name.to_string().as_str()) {
            Some(table) => Ok(table.clone()),
            _ => plan_err!("Table not found: {}", name.table()),
        }
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }

    fn udf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udaf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }
}

fn create_remote_table_source(
    model: &Model,
    wren_mdl: &WrenMDL,
) -> Result<Arc<dyn TableSource>> {
    if let Some(table_provider) = wren_mdl.get_table(&model.table_reference) {
        Ok(Arc::new(DefaultTableSource::new(table_provider)))
    } else {
        let schema = create_schema(model.get_physical_columns());
        Ok(Arc::new(LogicalTableSource::new(schema?)))
    }
}

fn create_schema(columns: Vec<Arc<Column>>) -> Result<SchemaRef> {
    let fields: Vec<Field> = columns
        .iter()
        .filter(|c| !c.is_calculated)
        .flat_map(|column| {
            if column.expression.is_none() {
                let data_type = if let Ok(data_type) = map_data_type(&column.r#type) {
                    data_type
                } else {
                    // TODO optimize to use Datafusion's error type
                    unimplemented!("Unsupported data type: {}", column.r#type)
                };
                vec![Field::new(&column.name, data_type, column.no_null)]
            } else if let Ok(idents) =
                utils::collect_identifiers(column.expression.as_ref().unwrap())
            {
                idents
                    .iter()
                    .map(|c| {
                        // we don't know the data type or nullable of the remote table,
                        // so we just mock a Int32 type and false here
                        Field::new(&c.name, DataType::Int8, true)
                    })
                    .collect()
            } else {
                panic!(
                    "Failed to collect identifiers from expression: {}",
                    column.expression.as_ref().unwrap()
                );
            }
        })
        .collect();
    Ok(SchemaRef::new(Schema::new_with_metadata(
        fields,
        HashMap::new(),
    )))
}

pub(crate) struct DynamicContextProvider {
    delegate: Box<dyn ContextProvider>,
}

impl DynamicContextProvider {
    pub fn new(delegate: Box<dyn ContextProvider>) -> Self {
        Self { delegate }
    }
}

impl ContextProvider for DynamicContextProvider {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        self.delegate.get_table_source(name)
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        self.delegate.options()
    }

    fn udf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udaf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }
}
