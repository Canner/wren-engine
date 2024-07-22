use std::{collections::HashMap, sync::Arc};

use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::DefaultTableSource;
use datafusion::{
    common::{plan_err, Result},
    config::ConfigOptions,
    logical_expr::{AggregateUDF, ScalarUDF, TableSource, WindowUDF},
    sql::{planner::ContextProvider, TableReference},
};

use crate::mdl::WrenMDL;

use super::utils::create_table_source;

#[deprecated(since = "0.8.0", note = "try to create plan by SessionContext instead")]
/// WrenContextProvider is a ContextProvider implementation that uses the WrenMDL
/// to provide table sources and other metadata.
pub struct WrenContextProvider {
    options: ConfigOptions,
    tables: HashMap<String, Arc<dyn TableSource>>,
}

#[allow(deprecated)]
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

#[allow(deprecated)]
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
