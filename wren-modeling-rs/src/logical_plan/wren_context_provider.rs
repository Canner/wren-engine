use std::{collections::HashMap, sync::Arc};

use crate::mdl::WrenMDL;
use arrow_schema::DataType;
use datafusion::{
    common::{plan_err, Result},
    config::ConfigOptions,
    logical_expr::{AggregateUDF, ScalarUDF, TableSource, WindowUDF},
    sql::{planner::ContextProvider, TableReference},
};

use super::utils::create_table_source;

pub struct WrenContextProvider {
    options: ConfigOptions,
    tables: HashMap<String, Arc<dyn TableSource>>,
}

impl WrenContextProvider {
    pub fn new(mdl: &WrenMDL) -> Self {
        let mut tables = HashMap::new();
        mdl.manifest.models.iter().for_each(|model| {
            let name = model.name.clone();
            tables.insert(name.clone(), create_table_source(model));
        });
        Self {
            tables,
            options: Default::default(),
        }
    }
}

impl ContextProvider for WrenContextProvider {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        match self.tables.get(name.table()) {
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

    fn udfs_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udafs_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udwfs_names(&self) -> Vec<String> {
        Vec::new()
    }
}
