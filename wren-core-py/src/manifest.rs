use crate::errors::CoreError;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use pyo3::{pyclass, pyfunction, pymethods, PyResult};
use serde::{Deserialize, Serialize};
use std::iter::Iterator;
use std::sync::Arc;
use wren_core::mdl::manifest::{
    Column, DataSource, JoinType, Manifest, Metric, Model, Relationship, TimeGrain,
    TimeUnit, View,
};

/// Convert a manifest to a JSON string and then encode it as base64.
#[pyfunction]
pub fn to_json_base64(mdl: PyManifest) -> Result<String, CoreError> {
    let mdl_json = serde_json::to_string(&mdl)?;
    let mdl_base64 = BASE64_STANDARD.encode(mdl_json.as_bytes());
    Ok(mdl_base64)
}

/// Convert a base64 encoded JSON string to a manifest object.
pub fn to_manifest(mdl_base64: &str) -> Result<Manifest, CoreError> {
    let decoded_bytes = BASE64_STANDARD.decode(mdl_base64)?;
    let mdl_json = String::from_utf8(decoded_bytes)?;
    let manifest = serde_json::from_str::<Manifest>(&mdl_json)?;
    Ok(manifest)
}

#[pyclass(name = "Manifest")]
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PyManifest {
    pub catalog: String,
    pub schema: String,
    pub data_source: Option<DataSource>,
    pub models: Vec<Arc<Model>>,
    pub relationships: Vec<Arc<Relationship>>,
    pub metrics: Vec<Arc<Metric>>,
    pub views: Vec<Arc<View>>,
}

#[pymethods]
impl PyManifest {
    #[getter]
    fn catalog(&self) -> PyResult<String> {
        Ok(self.catalog.clone())
    }

    #[getter]
    fn schema(&self) -> PyResult<String> {
        Ok(self.schema.clone())
    }

    #[getter]
    fn models(&self) -> PyResult<Vec<PyModel>> {
        Ok(self
            .models
            .iter()
            .map(|m| PyModel::from(m.as_ref()))
            .collect())
    }

    #[getter]
    fn relationships(&self) -> PyResult<Vec<PyRelationship>> {
        Ok(self
            .relationships
            .iter()
            .map(|r| PyRelationship::from(r.as_ref()))
            .collect())
    }

    #[getter]
    fn metrics(&self) -> PyResult<Vec<PyMetric>> {
        Ok(self
            .metrics
            .iter()
            .map(|m| PyMetric::from(m.as_ref()))
            .collect())
    }

    #[getter]
    fn views(&self) -> PyResult<Vec<PyView>> {
        Ok(self
            .views
            .iter()
            .map(|v| PyView::from(v.as_ref()))
            .collect())
    }

    #[getter]
    fn data_source(&self) -> PyResult<Option<PyDataSource>> {
        Ok(self.data_source.map(PyDataSource::from))
    }
}

impl From<&Manifest> for PyManifest {
    fn from(manifest: &Manifest) -> Self {
        Self {
            catalog: manifest.catalog.clone(),
            schema: manifest.schema.clone(),
            models: manifest.models.clone(),
            relationships: manifest.relationships.clone(),
            metrics: manifest.metrics.clone(),
            views: manifest.views.clone(),
            data_source: manifest.data_source,
        }
    }
}

#[pyclass(name = "Model")]
#[derive(Serialize, Deserialize, Debug)]
pub struct PyModel {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub ref_sql: Option<String>,
    #[pyo3(get)]
    pub base_object: Option<String>,
    #[pyo3(get)]
    pub table_reference: Option<String>,
    pub columns: Vec<Arc<Column>>,
    #[pyo3(get)]
    pub primary_key: Option<String>,
    #[pyo3(get)]
    pub cached: bool,
    #[pyo3(get)]
    pub refresh_time: Option<String>,
}

#[pymethods]
impl PyModel {
    #[getter]
    fn columns(&self) -> PyResult<Vec<PyColumn>> {
        Ok(self
            .columns
            .iter()
            .map(|c| PyColumn::from(c.as_ref()))
            .collect())
    }
}

impl From<&Model> for PyModel {
    fn from(model: &Model) -> Self {
        Self {
            name: model.name.clone(),
            ref_sql: model.ref_sql.clone(),
            base_object: model.base_object.clone(),
            table_reference: Some(String::from(model.table_reference())),
            columns: model.columns.clone(),
            primary_key: model.primary_key.clone(),
            cached: model.cached,
            refresh_time: model.refresh_time.clone(),
        }
    }
}

#[pyclass(name = "Column")]
#[derive(Serialize, Deserialize, Debug)]
pub struct PyColumn {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub r#type: String,
    #[pyo3(get)]
    pub relationship: Option<String>,
    #[pyo3(get)]
    pub is_calculated: bool,
    #[pyo3(get)]
    pub not_null: bool,
    #[pyo3(get)]
    pub expression: Option<String>,
    #[pyo3(get)]
    pub is_hidden: bool,
}

impl From<&Column> for PyColumn {
    fn from(column: &Column) -> Self {
        Self {
            name: column.name.clone(),
            r#type: column.r#type.clone(),
            relationship: column.relationship.clone(),
            is_calculated: column.is_calculated,
            not_null: column.not_null,
            expression: column.expression.clone(),
            is_hidden: column.is_hidden,
        }
    }
}

#[pyclass(name = "Relationship")]
#[derive(Serialize, Deserialize, Debug)]
pub struct PyRelationship {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub models: Vec<String>,
    pub join_type: JoinType,
    #[pyo3(get)]
    pub condition: String,
}

#[pymethods]
impl PyRelationship {
    #[getter]
    fn join_type(&self) -> PyResult<PyJoinType> {
        Ok(PyJoinType::from(&self.join_type))
    }
}

impl From<&Relationship> for PyRelationship {
    fn from(relationship: &Relationship) -> Self {
        Self {
            name: relationship.name.clone(),
            models: relationship.models.clone(),
            join_type: relationship.join_type,
            condition: relationship.condition.clone(),
        }
    }
}

#[pyclass(name = "JoinType", eq)]
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum PyJoinType {
    #[serde(alias = "one_to_one")]
    OneToOne,
    #[serde(alias = "one_to_many")]
    OneToMany,
    #[serde(alias = "many_to_one")]
    ManyToOne,
    #[serde(alias = "many_to_many")]
    ManyToMany,
}

impl From<&JoinType> for PyJoinType {
    fn from(join_type: &JoinType) -> Self {
        match join_type {
            JoinType::OneToOne => PyJoinType::OneToOne,
            JoinType::OneToMany => PyJoinType::OneToMany,
            JoinType::ManyToOne => PyJoinType::ManyToOne,
            JoinType::ManyToMany => PyJoinType::ManyToMany,
        }
    }
}

#[pyclass(name = "Metric")]
#[derive(Serialize, Deserialize, Debug)]
pub struct PyMetric {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub base_object: String,
    pub dimension: Vec<Arc<Column>>,
    pub measure: Vec<Arc<Column>>,
    pub time_grain: Vec<TimeGrain>,
    #[pyo3(get)]
    pub cached: bool,
    #[pyo3(get)]
    pub refresh_time: Option<String>,
}

impl From<&Metric> for PyMetric {
    fn from(metric: &Metric) -> Self {
        Self {
            name: metric.name.clone(),
            base_object: metric.base_object.clone(),
            dimension: metric.dimension.clone(),
            measure: metric.measure.clone(),
            time_grain: metric.time_grain.clone(),
            cached: metric.cached,
            refresh_time: metric.refresh_time.clone(),
        }
    }
}

#[pyclass(name = "TimeGrain")]
#[derive(Serialize, Deserialize, Debug)]
pub struct PyTimeGrain {
    pub name: String,
    pub ref_column: String,
    pub date_parts: Vec<TimeUnit>,
}

#[pyclass(name = "TimeUnit", eq)]
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum PyTimeUnit {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

#[pyclass(name = "View")]
#[derive(Serialize, Deserialize, Debug)]
pub struct PyView {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub statement: String,
}

impl From<&View> for PyView {
    fn from(view: &View) -> Self {
        Self {
            name: view.name.clone(),
            statement: view.statement.clone(),
        }
    }
}

#[pyclass(name = "DataSource", eq)]
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PyDataSource {
    #[serde(alias = "bigquery")]
    BigQuery,
    #[serde(alias = "clickhouse")]
    Clickhouse,
    #[serde(alias = "canner")]
    Canner,
    #[serde(alias = "trino")]
    Trino,
    #[serde(alias = "mssql")]
    MsSQL,
    #[serde(alias = "mysql")]
    MySQL,
    #[serde(alias = "postgres")]
    Postgres,
    #[serde(alias = "snowflake")]
    Snowflake,
    #[serde(alias = "datafusion")]
    Datafusion,
    #[serde(alias = "duckdb")]
    DuckDB,
}

impl From<DataSource> for PyDataSource {
    fn from(data_source: DataSource) -> Self {
        match data_source {
            DataSource::BigQuery => PyDataSource::BigQuery,
            DataSource::Clickhouse => PyDataSource::Clickhouse,
            DataSource::Canner => PyDataSource::Canner,
            DataSource::Trino => PyDataSource::Trino,
            DataSource::MSSQL => PyDataSource::MsSQL,
            DataSource::MySQL => PyDataSource::MySQL,
            DataSource::Postgres => PyDataSource::Postgres,
            DataSource::Snowflake => PyDataSource::Snowflake,
            DataSource::Datafusion => PyDataSource::Datafusion,
            DataSource::DuckDB => PyDataSource::DuckDB,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::manifest::{to_json_base64, to_manifest, PyManifest};
    use std::sync::Arc;
    use wren_core::mdl::manifest::DataSource::BigQuery;
    use wren_core::mdl::manifest::Model;

    #[test]
    fn test_manifest_to_json_base64() {
        let py_manifest = PyManifest {
            catalog: "catalog".to_string(),
            schema: "schema".to_string(),
            models: vec![
                Arc::from(Model {
                    name: "model_1".to_string(),
                    ref_sql: "SELECT * FROM table".to_string().into(),
                    base_object: None,
                    table_reference: None,
                    columns: vec![],
                    primary_key: None,
                    cached: false,
                    refresh_time: None,
                }),
                Arc::from(Model {
                    name: "model_2".to_string(),
                    ref_sql: None,
                    base_object: None,
                    table_reference: "catalog.schema.table".to_string().into(),
                    columns: vec![],
                    primary_key: None,
                    cached: false,
                    refresh_time: None,
                }),
            ],
            relationships: vec![],
            metrics: vec![],
            views: vec![],
            data_source: Some(BigQuery),
        };
        let base64_str = to_json_base64(py_manifest).unwrap();
        let manifest = to_manifest(&base64_str).unwrap();
        assert_eq!(manifest.catalog, "catalog");
        assert_eq!(manifest.schema, "schema");
        assert_eq!(manifest.models.len(), 2);
        assert_eq!(manifest.models[0].name, "model_1");
        assert_eq!(
            manifest.models[0].ref_sql,
            Some("SELECT * FROM table".to_string())
        );
        assert_eq!(manifest.models[1].name(), "model_2");
        assert_eq!(manifest.models[1].table_reference(), "catalog.schema.table");
        assert_eq!(manifest.data_source, Some(BigQuery));
    }
}
