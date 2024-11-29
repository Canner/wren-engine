use crate::errors::CoreError;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use pyo3::{pyclass, pyfunction, pymethods, PyResult};
use serde::{Deserialize, Serialize};
use std::iter::Iterator;
use std::sync::Arc;
use wren_core::mdl::manifest::{
    Column, JoinType, Manifest, Metric, Model, Relationship, TimeGrain, TimeUnit, View,
};

#[pyfunction]
pub fn to_json_base64(mdl: PyManifest) -> Result<String, CoreError> {
    let mdl_json = serde_json::to_string(&mdl)?;
    let mdl_base64 = BASE64_STANDARD.encode(mdl_json.as_bytes());
    Ok(mdl_base64)
}

pub fn to_manifest(mdl_base64: &str) -> Result<Manifest, CoreError> {
    let decoded_bytes = BASE64_STANDARD.decode(mdl_base64)?;
    let mdl_json = String::from_utf8(decoded_bytes)?;
    let manifest = serde_json::from_str::<Manifest>(&mdl_json)?;
    Ok(manifest)
}

#[pyclass(name = "Manifest")]
#[derive(Serialize, Deserialize, Clone)]
pub struct PyManifest {
    pub catalog: String,
    pub schema: String,
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
}

#[pyclass(name = "Model")]
#[derive(Serialize, Deserialize)]
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
            table_reference: model.table_reference.clone(),
            columns: model.columns.clone(),
            primary_key: model.primary_key.clone(),
            cached: model.cached,
            refresh_time: model.refresh_time.clone(),
        }
    }
}

#[pyclass(name = "Column")]
#[derive(Serialize, Deserialize)]
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
#[derive(Serialize, Deserialize)]
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
#[derive(Serialize, Deserialize, PartialEq, Eq)]
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
#[derive(Serialize, Deserialize)]
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
#[derive(Serialize, Deserialize)]
pub struct PyTimeGrain {
    pub name: String,
    pub ref_column: String,
    pub date_parts: Vec<TimeUnit>,
}

#[pyclass(name = "TimeUnit", eq)]
#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub enum PyTimeUnit {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

#[pyclass(name = "View")]
#[derive(Serialize, Deserialize)]
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
