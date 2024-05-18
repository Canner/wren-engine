use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};

// This is the main struct that holds all the information about the manifest
#[derive(Serialize, Deserialize, Debug)]
pub struct Manifest {
    pub catalog: String,
    pub schema: String,
    #[serde(default)]
    pub models: Vec<Arc<Model>>,
    #[serde(default)]
    pub relationships: Vec<Arc<Relationship>>,
    #[serde(default)]
    pub metrics: Vec<Arc<Metric>>,
    #[serde(default)]
    pub views: Vec<Arc<View>>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Model {
    pub name: String,
    #[serde(default)]
    pub ref_sql: String,
    #[serde(default)]
    pub base_object: String,
    #[serde(default)]
    pub table_reference: String,
    pub columns: Vec<Arc<Column>>,
    #[serde(default)]
    pub primary_key: String,
    #[serde(default)]
    pub cached: bool,
    #[serde(default)]
    pub refresh_time: String,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Column {
    pub name: String,
    pub r#type: String,
    #[serde(default)]
    pub relationship: String,
    #[serde(default)]
    pub is_calculated: bool,
    #[serde(default)]
    pub no_null: bool,
    #[serde(default)]
    pub expression: Option<String>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Relationship {
    pub name: String,
    pub models: Vec<String>,
    pub join_type: JoinType,
    pub condition: String,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

// handle case insensitive
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    OneToOne,
    OneToMany,
    ManyToOne,
    ManyToMany,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Metric {
    pub name: String,
    pub base_object: String,
    pub dimension: Vec<Arc<Column>>,
    pub measure: Vec<Arc<Column>>,
    pub time_grain: Vec<TimeGrain>,
    pub cached: bool,
    pub refresh_time: String,
    pub properties: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TimeGrain {
    pub name: String,
    pub ref_column: String,
    pub date_parts: Vec<TimeUnit>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum TimeUnit {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct View {
    pub name: String,
    pub statement: String,
    pub properties: HashMap<String, String>,
}
