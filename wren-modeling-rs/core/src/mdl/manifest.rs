use std::fmt::Display;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

// This is the main struct that holds all the information about the manifest
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
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
    pub primary_key: Option<String>,
    #[serde(default)]
    pub cached: bool,
    #[serde(default)]
    pub refresh_time: String,
    #[serde(default)]
    pub properties: Vec<(String, String)>,
}

impl Model {
    /// Physical columns are columns that can be selected from the model.
    /// e.g. columns that are not a relationship column
    pub fn get_physical_columns(&self) -> Vec<Arc<Column>> {
        self.columns
            .iter()
            .filter(|c| c.relationship.is_none())
            .map(Arc::clone)
            .collect()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn get_column(&self, column_name: &str) -> Option<Arc<Column>> {
        self.columns
            .iter()
            .find(|c| c.name == column_name)
            .map(Arc::clone)
    }

    pub fn primary_key(&self) -> Option<&str> {
        self.primary_key.as_deref()
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct Column {
    pub name: String,
    pub r#type: String,
    #[serde(default)]
    pub relationship: Option<String>,
    #[serde(default)]
    pub is_calculated: bool,
    #[serde(default)]
    pub no_null: bool,
    #[serde(default)]
    pub expression: Option<String>,
    #[serde(default)]
    pub properties: Vec<(String, String)>,
}

impl Column {
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Serialize, Deserialize, Debug, Hash, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Relationship {
    pub name: String,
    pub models: Vec<String>,
    pub join_type: JoinType,
    pub condition: String,
    #[serde(default)]
    pub properties: Vec<(String, String)>,
}

// handle case insensitive
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    OneToOne,
    OneToMany,
    ManyToOne,
    ManyToMany,
}

impl JoinType {
    pub fn is_to_one(&self) -> bool {
        matches!(self, JoinType::OneToOne | JoinType::ManyToOne)
    }
}

impl Display for JoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinType::OneToOne => write!(f, "one_to_one"),
            JoinType::OneToMany => write!(f, "one_to_many"),
            JoinType::ManyToOne => write!(f, "many_to_one"),
            JoinType::ManyToMany => write!(f, "many_to_many"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct Metric {
    pub name: String,
    pub base_object: String,
    pub dimension: Vec<Arc<Column>>,
    pub measure: Vec<Arc<Column>>,
    pub time_grain: Vec<TimeGrain>,
    pub cached: bool,
    pub refresh_time: String,
    pub properties: Vec<(String, String)>,
}

impl Metric {
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct TimeGrain {
    pub name: String,
    pub ref_column: String,
    pub date_parts: Vec<TimeUnit>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
pub enum TimeUnit {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
pub struct View {
    pub name: String,
    pub statement: String,
    #[serde(default)]
    pub properties: Vec<(String, String)>,
}
