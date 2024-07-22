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
    #[serde(default, with = "table_reference")]
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

mod table_reference {
    use serde::{self, Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Deserialize, Serialize, Default)]
    struct TableReference {
        catalog: Option<String>,
        schema: Option<String>,
        table: String,
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
    where
        D: Deserializer<'de>,
    {
        let TableReference {
            catalog,
            schema,
            table,
        } = TableReference::deserialize(deserializer)?;
        let mut result = String::new();
        if let Some(catalog) = catalog.filter(|c| !c.is_empty()) {
            result.push_str(&catalog);
            result.push('.');
        }
        if let Some(schema) = schema.filter(|s| !s.is_empty()) {
            result.push_str(&schema);
            result.push('.');
        }
        result.push_str(&table);
        Ok(result)
    }

    pub fn serialize<S>(table_ref: &String, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let parts: Vec<&str> = table_ref.split('.').filter(|p| !p.is_empty()).collect();
        if parts.len() > 3 {
            return Err(serde::ser::Error::custom(format!(
                "Invalid table reference: {table_ref}"
            )));
        }
        let table_ref = if parts.len() == 3 {
            TableReference {
                catalog: Some(parts[0].to_string()),
                schema: Some(parts[1].to_string()),
                table: parts[2].to_string(),
            }
        } else if parts.len() == 2 {
            TableReference {
                catalog: None,
                schema: Some(parts[0].to_string()),
                table: parts[1].to_string(),
            }
        } else if parts.len() == 1 {
            TableReference {
                catalog: None,
                schema: None,
                table: parts[0].to_string(),
            }
        } else {
            TableReference::default()
        };
        table_ref.serialize(serializer)
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
