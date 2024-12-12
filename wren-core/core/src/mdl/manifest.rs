use std::fmt::Display;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::NoneAsEmptyString;

/// This is the main struct that holds all the information about the manifest
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone)]
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
    #[serde(default)]
    pub data_source: Option<DataSource>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DataSource {
    #[serde(alias = "bigquery")]
    BigQuery,
    #[serde(alias = "clickhouse")]
    Clickhouse,
    #[serde(alias = "canner")]
    Canner,
    #[serde(alias = "trino")]
    Trino,
    #[serde(alias = "mssql")]
    MSSQL,
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

impl Display for DataSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataSource::BigQuery => write!(f, "BIGQUERY"),
            DataSource::Clickhouse => write!(f, "CLICKHOUSE"),
            DataSource::Canner => write!(f, "CANNER"),
            DataSource::Trino => write!(f, "TRINO"),
            DataSource::MSSQL => write!(f, "MSSQL"),
            DataSource::MySQL => write!(f, "MYSQL"),
            DataSource::Postgres => write!(f, "POSTGRES"),
            DataSource::Snowflake => write!(f, "SNOWFLAKE"),
            DataSource::Datafusion => write!(f, "DATAFUSION"),
            DataSource::DuckDB => write!(f, "DUCKDB"),
        }
    }
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct Model {
    pub name: String,
    #[serde(default)]
    pub ref_sql: Option<String>,
    #[serde(default)]
    pub base_object: Option<String>,
    #[serde(default, with = "table_reference")]
    pub table_reference: Option<String>,
    pub columns: Vec<Arc<Column>>,
    #[serde(default)]
    pub primary_key: Option<String>,
    #[serde(default, with = "bool_from_int")]
    pub cached: bool,
    #[serde(default)]
    pub refresh_time: Option<String>,
}

impl Model {
    pub fn table_reference(&self) -> &str {
        self.table_reference.as_deref().unwrap_or("")
    }
}

mod table_reference {
    use serde::{self, Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Deserialize, Serialize, Default)]
    struct TableReference {
        catalog: Option<String>,
        schema: Option<String>,
        table: Option<String>,
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Option::deserialize(deserializer)?
            .map(
                |TableReference {
                     catalog,
                     schema,
                     table,
                 }| {
                    [catalog, schema, table]
                        .into_iter()
                        .filter_map(|s| s.filter(|x| !x.is_empty()))
                        .collect::<Vec<_>>()
                        .join(".")
                },
            )
            .filter(|s| !s.is_empty()))
    }

    pub fn serialize<S>(
        table_ref: &Option<String>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let Some(table_ref) = table_ref {
            let parts: Vec<&str> =
                table_ref.split('.').filter(|p| !p.is_empty()).collect();
            if parts.len() > 3 {
                return Err(serde::ser::Error::custom(format!(
                    "Invalid table reference: {table_ref}"
                )));
            }
            let table_ref = if parts.len() == 3 {
                TableReference {
                    catalog: Some(parts[0].to_string()),
                    schema: Some(parts[1].to_string()),
                    table: Some(parts[2].to_string()),
                }
            } else if parts.len() == 2 {
                TableReference {
                    catalog: None,
                    schema: Some(parts[0].to_string()),
                    table: Some(parts[1].to_string()),
                }
            } else if parts.len() == 1 {
                TableReference {
                    catalog: None,
                    schema: None,
                    table: Some(parts[0].to_string()),
                }
            } else {
                TableReference::default()
            };
            table_ref.serialize(serializer)
        } else {
            serializer.serialize_none()
        }
    }
}

mod bool_from_int {
    use serde::{self, Deserialize, Deserializer, Serialize, Serializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<bool, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value: serde_json::Value = Deserialize::deserialize(deserializer)?;
        match value {
            serde_json::Value::Bool(b) => Ok(b),
            // Backward compatibility with Wren AI manifests
            // In the legacy manifest format generated by Wren AI, boolean values are represented as integers (0 or 1)
            serde_json::Value::Number(n) if n.is_u64() => Ok(n.as_u64().unwrap() != 0),
            _ => Err(serde::de::Error::custom("invalid type for boolean")),
        }
    }

    pub fn serialize<S>(value: &bool, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Serialize::serialize(value, serializer)
    }
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct Column {
    pub name: String,
    pub r#type: String,
    #[serde(default)]
    pub relationship: Option<String>,
    #[serde(default, with = "bool_from_int")]
    pub is_calculated: bool,
    #[serde(default, with = "bool_from_int")]
    pub not_null: bool,
    #[serde_as(as = "NoneAsEmptyString")]
    #[serde(default)]
    pub expression: Option<String>,
    #[serde(default, with = "bool_from_int")]
    pub is_hidden: bool,
}

#[derive(Serialize, Deserialize, Debug, Hash, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Relationship {
    pub name: String,
    pub models: Vec<String>,
    pub join_type: JoinType,
    pub condition: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JoinType {
    #[serde(alias = "one_to_one")]
    OneToOne,
    #[serde(alias = "one_to_many")]
    OneToMany,
    #[serde(alias = "many_to_one")]
    ManyToOne,
    #[serde(alias = "many_to_many")]
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
    #[serde(default, with = "bool_from_int")]
    pub cached: bool,
    pub refresh_time: Option<String>,
}

impl Metric {
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TimeGrain {
    pub name: String,
    pub ref_column: String,
    pub date_parts: Vec<TimeUnit>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone)]
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
}

impl View {
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use crate::mdl::manifest::table_reference;
    use serde_json::Serializer;

    #[test]
    fn test_table_reference_serialize() {
        [
            (
                Some("catalog.schema.table".to_string()),
                r#"{"catalog":"catalog","schema":"schema","table":"table"}"#,
            ),
            (
                Some("schema.table".to_string()),
                r#"{"catalog":null,"schema":"schema","table":"table"}"#,
            ),
            (
                Some("table".to_string()),
                r#"{"catalog":null,"schema":null,"table":"table"}"#,
            ),
            (None, "null"),
        ]
        .iter()
        .for_each(|(table_ref, expected)| {
            let mut buf = Vec::new();
            table_reference::serialize(table_ref, &mut Serializer::new(&mut buf))
                .unwrap();
            assert_eq!(String::from_utf8(buf).unwrap(), *expected);
        });
    }
}
