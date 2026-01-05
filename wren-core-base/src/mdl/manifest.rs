use std::error::Error;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;

#[cfg(not(feature = "python-binding"))]
#[allow(deprecated)]
mod manifest_impl {
    use crate::mdl::manifest::bool_from_int;
    use crate::mdl::manifest::table_reference;
    use manifest_macro::{
        column, column_level_access_control, column_level_operator, column_level_security,
        data_source, join_type, manifest, metric, model, normalized_expr, normalized_expr_type,
        relationship, row_level_access_control, row_level_operator, row_level_security,
        session_property, time_grain, time_unit, view,
    };
    use serde::{Deserialize, Serialize};
    use serde_with::serde_as;
    use serde_with::DeserializeFromStr;
    use serde_with::NoneAsEmptyString;
    use serde_with::SerializeDisplay;
    use std::sync::Arc;
    manifest!(false);
    data_source!(false);
    model!(false);
    column!(false);
    relationship!(false);
    metric!(false);
    view!(false);
    join_type!(false);
    time_grain!(false);
    time_unit!(false);
    row_level_access_control!(false);
    column_level_access_control!(false);
    session_property!(false);
    row_level_security!(false);
    row_level_operator!(false);
    column_level_security!(false);
    normalized_expr!(false);
    normalized_expr_type!(false);
    column_level_operator!(false);
}

#[cfg(feature = "python-binding")]
#[allow(deprecated)]
mod manifest_impl {
    use crate::mdl::manifest::bool_from_int;
    use crate::mdl::manifest::table_reference;
    use manifest_macro::{
        column, column_level_access_control, column_level_operator, column_level_security,
        data_source, join_type, manifest, metric, model, normalized_expr, normalized_expr_type,
        relationship, row_level_access_control, row_level_operator, row_level_security,
        session_property, time_grain, time_unit, view,
    };
    use pyo3::pyclass;
    use serde::{Deserialize, Serialize};
    use serde_with::serde_as;
    use serde_with::DeserializeFromStr;
    use serde_with::NoneAsEmptyString;
    use serde_with::SerializeDisplay;
    use std::sync::Arc;

    data_source!(true);
    model!(true);
    column!(true);
    relationship!(true);
    metric!(true);
    view!(true);
    join_type!(true);
    time_grain!(true);
    time_unit!(true);
    manifest!(true);
    row_level_access_control!(true);
    column_level_access_control!(true);
    session_property!(true);
    row_level_security!(true);
    row_level_operator!(true);
    column_level_security!(true);
    normalized_expr!(true);
    normalized_expr_type!(true);
    column_level_operator!(true);
}

pub use crate::mdl::manifest::manifest_impl::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedDataSourceError {
    pub message: String,
}

impl ParsedDataSourceError {
    pub fn new(msg: &str) -> ParsedDataSourceError {
        ParsedDataSourceError {
            message: msg.to_string(),
        }
    }
}

impl Display for ParsedDataSourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ParsedDataSourceError: {}", self.message)
    }
}

impl Error for ParsedDataSourceError {
    #[allow(deprecated)]
    fn description(&self) -> &str {
        &self.message
    }
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
            DataSource::LocalFile => write!(f, "LOCAL_FILE"),
            DataSource::S3File => write!(f, "S3_FILE"),
            DataSource::GcsFile => write!(f, "GCS_FILE"),
            DataSource::MinioFile => write!(f, "MINIO_FILE"),
            DataSource::Oracle => write!(f, "ORACLE"),
            DataSource::Athena => write!(f, "ATHENA"),
            DataSource::Redshift => write!(f, "REDSHIFT"),
            DataSource::Databricks => write!(f, "DATABRICKS"),
            DataSource::Spark => write!(f, "SPARK"),
        }
    }
}

impl FromStr for DataSource {
    type Err = ParsedDataSourceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "BIGQUERY" => Ok(DataSource::BigQuery),
            "CLICKHOUSE" => Ok(DataSource::Clickhouse),
            "CANNER" => Ok(DataSource::Canner),
            "TRINO" => Ok(DataSource::Trino),
            "MSSQL" => Ok(DataSource::MSSQL),
            "MYSQL" => Ok(DataSource::MySQL),
            "POSTGRES" => Ok(DataSource::Postgres),
            "SNOWFLAKE" => Ok(DataSource::Snowflake),
            "DATAFUSION" => Ok(DataSource::Datafusion),
            "DUCKDB" => Ok(DataSource::DuckDB),
            "LOCAL_FILE" => Ok(DataSource::LocalFile),
            "S3_FILE" => Ok(DataSource::S3File),
            "GCS_FILE" => Ok(DataSource::GcsFile),
            "MINIO_FILE" => Ok(DataSource::MinioFile),
            "ORACLE" => Ok(DataSource::Oracle),
            "ATHENA" => Ok(DataSource::Athena),
            "REDSHIFT" => Ok(DataSource::Redshift),
            "DATABRICKS" => Ok(DataSource::Databricks),
            "SPARK" => Ok(DataSource::Spark),
            _ => Err(ParsedDataSourceError::new(&format!(
                "Unknown data source: {}",
                s
            ))),
        }
    }
}

mod table_reference {
    use serde::{self, Deserialize, Deserializer, Serialize, Serializer};

    use crate::mdl::utils::{parse_identifiers_normalized, quote_identifier};

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
                        .filter_map(|s| {
                            s.filter(|x| !x.is_empty())
                                .map(|x| quote_identifier(&x).to_string())
                        })
                        .collect::<Vec<_>>()
                        .join(".")
                },
            )
            .filter(|s| !s.is_empty()))
    }

    pub fn serialize<S>(table_ref: &Option<String>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let Some(table_ref) = table_ref {
            let parts: Vec<String> =
                parse_identifiers_normalized(table_ref, false).map_err(|e| {
                    serde::ser::Error::custom(format!(
                        "Failed to parse table reference: {table_ref}, error: {e}"
                    ))
                })?;
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

impl Model {
    /// Physical columns are columns that can be selected from the model.
    /// All physical columns are visible columns, but not all visible columns are physical columns
    /// e.g. columns that are not a relationship column
    pub fn get_physical_columns(&self, show_visible_only: bool) -> Vec<Arc<Column>> {
        if show_visible_only {
            self.get_visible_columns()
                .filter(|c| c.relationship.is_none())
                .map(|c| Arc::clone(&c))
                .collect()
        } else {
            self.columns
                .iter()
                .filter(|c| c.relationship.is_none())
                .map(Arc::clone)
                .collect()
        }
    }

    /// Return the name of the model
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the iterator of all visible columns
    pub fn get_visible_columns(&self) -> impl Iterator<Item = Arc<Column>> + '_ {
        self.columns.iter().filter(|f| !f.is_hidden).map(Arc::clone)
    }

    /// Get the specified visible column by name
    pub fn get_visible_column(&self, column_name: &str) -> Option<Arc<Column>> {
        self.get_visible_columns()
            .find(|c| c.name == column_name)
            .map(|c| Arc::clone(&c))
    }

    pub fn get_column(&self, column_name: &str) -> Option<Arc<Column>> {
        self.columns
            .iter()
            .find(|c| c.name == column_name)
            .map(Arc::clone)
    }

    /// Return the primary key of the model
    pub fn primary_key(&self) -> Option<&str> {
        self.primary_key.as_deref()
    }

    /// Return the table reference of the model
    pub fn table_reference(&self) -> &str {
        self.table_reference.as_deref().unwrap_or("")
    }

    pub fn row_level_access_controls(&self) -> &[Arc<RowLevelAccessControl>] {
        &self.row_level_access_controls
    }
}

impl PartialOrd for Model {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Model {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name.cmp(&other.name)
    }
}

impl Column {
    /// Return the name of the column
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the expression of the column
    pub fn expression(&self) -> Option<&str> {
        self.expression.as_deref()
    }

    pub fn column_level_access_control(&self) -> Option<Arc<ColumnLevelAccessControl>> {
        if let Some(ref cla) = &self.column_level_access_control {
            Some(Arc::clone(cla))
        } else {
            None
        }
    }
}

impl Metric {
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl View {
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl SessionProperty {
    pub fn normalized_name(&self) -> &str {
        &self.normalized_name
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
            table_reference::serialize(table_ref, &mut Serializer::new(&mut buf)).unwrap();
            assert_eq!(String::from_utf8(buf).unwrap(), *expected);
        });
    }

    #[test]
    fn test_case_sensitive() {
        let table_ref = Some(r#""Catalog"."Schema"."Table""#.to_string());
        let mut buf = Vec::new();
        table_reference::serialize(&table_ref, &mut Serializer::new(&mut buf)).unwrap();
        let serialized = String::from_utf8(buf).unwrap();
        assert_eq!(
            serialized,
            r#"{"catalog":"Catalog","schema":"Schema","table":"Table"}"#
        );
    }
}
