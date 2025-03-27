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

use quote::quote;
use syn::{parse_macro_input, LitBool};

/// This macro generates a struct for `Manifest`
/// If python_binding is true, it will generate a `pyclass` attribute
#[proc_macro]
pub fn manifest(python_binding: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(python_binding as LitBool);
    let python_binding = if input.value {
        quote! {
            #[pyclass]
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #python_binding
        #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone)]
        #[serde(rename_all = "camelCase")]
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
    };
    proc_macro::TokenStream::from(expanded)
}

/// This macro generates an enum for `DataSource`
/// If python_binding is true, it will generate a `pyclass` attribute
#[proc_macro]
pub fn data_source(python_binding: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(python_binding as LitBool);
    let python_binding = if input.value {
        quote! {
            #[pyclass(eq, eq_int)]
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #python_binding
        #[derive(Serialize, Deserialize, Debug, Default, PartialEq, Eq, Hash, Clone, Copy)]
        #[serde(rename_all = "UPPERCASE")]
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
            #[default]
            #[serde(alias = "datafusion")]
            Datafusion,
            #[serde(alias = "duckdb")]
            DuckDB,
            #[serde(alias = "local_file")]
            LocalFile,
            #[serde(alias = "s3_file")]
            S3File,
            #[serde(alias = "gcs_file")]
            GcsFile,
            #[serde(alias = "minio_file")]
            MinioFile,
        }
    };
    proc_macro::TokenStream::from(expanded)
}

/// This macro generates a struct for `Model`
/// If python_binding is true, it will generate a `pyclass` attribute
#[proc_macro]
pub fn model(python_binding: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(python_binding as LitBool);
    let python_binding = if input.value {
        quote! {
            #[pyclass]
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #python_binding
        #[serde_as]
        #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone)]
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
    };
    proc_macro::TokenStream::from(expanded)
}

/// This macro generates a struct for `Column`
/// If python_binding is true, it will generate a `pyclass` attribute
#[proc_macro]
pub fn column(python_binding: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(python_binding as LitBool);
    let python_binding = if input.value {
        quote! {
            #[pyclass]
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #python_binding
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
            pub rls: Option<RowLevelSecurity>,
            pub cls: Option<ColumnLevelSecurity>,
        }
    };
    proc_macro::TokenStream::from(expanded)
}

/// This macro generates a struct for `Relationship`
/// If python_binding is true, it will generate a `pyclass` attribute
#[proc_macro]
pub fn relationship(python_binding: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(python_binding as LitBool);
    let python_binding = if input.value {
        quote! {
            #[pyclass]
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #python_binding
        #[serde_as]
        #[derive(Serialize, Deserialize, Debug, Hash, PartialEq, Eq)]
        #[serde(rename_all = "camelCase")]
        pub struct Relationship {
            pub name: String,
            pub models: Vec<String>,
            pub join_type: JoinType,
            pub condition: String,
        }
    };
    proc_macro::TokenStream::from(expanded)
}

/// This macro generates an enum for `JoinType`
/// If python_binding is true, it will generate a `pyclass` attribute
#[proc_macro]
pub fn join_type(python_binding: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(python_binding as LitBool);
    let python_binding = if input.value {
        quote! {
            #[pyclass(eq, eq_int)]
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #python_binding
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
    };
    proc_macro::TokenStream::from(expanded)
}

/// This macro generates a struct for `Metric`
/// If python_binding is true, it will generate a `pyclass` attribute
#[proc_macro]
pub fn metric(python_binding: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(python_binding as LitBool);
    let python_binding = if input.value {
        quote! {
            #[pyclass]
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #python_binding
        #[serde_as]
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
    };
    proc_macro::TokenStream::from(expanded)
}

/// This macro generates a struct for `TimeGrain`
/// If python_binding is true, it will generate a `pyclass` attribute
#[proc_macro]
pub fn time_grain(python_binding: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(python_binding as LitBool);
    let python_binding = if input.value {
        quote! {
            #[pyclass]
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #python_binding
        #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone)]
        #[serde(rename_all = "camelCase")]
        pub struct TimeGrain {
            pub name: String,
            pub ref_column: String,
            pub date_parts: Vec<TimeUnit>,
        }
    };
    proc_macro::TokenStream::from(expanded)
}

/// This macro generates an enum for `TimeUnit`
/// If python_binding is true, it will generate a `pyclass` attribute
#[proc_macro]
pub fn time_unit(python_binding: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(python_binding as LitBool);
    let python_binding = if input.value {
        quote! {
            #[pyclass(eq, eq_int)]
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #python_binding
        #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone)]
        pub enum TimeUnit {
            Year,
            Month,
            Day,
            Hour,
            Minute,
            Second,
        }
    };
    proc_macro::TokenStream::from(expanded)
}

/// This macro generates a struct for `View`
/// If python_binding is true, it will generate a `pyclass` attribute
#[proc_macro]
pub fn view(python_binding: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(python_binding as LitBool);
    let python_binding = if input.value {
        quote! {
            #[pyclass]
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #python_binding
        #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
        pub struct View {
            pub name: String,
            pub statement: String,
        }
    };
    proc_macro::TokenStream::from(expanded)
}

#[proc_macro]
pub fn row_level_security(python_binding: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(python_binding as LitBool);
    let python_binding = if input.value {
        quote! {
            #[pyclass]
        }
    } else {
        quote! {}
    };
    let expanded = quote! {
        #python_binding
        #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
        pub struct RowLevelSecurity {
            pub name: String,
            pub operator: RowLevelOperator,
        }
    };
    proc_macro::TokenStream::from(expanded)
}

#[proc_macro]
pub fn row_level_operator(python_binding: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(python_binding as LitBool);
    let python_binding = if input.value {
        quote! {
            #[pyclass(eq, eq_int)]
        }
    } else {
        quote! {}
    };
    let expanded = quote! {
        #python_binding
        #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
        #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
        pub enum RowLevelOperator {
            Equals,
            NotEquals,
            GreaterThan,
            LessThan,
            GreaterThanOrEquals,
            LessThanOrEquals,
            IN,
            NotIn,
            LIKE,
            NotLike,
        }
    };
    proc_macro::TokenStream::from(expanded)
}

#[proc_macro]
pub fn column_level_security(python_binding: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(python_binding as LitBool);
    let python_binding = if input.value {
        quote! {
            #[pyclass]
        }
    } else {
        quote! {}
    };
    let expanded = quote! {
        #python_binding
        #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
        pub struct ColumnLevelSecurity {
            pub name: String,
            pub operator: ColumnLevelOperator,
            pub threshold: NormalizedExpr,
        }
    };
    proc_macro::TokenStream::from(expanded)
}

#[proc_macro]
pub fn column_level_operator(python_binding: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(python_binding as LitBool);
    let python_binding = if input.value {
        quote! {
            #[pyclass(eq, eq_int)]
        }
    } else {
        quote! {}
    };
    let expanded = quote! {
        #python_binding
        #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
        #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
        pub enum ColumnLevelOperator {
            Equals,
            NotEquals,
            GreaterThan,
            LessThan,
            GreaterThanOrEquals,
            LessThanOrEquals,
        }
    };
    proc_macro::TokenStream::from(expanded)
}

#[proc_macro]
pub fn normalized_expr(python_binding: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(python_binding as LitBool);
    let python_binding = if input.value {
        quote! {
            #[pyclass]
        }
    } else {
        quote! {}
    };
    let expanded = quote! {
        #python_binding
        #[derive(SerializeDisplay, DeserializeFromStr, Debug, PartialEq, Eq, Hash)]
        pub struct NormalizedExpr {
            pub value: String,
            #[serde_with(alias = "type")]
            pub data_type: NormalizedExprType,
        }
    };
    proc_macro::TokenStream::from(expanded)
}

#[proc_macro]
pub fn normalized_expr_type(python_binding: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(python_binding as LitBool);
    let python_binding = if input.value {
        quote! {
            #[pyclass(eq, eq_int)]
        }
    } else {
        quote! {}
    };
    let expanded = quote! {
        #python_binding
        #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
        #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
        pub enum NormalizedExprType {
            Numeric,
            String,
        }
    };
    proc_macro::TokenStream::from(expanded)
}
