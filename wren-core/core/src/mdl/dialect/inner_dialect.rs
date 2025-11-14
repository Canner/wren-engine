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

use std::sync::Arc;

use crate::mdl::dialect::utils::scalar_function_to_sql_internal;
use crate::mdl::function::dialect::bigquery::{
    bigquery_aggregate_functions, bigquery_scalar_functions, bigquery_window_functions,
};
use crate::mdl::function::{aggregate_functions, scalar_functions, window_functions};
use crate::mdl::manifest::DataSource;
use datafusion::common::{plan_err, Result};
use datafusion::logical_expr::sqlparser::keywords::ALL_KEYWORDS;
use datafusion::logical_expr::{Expr, LogicalPlan};

use datafusion::scalar::ScalarValue;
use datafusion::sql::sqlparser::ast::{
    self, ExtractSyntax, Function, Ident, ObjectName, ObjectNamePart, WindowFrameBound,
};
use datafusion::sql::unparser::ast::{
    RelationBuilder, TableFactorBuilder, TableFunctionRelationBuilder,
};
use datafusion::sql::unparser::dialect::DateFieldExtractStyle;
use datafusion::sql::unparser::Unparser;
use regex::Regex;

/// [InnerDialect] is a trait that defines the methods that for dialect-specific SQL generation.
pub trait InnerDialect: Send + Sync {
    /// This method is used to override the SQL generation for scalar functions.
    /// If the function is not rewritten, it should return `None`.
    fn scalar_function_to_sql_overrides(
        &self,
        _unparser: &Unparser,
        _function_name: &str,
        _args: &[Expr],
    ) -> Result<Option<ast::Expr>> {
        Ok(None)
    }

    /// A wrapper for [datafusion::sql::unparser::dialect::Dialect::unnest_as_table_factor].
    fn unnest_as_table_factor(&self) -> bool {
        false
    }

    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        None
    }

    fn col_alias_overrides(&self, _alias: &str) -> Result<Option<String>> {
        Ok(None)
    }

    fn window_func_support_window_frame(
        &self,
        _func_name: &str,
        _start_bound: &WindowFrameBound,
        _end_bound: &WindowFrameBound,
    ) -> bool {
        true
    }

    fn to_unicode_string_literal(&self, _s: &str) -> Option<ast::Expr> {
        None
    }

    fn unparse_unnest_table_factor(
        &self,
        _unnest: &datafusion::logical_expr::Unnest,
        _columns: &[ast::Ident],
        _unparser: &Unparser,
    ) -> Result<Option<datafusion::sql::unparser::ast::TableFactorBuilder>> {
        Ok(None)
    }

    fn relation_alias_overrides(
        &self,
        _relation_builder: &mut datafusion::sql::unparser::ast::RelationBuilder,
        _alias: Option<&ast::TableAlias>,
    ) -> bool {
        false
    }

    fn date_field_extract_style(&self) -> Option<DateFieldExtractStyle> {
        None
    }

    /// Define the supported UDFs for the dialect which will be registered in the execution context.
    fn supported_udfs(&self) -> Vec<Arc<datafusion::logical_expr::ScalarUDF>> {
        scalar_functions()
    }

    /// Define the supported UDAFs for the dialect which will be registered in the execution context.
    fn supported_udafs(&self) -> Vec<Arc<datafusion::logical_expr::AggregateUDF>> {
        aggregate_functions()
    }

    /// Define the supported UDWFs for the dialect which will be registered in the execution context.
    fn supported_udwfs(&self) -> Vec<Arc<datafusion::logical_expr::WindowUDF>> {
        window_functions()
    }
}

/// [get_inner_dialect] returns the suitable InnerDialect for the given data source.
pub fn get_inner_dialect(data_source: &DataSource) -> Box<dyn InnerDialect> {
    match data_source {
        DataSource::MySQL => Box::new(MySQLDialect {}),
        DataSource::BigQuery => Box::new(BigQueryDialect {}),
        DataSource::Oracle => Box::new(OracleDialect {}),
        DataSource::MSSQL => Box::new(MsSqlDialect {}),
        DataSource::Snowflake => Box::new(SnowflakeDialect {}),
        _ => Box::new(GenericDialect {}),
    }
}

/// [GenericDialect] is a dialect that doesn't have any specific SQL generation rules.
/// It follows the default DataFusion SQL generation.
pub struct GenericDialect {}

impl InnerDialect for GenericDialect {}

/// [MySQLDialect] is a dialect that overrides the SQL generation for MySQL dialect.
pub struct MySQLDialect {}

impl InnerDialect for MySQLDialect {
    fn scalar_function_to_sql_overrides(
        &self,
        unparser: &Unparser,
        function_name: &str,
        args: &[Expr],
    ) -> Result<Option<ast::Expr>> {
        match function_name {
            "btrim" => scalar_function_to_sql_internal(unparser, None, "trim", args),
            _ => Ok(None),
        }
    }

    fn date_field_extract_style(&self) -> Option<DateFieldExtractStyle> {
        Some(DateFieldExtractStyle::Extract)
    }
}

pub struct BigQueryDialect {}

impl InnerDialect for BigQueryDialect {
    fn supported_udafs(&self) -> Vec<Arc<datafusion::logical_expr::AggregateUDF>> {
        bigquery_aggregate_functions()
    }

    fn supported_udfs(&self) -> Vec<Arc<datafusion::logical_expr::ScalarUDF>> {
        bigquery_scalar_functions()
    }

    fn supported_udwfs(&self) -> Vec<Arc<datafusion::logical_expr::WindowUDF>> {
        bigquery_window_functions()
    }

    fn unnest_as_table_factor(&self) -> bool {
        true
    }

    fn col_alias_overrides(&self, alias: &str) -> Result<Option<String>> {
        // Check if alias contains any special characters not supported by BigQuery col names
        // https://cloud.google.com/bigquery/docs/schemas#flexible-column-names
        let special_chars: [char; 20] = [
            '!', '"', '$', '(', ')', '*', ',', '.', '/', ';', '?', '@', '[', '\\', ']',
            '^', '`', '{', '}', '~',
        ];

        if alias.chars().any(|c| special_chars.contains(&c)) {
            let mut encoded_name = String::new();
            for c in alias.chars() {
                if special_chars.contains(&c) {
                    encoded_name.push_str(&format!("_{}", c as u32));
                } else {
                    encoded_name.push(c);
                }
            }
            Ok(Some(encoded_name))
        } else {
            Ok(Some(alias.to_string()))
        }
    }

    fn scalar_function_to_sql_overrides(
        &self,
        unparser: &Unparser,
        function_name: &str,
        args: &[Expr],
    ) -> Result<Option<ast::Expr>> {
        match function_name {
            "date_part" => {
                if args.len() != 2 {
                    return plan_err!(
                        "date_part requires exactly 2 arguments, found {}",
                        args.len()
                    );
                }
                Ok(Some(ast::Expr::Extract {
                    field: self.datetime_field_from_expr(&args[0])?,
                    syntax: ExtractSyntax::From,
                    expr: Box::new(unparser.expr_to_sql(&args[1])?),
                }))
            }
            // DATE_DIFF(end_date, start_date, granularity)
            // https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_diff
            "date_diff" => self.transform_diff_function("DATE_DIFF", args, unparser),
            "time_diff" => self.transform_diff_function("TIME_DIFF", args, unparser),
            "timestamp_diff" => {
                self.transform_diff_function("TIMESTAMP_DIFF", args, unparser)
            }
            "datetime_diff" => {
                self.transform_diff_function("DATETIME_DIFF", args, unparser)
            }
            "now" => {
                scalar_function_to_sql_internal(unparser, None, "CURRENT_TIMESTAMP", args)
            }
            "btrim" => scalar_function_to_sql_internal(unparser, None, "trim", args),
            "get_path" => {
                scalar_function_to_sql_internal(unparser, None, "JSON_EXTRACT", args)
            }
            "as_array" => {
                if args.len() != 1 {
                    return plan_err!(
                        "AS_ARRAY requires exactly 1 argument, found {}",
                        args.len()
                    );
                }
                if let Expr::ScalarFunction(function) = &args[0] {
                    if function.func.name().eq_ignore_ascii_case("get_path") {
                        return scalar_function_to_sql_internal(
                            unparser,
                            None,
                            "JSON_EXTRACT_ARRAY",
                            &function.args,
                        );
                    }
                }

                plan_err!("AS_ARRAY requires the argument to be a GET_PATH function.")
            }
            "as_varchar" => {
                scalar_function_to_sql_internal(unparser, None, "lax_string", args)
            }
            "as_integer" => {
                scalar_function_to_sql_internal(unparser, None, "lax_int64", args)
            }
            "as_double" => {
                scalar_function_to_sql_internal(unparser, None, "lax_float64", args)
            }
            "as_boolean" => {
                scalar_function_to_sql_internal(unparser, None, "lax_bool", args)
            }
            _ => Ok(None),
        }
    }

    /// BigQuery only allow the aggregation function with window frame.
    /// Other [window functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/window-functions) are not supported.
    fn window_func_support_window_frame(
        &self,
        func_name: &str,
        _start_bound: &WindowFrameBound,
        _end_bound: &WindowFrameBound,
    ) -> bool {
        !matches!(
            func_name,
            "cume_dist"
                | "dense_rank"
                | "first_value"
                | "lag"
                | "last_value"
                | "lead"
                | "nth_value"
                | "ntile"
                | "percent_rank"
                | "percentile_cont"
                | "percentile_disc"
                | "rank"
                | "row_number"
                | "st_clusterdbscan"
        )
    }
}

impl BigQueryDialect {
    fn transform_diff_function(
        &self,
        func_name: &str,
        args: &[Expr],
        unparser: &Unparser,
    ) -> Result<Option<ast::Expr>> {
        if args.len() != 3 {
            return plan_err!(
                "{} requires exactly 3 arguments, found {}",
                func_name,
                args.len()
            );
        }
        let Expr::Literal(ScalarValue::Utf8(Some(s)), _) = args[0].clone() else {
            return plan_err!(
                "{} requires a string literal as the third argument (granularity)",
                func_name
            );
        };
        let granularity = ast::Expr::Identifier(Ident::new(
            self.datetime_field_from_str(&s)?.to_string(),
        ));
        Ok(Some(ast::Expr::Function(Function {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(func_name))]),
            args: ast::FunctionArguments::List(ast::FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![
                    unparser.expr_to_sql(&args[2]).map(|e| {
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e))
                    })?,
                    unparser.expr_to_sql(&args[1]).map(|e| {
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e))
                    })?,
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(granularity)),
                ],
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: ast::FunctionArguments::None,
            uses_odbc_syntax: false,
        })))
    }

    fn datetime_field_from_expr(&self, expr: &Expr) -> Result<ast::DateTimeField> {
        match expr {
            Expr::Literal(ScalarValue::Utf8(Some(s)), _)
            | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => {
                Ok(self.datetime_field_from_str(s)?)
            }
            _ => plan_err!(
                "Invalid argument type for datetime field. Expected UTF8 string."
            ),
        }
    }

    /// BigQuery supports only the following date part
    /// <https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#extract>
    fn datetime_field_from_str(&self, s: &str) -> Result<ast::DateTimeField> {
        let s = s.to_uppercase();
        if s.starts_with("WEEK") {
            if s.len() > 4 {
                // Parse WEEK(MONDAY) format
                if let Some(start) = s.find('(') {
                    if let Some(end) = s.find(')') {
                        let weekday = &s[start + 1..end];
                        match weekday {
                            "SUNDAY" | "MONDAY" | "TUESDAY" | "WEDNESDAY" 
                            | "THURSDAY" | "FRIDAY" | "SATURDAY" => {
                                return Ok(ast::DateTimeField::Week(Some(Ident::new(weekday))));
                            }
                            _ => return plan_err!("Invalid weekday '{}' for WEEK. Valid values are SUNDAY, MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, and SATURDAY", weekday),
                        }
                    }
                }
                return plan_err!("Invalid WEEK format '{}'. Expected WEEK(WEEKDAY)", s);
            }
            return Ok(ast::DateTimeField::Week(None));
        }
        match s.as_str() {
            "DAYOFWEEK" => Ok(ast::DateTimeField::DayOfWeek),
            "DAY" => Ok(ast::DateTimeField::Day),
            "DAYOFYEAR" => Ok(ast::DateTimeField::DayOfYear),
            "ISOWEEK" => Ok(ast::DateTimeField::IsoWeek),
            "MONTH" => Ok(ast::DateTimeField::Month),
            "QUARTER" => Ok(ast::DateTimeField::Quarter),
            "YEAR" => Ok(ast::DateTimeField::Year),
            "ISOYEAR" => Ok(ast::DateTimeField::Isoyear),
            "MICROSECOND" => Ok(ast::DateTimeField::Microsecond),
            "MILLISECOND" => Ok(ast::DateTimeField::Millisecond),
            "SECOND" => Ok(ast::DateTimeField::Second),
            "MINUTE" => Ok(ast::DateTimeField::Minute),
            "HOUR" => Ok(ast::DateTimeField::Hour),
            _ => {
                plan_err!("Unsupported date part '{}' for BIGQUERY. Valid values are: WEEK, DAYOFWEEK, DAY, DAYOFYEAR, ISOWEEK, MONTH, QUARTER, YEAR, ISOYEAR, MICROSECOND, MILLISECOND, SECOND, MINUTE, HOUR", s)
            }
        }
    }
}

pub struct OracleDialect {}

impl InnerDialect for OracleDialect {
    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        // Oracle defaults to upper case for identifiers
        let identifier_regex = Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").unwrap();
        if ALL_KEYWORDS.contains(&identifier.to_uppercase().as_str())
            || !identifier_regex.is_match(identifier)
            || non_uppercase(identifier)
        {
            Some('"')
        } else {
            None
        }
    }
}

fn non_uppercase(sql: &str) -> bool {
    let uppsercase = sql.to_uppercase();
    uppsercase != sql
}

pub struct MsSqlDialect {}

impl InnerDialect for MsSqlDialect {
    fn to_unicode_string_literal(&self, s: &str) -> Option<ast::Expr> {
        if !s.is_ascii() {
            Some(ast::Expr::value(ast::Value::NationalStringLiteral(
                s.to_string(),
            )))
        } else {
            None
        }
    }
}

pub static UNNAMED_SNOWFLAKE_FLATTEN_SUBQUERY_PREFIX: &str = "__unnamed_flatten_subquery";

pub struct SnowflakeDialect {}

impl InnerDialect for SnowflakeDialect {
    fn unnest_as_table_factor(&self) -> bool {
        true
    }

    fn unparse_unnest_table_factor(
        &self,
        unnest: &datafusion::logical_expr::Unnest,
        columns: &[ast::Ident],
        unparser: &Unparser,
    ) -> Result<Option<datafusion::sql::unparser::ast::TableFactorBuilder>> {
        let LogicalPlan::Projection(projection) = unnest.input.as_ref() else {
            return Ok(None);
        };

        if !matches!(projection.input.as_ref(), LogicalPlan::EmptyRelation(_)) {
            // It may be possible that UNNEST is used as a source for the query.
            // However, at this point, we don't yet know if it is just a single expression
            // from another source or if it's from UNNEST.
            //
            // Unnest(Projection(EmptyRelation)) denotes a case with `UNNEST([...])`,
            // which is normally safe to unnest as a table factor.
            // However, in the future, more comprehensive checks can be added here.
            return Ok(None);
        };

        let mut table_function_relation = TableFunctionRelationBuilder::default();
        let exprs = projection
            .expr
            .iter()
            .map(|e| unparser.expr_to_sql(e))
            .collect::<Result<Vec<_>>>()?;

        if exprs.len() != 1 {
            // Snowflake FLATTEN function only supports a single argument.
            return plan_err!(
                "Only support one argument for Snowflake FLATTEN, found {}",
                exprs.len()
            );
        }

        if columns.len() != 1 {
            // Snowflake FLATTEN function only supports a single output column.
            return plan_err!(
                "Only support one output column for Snowflake FLATTEN, found {}",
                columns.len()
            );
        }

        // To get the flattened result, we need to override the output columns of the FLATTEN function.
        // The 4th column corresponds to the flattened value, which we will alias to the desired output column name.
        // https://docs.snowflake.com/en/sql-reference/functions/flatten#output
        let column_alias = vec![
            unparser.new_ident_quoted_if_needs("SEQ".to_string()),
            unparser.new_ident_quoted_if_needs("KEY".to_string()),
            unparser.new_ident_quoted_if_needs("PATH".to_string()),
            unparser.new_ident_quoted_if_needs("INDEX".to_string()),
            columns[0].clone(),
            unparser.new_ident_quoted_if_needs("THIS".to_string()),
        ];

        let func_expr = ast::Expr::Function(Function {
            name: vec![Ident::new("FLATTEN")].into(),
            uses_odbc_syntax: false,
            parameters: ast::FunctionArguments::None,
            args: ast::FunctionArguments::List(ast::FunctionArgumentList {
                args: exprs
                    .into_iter()
                    .map(|e| ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)))
                    .collect(),
                duplicate_treatment: None,
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
        });
        table_function_relation.expr(func_expr);
        table_function_relation.alias(Some(
            unparser.new_table_alias(
                unparser
                    .alias_generator
                    .next(UNNAMED_SNOWFLAKE_FLATTEN_SUBQUERY_PREFIX),
                column_alias,
            ),
        ));
        Ok(Some(TableFactorBuilder::TableFunction(
            table_function_relation,
        )))
    }

    fn relation_alias_overrides(
        &self,
        relation_builder: &mut RelationBuilder,
        alias: Option<&ast::TableAlias>,
    ) -> bool {
        if let Some(TableFactorBuilder::TableFunction(rel_builder)) =
            relation_builder.relation.as_mut()
        {
            if let Some(value) = &alias {
                if let Some(alias) = rel_builder.alias.as_mut() {
                    if alias
                        .name
                        .value
                        .starts_with(UNNAMED_SNOWFLAKE_FLATTEN_SUBQUERY_PREFIX)
                        && value.columns.len() == 1
                    {
                        let mut new_columns = alias.columns.clone();
                        new_columns[4] = value.columns[0].clone();
                        let new_alias = ast::TableAlias {
                            name: value.name.clone(),
                            columns: new_columns,
                        };
                        rel_builder.alias = Some(new_alias);
                        return true;
                    }
                }
            }
        }
        false
    }
}
