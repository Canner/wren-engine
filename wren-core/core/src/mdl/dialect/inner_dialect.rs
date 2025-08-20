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

use crate::mdl::dialect::utils::scalar_function_to_sql_internal;
use crate::mdl::manifest::DataSource;
use datafusion::common::{plan_err, Result};
use datafusion::logical_expr::sqlparser::keywords::ALL_KEYWORDS;
use datafusion::logical_expr::Expr;

use datafusion::scalar::ScalarValue;
use datafusion::sql::sqlparser::ast::{
    self, DataType, DateTimeField, Expr as AstExpr, ExtractSyntax, Function,
    FunctionArg, FunctionArgExpr, Ident, Interval, TimezoneInfo, Value, WindowFrameBound,
};
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
}

/// [get_inner_dialect] returns the suitable InnerDialect for the given data source.
pub fn get_inner_dialect(data_source: &DataSource) -> Box<dyn InnerDialect> {
    match data_source {
        DataSource::MySQL => Box::new(MySQLDialect {}),
        DataSource::BigQuery => Box::new(BigQueryDialect {}),
        DataSource::Oracle => Box::new(OracleDialect {}),
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
            "btrim" => scalar_function_to_sql_internal(unparser, "trim", args),
            _ => Ok(None),
        }
    }
}

pub struct BigQueryDialect {}

impl InnerDialect for BigQueryDialect {
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
                if args.len() != 2 && args.len() != 3 {
                    return plan_err!(
                        "date_part requires 2 or 3 arguments, found {}",
                        args.len()
                    );
                }
                // Base timestamp/datetime expression
                let mut source_expr = unparser.expr_to_sql(&args[1])?;
                // Apply timezone if provided as 3rd arg
                if args.len() == 3 {
                    if let Expr::Literal(ScalarValue::Utf8(Some(tz))) = &args[2] {
                        source_expr = AstExpr::AtTimeZone {
                            timestamp: Box::new(source_expr),
                            time_zone: TimezoneInfo::Tz(tz.clone()),
                        };
                    }
                }
                Ok(Some(ast::Expr::Extract {
                    field: self.datetime_field_from_expr(&args[0])?,
                    syntax: ExtractSyntax::From,
                    expr: Box::new(source_expr),
                }))
            }
            "date_trunc" | "datetime_trunc" | "timestamp_trunc" | "time_trunc" => {
                if args.len() != 2 {
                    return plan_err!(
                        "{} requires exactly 2 arguments, found {}",
                        function_name,
                        args.len()
                    );
                }
                Ok(Some(AstExpr::Function(Function {
                    name: ast::ObjectName(vec![Ident::new(function_name.to_uppercase())]),
                    args: vec![
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(
                            unparser.expr_to_sql(&args[1])?,
                        )),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(
                            unparser.expr_to_sql(&args[0])?,
                        )),
                    ],
                    filter: None,
                    null_treatment: None,
                    over: None,
                    distinct: false,
                    special: false,
                    order_by: Vec::new(),
                })))
            }
            "date_add" | "datetime_add" | "timestamp_add" | "time_add" | "date_sub"
            | "datetime_sub" | "timestamp_sub" | "time_sub" => {
                if args.len() != 2 {
                    return plan_err!(
                        "{} requires exactly 2 arguments, found {}",
                        function_name,
                        args.len()
                    );
                }

                let interval_expr = match &args[1] {
                    Expr::Literal(ScalarValue::IntervalDayTime(Some(interval))) => {
                        let (days, ms) = (*interval >> 32, *interval as i32);
                        let use_day_unit = matches!(function_name, "date_add" | "date_sub");
                        let (value_str, unit) = if use_day_unit {
                            (format!("{}", days), DateTimeField::Day)
                        } else {
                            (
                                format!("{}", days * 24 * 3600 * 1000 + ms as i64),
                                DateTimeField::Millisecond,
                            )
                        };
                        AstExpr::Value(Value::Interval(Interval {
                            value: Box::new(AstExpr::Value(Value::Number(value_str, false))),
                            leading_field: Some(unit),
                            leading_precision: None,
                            last_field: None,
                            fractional_seconds_precision: None,
                        }))
                    }
                    Expr::Literal(ScalarValue::IntervalYearMonth(Some(interval))) => {
                        let (years, months) = (*interval / 12, *interval % 12);
                        if function_name.starts_with("time_") {
                            return plan_err!(
                                "Cannot add/subtract YEAR/MONTH interval to/from a TIME value"
                            );
                        }
                        AstExpr::Value(Value::Interval(Interval {
                            value: Box::new(AstExpr::Value(Value::Number(
                                format!("{}", years * 12 + months),
                                false,
                            ))),
                            leading_field: Some(DateTimeField::Month),
                            leading_precision: None,
                            last_field: None,
                            fractional_seconds_precision: None,
                        }))
                    }
                    _ => return plan_err!("Invalid interval for {}", function_name),
                };

                Ok(Some(AstExpr::Function(Function {
                    name: ast::ObjectName(vec![Ident::new(function_name.to_uppercase())]),
                    args: vec![
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(
                            unparser.expr_to_sql(&args[0])?,
                        )),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(interval_expr)),
                    ],
                    filter: None,
                    null_treatment: None,
                    over: None,
                    distinct: false,
                    special: false,
                    order_by: Vec::new(),
                })))
            }
            "date_diff" | "datetime_diff" | "timestamp_diff" | "time_diff" => {
                if args.len() != 3 {
                    return plan_err!(
                        "{} requires exactly 3 arguments, found {}",
                        function_name,
                        args.len()
                    );
                }
                Ok(Some(AstExpr::Function(Function {
                    name: ast::ObjectName(vec![Ident::new(function_name.to_uppercase())]),
                    args: vec![
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(
                            unparser.expr_to_sql(&args[1])?,
                        )),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(
                            unparser.expr_to_sql(&args[2])?,
                        )),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(
                            unparser.expr_to_sql(&args[0])?,
                        )),
                    ],
                    filter: None,
                    null_treatment: None,
                    over: None,
                    distinct: false,
                    special: false,
                    order_by: Vec::new(),
                })))
            }
            "parse_date" | "parse_datetime" | "parse_timestamp" | "format_date"
            | "format_datetime" | "format_timestamp" => {
                if args.len() != 2 {
                    return plan_err!(
                        "{} requires exactly 2 arguments, found {}",
                        function_name,
                        args.len()
                    );
                }
                Ok(Some(AstExpr::Function(Function {
                    name: ast::ObjectName(vec![Ident::new(function_name.to_uppercase())]),
                    args: vec![
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(
                            unparser.expr_to_sql(&args[0])?,
                        )),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(
                            unparser.expr_to_sql(&args[1])?,
                        )),
                    ],
                    filter: None,
                    null_treatment: None,
                    over: None,
                    distinct: false,
                    special: false,
                    order_by: Vec::new(),
                })))
            }
            "current_date" | "current_datetime" | "current_timestamp" => {
                if !args.is_empty() {
                    return plan_err!(
                        "{} requires no arguments, found {}",
                        function_name,
                        args.len()
                    );
                }
                Ok(Some(AstExpr::Function(Function {
                    name: ast::ObjectName(vec![Ident::new(function_name.to_uppercase())]),
                    args: vec![],
                    filter: None,
                    null_treatment: None,
                    over: None,
                    distinct: false,
                    special: false,
                    order_by: Vec::new(),
                })))
            }
            "generate_date_array" => {
                if args.len() != 2 && args.len() != 3 {
                    return plan_err!(
                        "generate_date_array requires 2 or 3 arguments, found {}",
                        args.len()
                    );
                }
                let mut fn_args = vec![
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(unparser.expr_to_sql(&args[0])?)),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(unparser.expr_to_sql(&args[1])?)),
                ];
                if args.len() == 3 {
                    fn_args.push(FunctionArg::Unnamed(FunctionArgExpr::Expr(unparser.expr_to_sql(&args[2])?)));
                }

                Ok(Some(AstExpr::Function(Function {
                    name: ast::ObjectName(vec![Ident::new(function_name.to_uppercase())]),
                    args: fn_args,
                    filter: None,
                    null_treatment: None,
                    over: None,
                    distinct: false,
                    special: false,
                    order_by: Vec::new(),
                })))
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
    fn datetime_field_from_expr(&self, expr: &Expr) -> Result<ast::DateTimeField> {
        match expr {
            Expr::Literal(ScalarValue::Utf8(Some(s)))
            | Expr::Literal(ScalarValue::LargeUtf8(Some(s))) => {
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
                                return Ok(ast::DateTimeField::Week(Some(Ident::new(
                                    weekday,
                                ))));
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
            "HOUR" => Ok(ast::DateTimeField::Hour),
            "MINUTE" => Ok(ast::DateTimeField::Minute),
            "SECOND" => Ok(ast::DateTimeField::Second),
            "MILLISECOND" => Ok(ast::DateTimeField::Millisecond),
            "MICROSECOND" => Ok(ast::DateTimeField::Microsecond),
            _ => {
                plan_err!("Unsupported date part '{}' for BigQuery", s)
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