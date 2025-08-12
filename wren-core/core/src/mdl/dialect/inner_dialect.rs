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
use datafusion::sql::sqlparser::ast::{self, ExtractSyntax, WindowFrameBound};
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
                if args.len() != 2 {
                    return plan_err!(
                        "date_part requires exactly 2 arguments, found {}",
                        args.len()
                    );
                }
                Ok(Some(ast::Expr::Extract {
                    field: datetime_field_from_expr(&args[0])?,
                    syntax: ExtractSyntax::From,
                    expr: Box::new(unparser.expr_to_sql(&args[1])?),
                }))
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
        match func_name {
            "cume_dist" | "dense_rank" | "first_value" | "lag" | "last_value"
            | "lead" | "nth_value" | "ntile" | "percent_rank" | "percentile_cont"
            | "percentile_disc" | "rank" | "row_number" | "st_clusterdbscan" => false,
            _ => true,
        }
    }
}

fn datetime_field_from_expr(expr: &Expr) -> Result<ast::DateTimeField> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)))
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s))) => {
            Ok(datetime_field_from_str(s)?)
        }
        _ => plan_err!("Invalid argument type for datetime field. Expected UTF8 string."),
    }
}

fn datetime_field_from_str(s: &str) -> Result<ast::DateTimeField> {
    match s.to_uppercase().as_str() {
        "YEAR" => Ok(ast::DateTimeField::Year),
        "YEARS" => Ok(ast::DateTimeField::Years),
        "QUARTER" => Ok(ast::DateTimeField::Quarter),
        "MONTH" => Ok(ast::DateTimeField::Month),
        "MONTHS" => Ok(ast::DateTimeField::Months),
        "WEEK" => Ok(ast::DateTimeField::Week(None)),
        "WEEKS" => Ok(ast::DateTimeField::Weeks),
        "DAY" => Ok(ast::DateTimeField::Day),
        "DAYOFWEEK" => Ok(ast::DateTimeField::DayOfWeek),
        "DAYOFYEAR" => Ok(ast::DateTimeField::DayOfYear),
        "DAYS" => Ok(ast::DateTimeField::Days),
        "DATE" => Ok(ast::DateTimeField::Date),
        "DATETIME" => Ok(ast::DateTimeField::Datetime),
        "HOUR" => Ok(ast::DateTimeField::Hour),
        "HOURS" => Ok(ast::DateTimeField::Hours),
        "MINUTE" => Ok(ast::DateTimeField::Minute),
        "MINUTES" => Ok(ast::DateTimeField::Minutes),
        "SECOND" => Ok(ast::DateTimeField::Second),
        "SECONDS" => Ok(ast::DateTimeField::Seconds),
        "CENTURY" => Ok(ast::DateTimeField::Century),
        "DECADE" => Ok(ast::DateTimeField::Decade),
        "DOW" => Ok(ast::DateTimeField::Dow),
        "DOY" => Ok(ast::DateTimeField::Doy),
        "EPOCH" => Ok(ast::DateTimeField::Epoch),
        "ISODOW" => Ok(ast::DateTimeField::Isodow),
        "ISOWEEK" => Ok(ast::DateTimeField::IsoWeek),
        "ISOYEAR" => Ok(ast::DateTimeField::Isoyear),
        "JULIAN" => Ok(ast::DateTimeField::Julian),
        "MICROSECOND" => Ok(ast::DateTimeField::Microsecond),
        "MICROSECONDS" => Ok(ast::DateTimeField::Microseconds),
        "MILLENIUM" => Ok(ast::DateTimeField::Millenium),
        "MILLENNIUM" => Ok(ast::DateTimeField::Millennium),
        "MILLISECOND" => Ok(ast::DateTimeField::Millisecond),
        "MILLISECONDS" => Ok(ast::DateTimeField::Milliseconds),
        "NANOSECOND" => Ok(ast::DateTimeField::Nanosecond),
        "NANOSECONDS" => Ok(ast::DateTimeField::Nanoseconds),
        "TIME" => Ok(ast::DateTimeField::Time),
        "TIMEZONE" => Ok(ast::DateTimeField::Timezone),
        "TIMEZONE_ABBR" => Ok(ast::DateTimeField::TimezoneAbbr),
        "TIMEZONE_HOUR" => Ok(ast::DateTimeField::TimezoneHour),
        "TIMEZONE_MINUTE" => Ok(ast::DateTimeField::TimezoneMinute),
        "TIMEZONE_REGION" => Ok(ast::DateTimeField::TimezoneRegion),
        "NODATETIME" => Ok(ast::DateTimeField::NoDateTime),
        _ => {
            let ident = ast::Ident::new(s);
            Ok(ast::DateTimeField::Custom(ident))
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
