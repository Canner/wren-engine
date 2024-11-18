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
use datafusion::common::{internal_err, plan_err, Result, ScalarValue};
use datafusion::logical_expr::sqlparser::ast::{Ident, Subscript};
use datafusion::logical_expr::sqlparser::keywords::ALL_KEYWORDS;
use datafusion::logical_expr::Expr;
use datafusion::sql::sqlparser::ast;
use datafusion::sql::sqlparser::ast::{Array, Value};
use datafusion::sql::unparser::dialect::{Dialect, IntervalStyle};
use datafusion::sql::unparser::Unparser;
use regex::Regex;

/// WrenDialect is a dialect for Wren engine. Handle the identifier quote style based on the
/// original Datafusion Dialect implementation but with more strict rules.
/// If the identifier isn't lowercase, it will be quoted.
pub struct WrenDialect {}

impl Dialect for WrenDialect {
    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        let identifier_regex = Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").unwrap();
        if ALL_KEYWORDS.contains(&identifier.to_uppercase().as_str())
            || !identifier_regex.is_match(identifier)
            || non_lowercase(identifier)
        {
            Some('"')
        } else {
            None
        }
    }

    fn interval_style(&self) -> IntervalStyle {
        IntervalStyle::SQLStandard
    }

    fn scalar_function_to_sql_overrides(
        &self,
        unparser: &Unparser<'_>,
        func_name: &str,
        args: &[Expr],
    ) -> Result<Option<ast::Expr>> {
        match func_name {
            "make_array" => {
                let sql = self.make_array_to_sql(args, unparser)?;
                Ok(Some(sql))
            }
            "array_element" => {
                let sql = self.array_element_to_sql(args, unparser)?;
                Ok(Some(sql))
            }
            "get_field" => self.get_fields_to_sql(args, unparser),
            "named_struct" => {
                let sql = self.named_struct_to_sql(args, unparser)?;
                Ok(Some(sql))
            }
            _ => Ok(None),
        }
    }
}

impl WrenDialect {
    fn make_array_to_sql(&self, args: &[Expr], unparser: &Unparser) -> Result<ast::Expr> {
        let args = args
            .iter()
            .map(|e| unparser.expr_to_sql(e))
            .collect::<Result<Vec<_>>>()?;
        Ok(ast::Expr::Array(Array {
            elem: args,
            named: false,
        }))
    }

    fn array_element_to_sql(
        &self,
        args: &[Expr],
        unparser: &Unparser,
    ) -> Result<ast::Expr> {
        if args.len() != 2 {
            return internal_err!("array_element must have exactly 2 arguments");
        }
        let array = unparser.expr_to_sql(&args[0])?;
        let index = unparser.expr_to_sql(&args[1])?;
        Ok(ast::Expr::Subscript {
            expr: Box::new(array),
            subscript: Box::new(Subscript::Index { index }),
        })
    }

    fn named_struct_to_sql(
        &self,
        args: &[Expr],
        unparser: &Unparser,
    ) -> Result<ast::Expr> {
        if args.is_empty() {
            return plan_err!("struct must have at least one field");
        }
        if args.len() % 2 != 0 {
            return internal_err!(
                "named_struct must have an even number of arguments or more than 0"
            );
        }
        let fields = args
            .chunks(2)
            .map(|pair| {
                let name = match &pair[0] {
                    Expr::Literal(ScalarValue::Utf8(Some(s))) => s,
                    _ => {
                        return internal_err!("named_struct field name must be a string")
                    }
                };
                let value = unparser.expr_to_sql(&pair[1])?;
                Ok(ast::DictionaryField {
                    key: self.new_ident_quoted_if_needs(name.to_string()),
                    value: Box::new(value),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ast::Expr::Dictionary(fields))
    }

    fn get_fields_to_sql(
        &self,
        args: &[Expr],
        unparser: &Unparser,
    ) -> Result<Option<ast::Expr>> {
        if args.len() != 2 {
            return internal_err!("get_fields must have exactly 2 argument");
        }

        let mut exprs = match unparser.expr_to_sql(&args[0])? {
            ast::Expr::CompoundIdentifier(exprs) => exprs,
            ast::Expr::Identifier(ident) => vec![ident],
            // If the first argument is not identifiers, unparse it as ScalarFunction
            _ => return Ok(None),
        };

        if let ast::Expr::Value(Value::SingleQuotedString(field_name)) =
            unparser.expr_to_sql(&args[1])?
        {
            exprs.extend(vec![self.new_ident_quoted_if_needs(field_name)]);
            return Ok(Some(ast::Expr::CompoundIdentifier(exprs)));
        }

        Ok(None)
    }

    fn new_ident_quoted_if_needs(&self, ident: String) -> Ident {
        let quote_style = self.identifier_quote_style(&ident);
        Ident {
            value: ident,
            quote_style,
        }
    }
}

fn non_lowercase(sql: &str) -> bool {
    let lowercase = sql.to_lowercase();
    lowercase != sql
}
