use std::{
    collections::{HashMap, HashSet},
    ops::ControlFlow,
    sync::Arc,
};

use datafusion::{
    common::{plan_err, Result, Spans},
    error::DataFusionError,
    prelude::Expr,
    sql::{
        parser::DFParserBuilder,
        sqlparser::{
            ast::{
                self, visit_expressions, visit_expressions_mut, Array, ExprWithAlias,
                Map, MapEntry,
            },
            dialect::GenericDialect,
        },
        TableReference,
    },
};
use wren_core_base::mdl::RowLevelAccessControl;
use wren_core_base::mdl::{Model, SessionProperty};

use crate::mdl::{context::SessionPropertiesRef, Dataset, SessionStateRef};

/// Collect the required field from the condition of row level access control rules.
pub fn collect_condition(
    model: &Model,
    condition: &str,
) -> Result<(Vec<Expr>, Vec<String>)> {
    let mut conditions = HashSet::new();
    let mut session_properties: HashSet<String> = HashSet::new();
    let mut error: Option<Result<_, DataFusionError>> = None;
    let dialect = GenericDialect {};
    let mut parser = DFParserBuilder::new(condition)
        .with_dialect(&dialect)
        .build()?;
    let expr = parser.parse_expr()?;
    visit_expressions(&expr, |expr| {
        // TODO: consider CompoundIdentifier and CompoundFieldAccess
        if let ast::Expr::Identifier(ast::Ident { value, .. }) = expr {
            if !value.starts_with("@") {
                if model.get_column(value).is_none() {
                    error = Some(plan_err!(
                        "The column {} is not in the model {}",
                        value,
                        model.name()
                    ));
                    return ControlFlow::Break(());
                }
                conditions.insert(Expr::Column(datafusion::common::Column {
                    relation: Some(TableReference::bare(model.name())),
                    name: value.to_string(),
                    spans: Spans::new(),
                }));
            } else {
                let session_property = value.trim_start_matches("@").to_string();
                if !session_properties.contains(&session_property) {
                    session_properties.insert(session_property);
                }
            }
        }
        ControlFlow::Continue(())
    });

    if let Some(err) = error {
        return err;
    }

    Ok((
        conditions.into_iter().collect(),
        session_properties.into_iter().collect::<Vec<_>>(),
    ))
}

/// Build the filter expression for the row level access control rule.
pub fn build_filter_expression(
    session_state: &SessionStateRef,
    model: Arc<Model>,
    properties: &SessionPropertiesRef,
    rule: &RowLevelAccessControl,
) -> Result<Expr> {
    let RowLevelAccessControl {
        condition,
        required_properties,
        ..
    } = rule;
    let mut error: Option<Result<Expr, DataFusionError>> = None;
    let dialect = GenericDialect {};
    let mut parser = DFParserBuilder::new(condition)
        .with_dialect(&dialect)
        .build()?;
    let mut expr = parser.parse_expr()?;

    visit_expressions_mut(&mut expr, |expr| {
        if let ast::Expr::Identifier(ast::Ident { value, .. }) = expr {
            if value.starts_with("@") {
                let property_name =
                    value.trim_start_matches("@").to_string().to_lowercase();
                let Some(property_value) = properties.get(&property_name).or_else(|| {
                    required_properties
                        .iter()
                        .filter(|r| {
                            !r.required && r.name.eq_ignore_ascii_case(&property_name)
                        })
                        .map(|r| &r.default_expr)
                        .next()
                }) else {
                    error = Some(plan_err!(
                        "The session property {} is not found in the session properties",
                        property_name
                    ));
                    return ControlFlow::Break(());
                };

                let Some(property_value) = property_value else {
                    error = Some(plan_err!(
                        "The session property {} should not be null",
                        property_name
                    ));
                    return ControlFlow::Break(());
                };

                if property_value.trim().is_empty() {
                    error = Some(plan_err!(
                        "The session property {} should not be empty",
                        property_name
                    ));
                    return ControlFlow::Break(());
                }

                match parse_expr(property_value) {
                    Ok(parsed_expr) => {
                        *expr = parsed_expr.expr;
                    }
                    Err(e) => {
                        error = Some(plan_err!(
                            "The session property {} is not valid: {}",
                            property_name,
                            e
                        ));
                        return ControlFlow::Break(());
                    }
                }
            }
        }
        ControlFlow::Continue(())
    });

    if let Some(error) = error {
        return error;
    }
    let df_schema = Dataset::Model(Arc::clone(&model)).to_qualified_schema()?;
    session_state
        .read()
        .create_logical_expr(&expr.to_string(), &df_schema)
}

fn parse_expr(expr: &str) -> Result<ExprWithAlias> {
    let dialect = GenericDialect {};
    let mut parser = DFParserBuilder::new(expr).with_dialect(&dialect).build()?;
    let expr = parser.parse_expr()?;
    prevent_invalid_expr(&expr.expr)?;
    Ok(expr)
}

/// Prevent invalid expression for the session property.
/// Only literal values are allowed.
fn prevent_invalid_expr(expr: &ast::Expr) -> Result<()> {
    match &expr {
        ast::Expr::Value(_) | ast::Expr::Interval(_) => Ok(()),
        ast::Expr::Array(Array { elem, .. }) => {
            for e in elem {
                prevent_invalid_expr(e)?;
            }
            Ok(())
        }
        ast::Expr::Map(Map { entries }) => {
            for MapEntry { key, value } in entries {
                prevent_invalid_expr(key)?;
                prevent_invalid_expr(value)?;
            }
            Ok(())
        }
        ast::Expr::Dictionary(fileds) => {
            for field in fileds {
                prevent_invalid_expr(&field.value)?;
            }
            Ok(())
        }
        _ => plan_err!("The session property {} allow only literal value", expr),
    }
}

/// Validate the input headers with the required properties.
/// If the result is false, the rules are not satisfied and it should be ignored.
///
/// If the required property is not found in the headers, return an error.
/// If the required property is found in the headers, return true.
/// If the optional property is found in the headers, return true.
/// If the optional property is not found in the headers but has a default value, return true.
/// If the optional property is not found in the headers and has no default value, return false.
pub fn validate_rule(
    required_properties: &[SessionProperty],
    headers: &HashMap<String, Option<String>>,
) -> Result<bool> {
    let exists = required_properties.iter().map(|property| {
        if property.required {
            if !is_property_present(headers, &property.name) {
                return plan_err!(
                    "Row level access control property {} is required, but not found in headers",
                    property.name
                );
            }
            Ok(true)
        } else {
            let exist = is_property_present(headers, &property.name);
            if exist || property.default_expr.as_ref().is_some_and(|expr| !expr.is_empty()) {
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }).collect::<Result<Vec<_>>>()?;

    Ok(exists.iter().all(|x| *x))
}

/// Check if the property is present in the headers and not empty
/// If the property is present and not empty, return true.
fn is_property_present(
    headers: &HashMap<String, Option<String>>,
    property_name: &str,
) -> bool {
    headers
        .get(&property_name.to_lowercase())
        .map(|v| v.as_ref().is_some_and(|value| !value.is_empty()))
        .unwrap_or(false)
}

#[cfg(test)]
mod test {
    use std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    };

    use datafusion::{
        error::Result,
        prelude::{Expr, SessionContext},
        sql::unparser::Unparser,
    };
    use insta::assert_snapshot;
    use wren_core_base::mdl::{
        ColumnBuilder, ModelBuilder, RowLevelAccessControl, SessionProperty,
    };

    use crate::logical_plan::analyze::access_control::{
        collect_condition, validate_rule,
    };

    use super::build_filter_expression;

    #[test]
    pub fn test_collect_condition() -> Result<()> {
        let model = ModelBuilder::new("model1")
            .column(ColumnBuilder::new("id", "int").build())
            .column(ColumnBuilder::new("name", "varchar").build())
            .build();

        let conditions = vec![
            "id = @session_id AND name = 'test'",
            "id = @session_id /* comment */ AND name = 'test'",
            "id = @session_id \nAND name = 'test'",
        ];
        for condition in conditions {
            let (required_exprs, session_properties) =
                collect_condition(&model, condition)?;
            assert_eq!(required_exprs.len(), 2);
            let name = required_exprs
                .into_iter()
                .map(|e| e.schema_name().to_string())
                .collect::<Vec<_>>();
            let expected: HashSet<&str> =
                ["model1.name", "model1.id"].iter().cloned().collect();
            let all_match = name.iter().all(|n| expected.contains(n.as_str()));

            if !all_match {
                panic!("should be all match, but got: {:?}", name);
            }
            assert_eq!(session_properties.len(), 1);
            assert_eq!(session_properties[0], "session_id");
        }

        let condition = "not_found  = @session_id AND name = 'test'";
        match collect_condition(&model, condition) {
            Err(error)
                if error.message()
                    == "The column not_found is not in the model model1" => {}
            _ => panic!("should be error"),
        };

        Ok(())
    }

    #[test]
    pub fn test_validate_rule() -> Result<()> {
        // required property
        assert!(validate_rule(
            &[SessionProperty::new_required("session_id")],
            &build_headers(&[("session_id".to_string(), Some("1".to_string()))])
        )?);

        match validate_rule(
            &[SessionProperty::new_required("session_id")],
            &build_headers(&[("session_id".to_string(), None)]),
        ) {
            Err(error) => {
                assert_snapshot!(error.message(), @"Row level access control property session_id is required, but not found in headers");
            }
            _ => panic!("should be error"),
        }

        match validate_rule(
            &[SessionProperty::new_required("session_id")],
            &build_headers(&[("session_id".to_string(), Some("".to_string()))]),
        ) {
            Err(error) => {
                assert_snapshot!(error.message(), @"Row level access control property session_id is required, but not found in headers");
            }
            _ => panic!("should be error"),
        }

        match validate_rule(
            &[SessionProperty::new_required("session_id")],
            &build_headers(&[]),
        ) {
            Err(error) => {
                assert_snapshot!(error.message(), @"Row level access control property session_id is required, but not found in headers");
            }
            _ => panic!("should be error"),
        }

        // optional property with default
        assert!(validate_rule(
            &[SessionProperty::new_optional(
                "session_id",
                Some("1".to_string())
            )],
            &build_headers(&[("session_id".to_string(), Some("2".to_string()))])
        )?);

        assert!(validate_rule(
            &[SessionProperty::new_optional(
                "session_id",
                Some("1".to_string())
            )],
            &build_headers(&[("session_id".to_string(), None)])
        )?);

        assert!(validate_rule(
            &[SessionProperty::new_optional(
                "session_id",
                Some("1".to_string())
            )],
            &build_headers(&[("session_id".to_string(), Some("".to_string()))])
        )?);

        assert!(validate_rule(
            &[SessionProperty::new_optional(
                "session_id",
                Some("1".to_string())
            )],
            &build_headers(&[])
        )?);

        // optional property without default
        assert!(validate_rule(
            &[SessionProperty::new_optional("session_id", None)],
            &build_headers(&[("session_id".to_string(), Some("2".to_string()))])
        )?);

        // expected false
        assert!(!validate_rule(
            &[SessionProperty::new_optional("session_id", None)],
            &build_headers(&[("session_id".to_string(), None)])
        )?);

        // expected false
        assert!(!validate_rule(
            &[SessionProperty::new_optional("session_id", None)],
            &build_headers(&[("session_id".to_string(), Some("".to_string()))])
        )?);

        // expected false
        assert!(!validate_rule(
            &[SessionProperty::new_optional("session_id", None)],
            &build_headers(&[])
        )?);

        assert!(validate_rule(
            &[
                SessionProperty::new_required("session_id"),
                SessionProperty::new_optional("session_id_1", None),
                SessionProperty::new_optional("session_id_2", Some("1".to_string()))
            ],
            &build_headers(&[
                ("session_id".to_string(), Some("1".to_string())),
                ("session_id_1".to_string(), Some("1".to_string())),
                ("session_id_2".to_string(), Some("2".to_string())),
            ])
        )?);

        // expected false
        assert!(!validate_rule(
            &[
                SessionProperty::new_required("session_id"),
                SessionProperty::new_optional("session_id_1", None),
                SessionProperty::new_optional("session_id_2", Some("1".to_string()))
            ],
            &build_headers(&[
                ("session_id".to_string(), Some("1".to_string())),
                ("session_id_1".to_string(), None),
                ("session_id_2".to_string(), Some("2".to_string())),
            ])
        )?);

        assert!(validate_rule(
            &[
                SessionProperty::new_required("session_id"),
                SessionProperty::new_optional("session_id_1", None),
                SessionProperty::new_optional("session_id_2", Some("1".to_string()))
            ],
            &build_headers(&[
                ("session_id".to_string(), Some("1".to_string())),
                ("session_id_1".to_string(), Some("1".to_string())),
                ("session_id_2".to_string(), None),
            ])
        )?);

        match validate_rule(
            &[
                SessionProperty::new_required("session_id"),
                SessionProperty::new_optional("session_id_1", None),
                SessionProperty::new_optional("session_id_2", Some("1".to_string())),
            ],
            &build_headers(&[
                ("session_id".to_string(), None),
                ("session_id_1".to_string(), Some("1".to_string())),
                ("session_id_2".to_string(), None),
            ]),
        ) {
            Err(error) => {
                assert_snapshot!(error.message(), @"Row level access control property session_id is required, but not found in headers");
            }
            _ => panic!("should be error"),
        }

        Ok(())
    }

    fn build_headers(
        field: &[(String, Option<String>)],
    ) -> HashMap<String, Option<String>> {
        let mut headers = HashMap::new();
        for (key, value) in field {
            headers.insert(key.clone(), value.clone());
        }
        headers
    }

    #[test]
    pub fn test_build_filter_expression() -> Result<()> {
        let ctx = SessionContext::new();
        let state = ctx.state_ref();
        let model = ModelBuilder::new("m1")
            .column(ColumnBuilder::new("id", "int").build())
            .column(ColumnBuilder::new("name", "varchar").build())
            .build();

        let headers = Arc::new(build_headers(&[
            ("session_id".to_string(), Some("1".to_string())),
            ("session_name".to_string(), Some("'test'".to_string())),
        ]));

        let rule = RowLevelAccessControl {
            condition: "id = @session_id AND name = @session_name".to_string(),
            required_properties: vec![
                SessionProperty::new_required("session_id"),
                SessionProperty::new_required("session_name"),
            ],
            name: "test".to_string(),
        };

        let expr = build_filter_expression(&state, Arc::clone(&model), &headers, &rule)?;
        assert_snapshot!(expr_to_sql(&expr)?, @"m1.id = 1 AND m1.\"name\" = 'test'");

        let rule = RowLevelAccessControl {
            condition: "id = @not_found AND name = @session_name".to_string(),
            required_properties: vec![
                SessionProperty::new_required("session_id"),
                SessionProperty::new_required("session_name"),
            ],
            name: "test".to_string(),
        };

        match build_filter_expression(&state, Arc::clone(&model), &headers, &rule) {
            Err(error) => {
                assert_snapshot!(error.to_string(), @"Error during planning: The session property not_found is not found in the session properties");
            }
            _ => panic!("should be error"),
        }

        let rule = RowLevelAccessControl {
            condition: "id = @session_id AND name = @session_name".to_string(),
            required_properties: vec![
                SessionProperty::new_required("session_id"),
                SessionProperty::new_required("session_name"),
            ],
            name: "test".to_string(),
        };

        let headers = Arc::new(build_headers(&[(
            "session_id".to_string(),
            Some("1".to_string()),
        )]));
        match build_filter_expression(&state, Arc::clone(&model), &headers, &rule) {
            Err(error) => {
                assert_snapshot!(error.to_string(), @"Error during planning: The session property session_name is not found in the session properties");
            }
            _ => panic!("should be error"),
        }

        let rule = RowLevelAccessControl {
            condition: "id = @session_id AND name = @session_name".to_string(),
            required_properties: vec![
                SessionProperty::new_required("session_id"),
                SessionProperty::new_optional("session_name", Some("'test'".to_string())),
            ],
            name: "test".to_string(),
        };

        let headers = Arc::new(build_headers(&[(
            "session_id".to_string(),
            Some("1".to_string()),
        )]));

        let expr = build_filter_expression(&state, Arc::clone(&model), &headers, &rule)?;
        assert_snapshot!(expr_to_sql(&expr)?, @"m1.id = 1 AND m1.\"name\" = 'test'");

        Ok(())
    }

    fn expr_to_sql(expr: &Expr) -> Result<String> {
        let unparser = Unparser::default().with_pretty(true);
        unparser.expr_to_sql(expr).map(|sql| sql.to_string())
    }

    #[test]
    pub fn test_match_case_insensitive() -> Result<()> {
        let ctx = SessionContext::new();
        let state = ctx.state_ref();
        let model = ModelBuilder::new("m1")
            .column(ColumnBuilder::new("id", "int").build())
            .column(ColumnBuilder::new("name", "varchar").build())
            .build();

        let headers: Arc<HashMap<String, Option<String>>> = Arc::new(build_headers(&[
            ("session_id".to_string(), Some("1".to_string())),
            ("session_name".to_string(), Some("'test'".to_string())),
        ]));

        let rule = RowLevelAccessControl {
            condition: "id = @session_id AND name = @SESSION_NAME".to_string(),
            required_properties: vec![
                SessionProperty::new_required("SESSION_ID"),
                SessionProperty::new_required("session_name"),
            ],
            name: "test".to_string(),
        };

        let expr = build_filter_expression(&state, Arc::clone(&model), &headers, &rule)?;
        assert_snapshot!(expr_to_sql(&expr)?, @"m1.id = 1 AND m1.\"name\" = 'test'");
        Ok(())
    }

    #[test]
    pub fn test_property_value() -> Result<()> {
        let ctx = SessionContext::new();
        let state = ctx.state_ref();
        let model = ModelBuilder::new("m1")
            .column(ColumnBuilder::new("id", "int").build())
            .column(ColumnBuilder::new("name", "varchar").build())
            .build();

        let rule = RowLevelAccessControl {
            condition: "id = @session_id".to_string(),
            required_properties: vec![SessionProperty::new_required("SESSION_ID")],
            name: "test".to_string(),
        };

        let valid_values = vec![
            "1",
            "'aaa'",
            "1.0",
            "true",
            "false",
            "[1,2,3]",
            "{'key': 'value'}",
            "{key: 'value'}",
            "INTERVAL '1' YEAR",
        ];

        for value in valid_values {
            let headers: Arc<HashMap<String, Option<String>>> = Arc::new(build_headers(
                &[("session_id".to_string(), Some(value.to_string()))],
            ));

            let expr =
                build_filter_expression(&state, Arc::clone(&model), &headers, &rule)?;
            expr_to_sql(&expr)?;
        }

        let invalid_values = vec![
            "1 + 1",
            "upper('aaa')",
            "(select 1)",
            "1 or 1",
            "aaa",
            "is null",
            "is not null",
            "case when 1 then 1 else 2 end",
            "[upper('aaa'), upper('aaa')]",
            "{'key': upper('aaa')}",
            "{ key: upper('aaa') }",
        ];

        for value in invalid_values {
            let headers: Arc<HashMap<String, Option<String>>> = Arc::new(build_headers(
                &[("session_id".to_string(), Some(value.to_string()))],
            ));

            match build_filter_expression(&state, Arc::clone(&model), &headers, &rule) {
                Err(_) => {}
                _ => panic!(
                    "should be error: {}",
                    &headers.get("session_id").unwrap().as_ref().unwrap()
                ),
            }
        }
        Ok(())
    }
}
