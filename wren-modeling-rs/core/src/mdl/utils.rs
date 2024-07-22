use std::collections::{BTreeSet, VecDeque};
use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::common::{plan_err, Column, DFSchema};
use datafusion::error::Result;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::sql::sqlparser::ast::Expr::{CompoundIdentifier, Identifier};
use datafusion::sql::sqlparser::ast::{visit_expressions, visit_expressions_mut, Ident};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use petgraph::algo::is_cyclic_directed;
use petgraph::{EdgeType, Graph};

use crate::logical_plan::utils::from_qualified_name;
use crate::mdl::manifest::Model;
use crate::mdl::{AnalyzedWrenMDL, ColumnReference, Dataset, SessionStateRef};

pub fn to_expr_queue(column: Column) -> VecDeque<String> {
    column.name.split('.').map(String::from).collect()
}

pub fn is_dag<'a, N: 'a, E: 'a, Ty, Ix>(g: &'a Graph<N, E, Ty, Ix>) -> bool
where
    Ty: EdgeType,
    Ix: petgraph::graph::IndexType,
{
    g.is_directed() && !is_cyclic_directed(g)
}

pub fn collect_identifiers(expr: &str) -> Result<BTreeSet<Column>> {
    let wrapped = format!("select {}", expr);
    let parsed = Parser::parse_sql(&GenericDialect {}, &wrapped)?;
    let statement = parsed[0].clone();
    let mut visited: BTreeSet<Column> = BTreeSet::new();

    visit_expressions(&statement, |expr| {
        match expr {
            Identifier(id) => {
                visited.insert(Column::from(id.value.clone()));
            }
            CompoundIdentifier(ids) => {
                let name = ids
                    .iter()
                    .map(|id| id.value.clone())
                    .collect::<Vec<String>>()
                    .join(".");
                visited.insert(Column::new_unqualified(name));
            }
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });
    Ok(visited)
}

/// Create the Logical Expr for the calculated field
pub fn create_wren_calculated_field_expr(
    column_rf: ColumnReference,
    analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
    session_state: SessionStateRef,
) -> Result<Expr> {
    if !column_rf.column.is_calculated {
        return plan_err!("Column is not calculated: {}", column_rf.column.name);
    }
    let qualified_col = from_qualified_name(
        &analyzed_wren_mdl.wren_mdl,
        column_rf.dataset.name(),
        column_rf.column.name(),
    );
    let Some(required_fields) = analyzed_wren_mdl
        .lineage
        .required_fields_map
        .get(&qualified_col)
    else {
        return plan_err!("Required fields not found for {}", qualified_col);
    };

    // collect all required models.
    let models = required_fields
        .iter()
        .map(|c| &c.relation)
        .filter(|r| r.is_some())
        .map(|r| r.clone().unwrap().table().to_string())
        .collect::<BTreeSet<_>>() // Collect into a BTreeSet to remove duplicates
        .into_iter() // Convert BTreeSet back into an iterator
        .map(|m| m.to_string())
        .collect::<Vec<String>>();
    // Remove all relationship fields from the expression. Only keep the target expression and its source table.
    let expr = column_rf.column.expression.clone().unwrap();
    let session_state = session_state.read();
    let mut expr = session_state.sql_to_expr(
        &expr,
        session_state.config_options().sql_parser.dialect.as_str(),
    )?;
    visit_expressions_mut(&mut expr, |e| {
        if let CompoundIdentifier(ids) = e {
            let name_size = ids.len();
            if name_size > 2 {
                let slice = &ids[name_size - 2..name_size];
                *e = CompoundIdentifier(slice.to_vec());
            }
        }
        ControlFlow::<()>::Continue(())
    });

    let Some(schema) = models
        .into_iter()
        .map(|m| analyzed_wren_mdl.wren_mdl().get_model(&m))
        .filter(|m| m.is_some())
        .map(|m| Dataset::Model(m.unwrap()))
        .map(|m| m.to_qualified_schema())
        .reduce(|acc, schema| acc?.join(&schema?))
        .transpose()?
    else {
        return plan_err!("Error for creating schemas: {}", qualified_col);
    };
    session_state.create_logical_expr(&expr.to_string(), &schema)
}

/// Create the Logical Expr for the remote column.
pub(crate) fn create_remote_expr_for_model(
    expr: &str,
    model: Arc<Model>,
    analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
    session_state: SessionStateRef,
) -> Result<Expr> {
    let dataset = Dataset::Model(model);
    let schema = dataset.to_remote_schema(
        Some(analyzed_wren_mdl.wren_mdl().get_register_tables()),
        Arc::clone(&session_state),
    )?;
    let session_state = session_state.read();
    session_state.create_logical_expr(
        qualified_expr(expr, &schema, &session_state)?.as_str(),
        &schema,
    )
}

/// Create the Logical Expr for the remote column.
pub(crate) fn create_wren_expr_for_model(
    expr: &str,
    model: Arc<Model>,
    session_state: SessionStateRef,
) -> Result<Expr> {
    let dataset = Dataset::Model(model);
    let schema = dataset.to_qualified_schema()?;
    let session_state = session_state.read();
    session_state.create_logical_expr(
        qualified_expr(expr, &schema, &session_state)?.as_str(),
        &schema,
    )
}

/// Add the table prefix for the column expression if it can be resolved by the schema.
fn qualified_expr(
    expr: &str,
    schema: &DFSchema,
    session_state: &SessionState,
) -> Result<String> {
    let mut expr = session_state.sql_to_expr(
        expr,
        session_state.config_options().sql_parser.dialect.as_str(),
    )?;
    visit_expressions_mut(&mut expr, |e| {
        if let Identifier(id) = e {
            if let Ok((Some(qualifier), _)) =
                schema.qualified_field_with_unqualified_name(&id.value)
            {
                let mut parts: Vec<_> =
                    qualifier.to_vec().into_iter().map(Ident::new).collect();
                parts.push(id.clone());
                *e = CompoundIdentifier(parts);
            }
        }
        ControlFlow::<()>::Continue(())
    });
    Ok(expr.to_string())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;

    use datafusion::error::Result;
    use datafusion::prelude::SessionContext;

    use crate::logical_plan::utils::from_qualified_name;
    use crate::mdl::manifest::Manifest;
    use crate::mdl::AnalyzedWrenMDL;

    #[test]
    fn test_create_wren_expr() -> Result<()> {
        let test_data: PathBuf =
            [env!("CARGO_MANIFEST_DIR"), "tests", "data", "mdl.json"]
                .iter()
                .collect();
        let mdl_json = fs::read_to_string(test_data.as_path()).unwrap();
        let mdl = serde_json::from_str::<Manifest>(&mdl_json).unwrap();
        let analyzed_mdl = Arc::new(AnalyzedWrenMDL::analyze(mdl)?);
        let ctx = SessionContext::new();
        let column_rf = analyzed_mdl
            .wren_mdl
            .qualified_references
            .get(&from_qualified_name(
                &analyzed_mdl.wren_mdl,
                "orders",
                "customer_name",
            ))
            .unwrap();
        let expr = super::create_wren_calculated_field_expr(
            column_rf.clone(),
            analyzed_mdl.clone(),
            ctx.state_ref(),
        )?;
        assert_eq!(expr.to_string(), "customer.name");
        Ok(())
    }

    #[test]
    fn test_create_wren_expr_non_relationship() -> Result<()> {
        let test_data: PathBuf =
            [env!("CARGO_MANIFEST_DIR"), "tests", "data", "mdl.json"]
                .iter()
                .collect();
        let mdl_json = fs::read_to_string(test_data.as_path()).unwrap();
        let mdl = serde_json::from_str::<Manifest>(&mdl_json).unwrap();
        let analyzed_mdl = Arc::new(AnalyzedWrenMDL::analyze(mdl)?);
        let ctx = SessionContext::new();
        let column_rf = analyzed_mdl
            .wren_mdl
            .qualified_references
            .get(&from_qualified_name(
                &analyzed_mdl.wren_mdl,
                "orders",
                "orderkey_plus_custkey",
            ))
            .unwrap();
        let expr = super::create_wren_calculated_field_expr(
            column_rf.clone(),
            analyzed_mdl,
            ctx.state_ref(),
        )?;
        assert_eq!(expr.to_string(), "orderkey + custkey");
        Ok(())
    }

    #[test]
    fn test_collect_identifiers() -> Result<()> {
        let expr = "orders.orderkey + orders.custkey";
        let result = super::collect_identifiers(expr)?;
        assert_eq!(result.len(), 2);
        assert!(result.contains(&super::Column::new_unqualified("orders.orderkey")));
        assert!(result.contains(&super::Column::new_unqualified("orders.custkey")));

        let expr = "customers.state || order_id";
        let result = super::collect_identifiers(expr)?;
        assert_eq!(result.len(), 2);
        assert!(result.contains(&super::Column::new_unqualified("customers.state")));
        assert!(result.contains(&super::Column::new_unqualified("order_id")));
        Ok(())
    }

    #[test]
    fn test_create_wren_expr_for_model() -> Result<()> {
        let test_data: PathBuf =
            [env!("CARGO_MANIFEST_DIR"), "tests", "data", "mdl.json"]
                .iter()
                .collect();
        let mdl_json = fs::read_to_string(test_data.as_path()).unwrap();
        let mdl = serde_json::from_str::<Manifest>(&mdl_json).unwrap();
        let analyzed_mdl = Arc::new(AnalyzedWrenMDL::analyze(mdl)?);
        let ctx = SessionContext::new();
        let model = analyzed_mdl.wren_mdl().get_model("customer").unwrap();
        let expr = super::create_wren_expr_for_model(
            "name",
            Arc::clone(&model),
            ctx.state_ref(),
        )?;
        assert_eq!(expr.to_string(), "customer.name");
        Ok(())
    }

    #[test]
    fn test_create_remote_expr_for_model() -> Result<()> {
        let test_data: PathBuf =
            [env!("CARGO_MANIFEST_DIR"), "tests", "data", "mdl.json"]
                .iter()
                .collect();
        let mdl_json = fs::read_to_string(test_data.as_path()).unwrap();
        let mdl = serde_json::from_str::<Manifest>(&mdl_json).unwrap();
        let analyzed_mdl = Arc::new(AnalyzedWrenMDL::analyze(mdl)?);
        let ctx = SessionContext::new();
        let model = analyzed_mdl.wren_mdl().get_model("customer").unwrap();
        let expr = super::create_remote_expr_for_model(
            "c_name",
            Arc::clone(&model),
            analyzed_mdl,
            ctx.state_ref(),
        )?;
        assert_eq!(expr.to_string(), "customer.c_name");
        Ok(())
    }
}
