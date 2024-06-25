use std::collections::{BTreeSet, VecDeque};
use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::common::{internal_err, plan_err, Column};
use datafusion::error::Result;
use datafusion::logical_expr::logical_plan::tree_node::unwrap_arc;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::sql::planner::SqlToRel;
use datafusion::sql::sqlparser::ast::Expr::{CompoundIdentifier, Identifier};
use datafusion::sql::sqlparser::ast::{visit_expressions, visit_expressions_mut};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use log::debug;
use petgraph::algo::is_cyclic_directed;
use petgraph::{EdgeType, Graph};

use crate::logical_plan::context_provider::DynamicContextProvider;
use crate::logical_plan::context_provider::{RemoteContextProvider, WrenContextProvider};
use crate::logical_plan::utils::from_qualified_name;
use crate::mdl::manifest::Model;
use crate::mdl::{AnalyzedWrenMDL, ColumnReference};

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
        .collect::<Vec<String>>()
        .join(", ");

    // Remove all relationship fields from the expression. Only keep the target expression and its source table.
    let expr = column_rf.column.expression.clone().unwrap();
    let wrapped = format!("select {} from {}", expr, models);
    let parsed = Parser::parse_sql(&GenericDialect {}, &wrapped).unwrap();
    let mut statement = parsed[0].clone();
    visit_expressions_mut(&mut statement, |expr| {
        if let CompoundIdentifier(ids) = expr {
            let name_size = ids.len();
            if name_size > 2 {
                let slice = &ids[name_size - 2..name_size];
                *expr = CompoundIdentifier(slice.to_vec());
            }
        }
        ControlFlow::<()>::Continue(())
    });
    debug!("Statement: {:?}", statement.to_string());
    // Create the expression only has the table prefix. We don't need the catalog and schema prefix when planning.
    let context_provider = WrenContextProvider::new_bare(&analyzed_wren_mdl.wren_mdl)?;
    let sql_to_rel = SqlToRel::new(&context_provider);
    let plan = match sql_to_rel.sql_statement_to_plan(statement.clone()) {
        Ok(plan) => plan,
        Err(e) => return plan_err!("Error creating plan: {}", e),
    };

    let result = match plan {
        LogicalPlan::Projection(projection) => {
            if let LogicalPlan::Aggregate(aggregation) = unwrap_arc(projection.input) {
                aggregation.aggr_expr[0].clone()
            } else {
                projection.expr[0].clone()
            }
        }
        _ => return internal_err!("Unexpected plan type: {:?}", plan),
    };
    Ok(result)
}

/// Create the Logical Expr for the remote column.
/// Use [RemoteContextProvider] to provide the context for the remote column.
pub(crate) fn create_remote_expr_for_model(
    expr: &String,
    model: Arc<Model>,
    analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
) -> Result<Expr> {
    let context_provider = RemoteContextProvider::new(&analyzed_wren_mdl.wren_mdl)?;
    create_expr_for_model(
        expr,
        model,
        DynamicContextProvider::new(Box::new(context_provider)),
    )
}

/// Create the Logical Expr for the remote column.
/// Use [RemoteContextProvider] to provide the context for the remote column.
pub(crate) fn create_wren_expr_for_model(
    expr: &String,
    model: Arc<Model>,
    analyzed_wren_mdl: Arc<AnalyzedWrenMDL>,
) -> Result<Expr> {
    let context_provider = WrenContextProvider::new(&analyzed_wren_mdl.wren_mdl)?;
    let wrapped = format!(
        "select {} from {}.{}.{}",
        expr,
        analyzed_wren_mdl.wren_mdl().catalog(),
        analyzed_wren_mdl.wren_mdl().schema(),
        &model.name
    );
    let parsed = Parser::parse_sql(&GenericDialect {}, &wrapped).unwrap();
    let statement = &parsed[0];
    debug!("Statement: {:?}", statement.to_string());

    let sql_to_rel = SqlToRel::new(&context_provider);
    let plan = sql_to_rel.sql_statement_to_plan(statement.clone())?;

    match plan {
        LogicalPlan::Projection(projection) => Ok(projection.expr[0].clone()),
        _ => internal_err!("Unexpected plan type: {:?}", plan),
    }
}

/// Create the Logical Expr for the column belong to the model according to the context provider
pub(crate) fn create_expr_for_model(
    expr: &String,
    model: Arc<Model>,
    context_provider: DynamicContextProvider,
) -> Result<Expr> {
    let wrapped = format!("select {} from {}", expr, &model.table_reference);
    let parsed = Parser::parse_sql(&GenericDialect {}, &wrapped).unwrap();
    let statement = &parsed[0];

    let sql_to_rel = SqlToRel::new(&context_provider);
    let plan = sql_to_rel.sql_statement_to_plan(statement.clone())?;
    match plan {
        LogicalPlan::Projection(projection) => Ok(projection.expr[0].clone()),
        _ => internal_err!("Unexpected plan type: {:?}", plan),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;

    use crate::logical_plan::utils::from_qualified_name;
    use crate::mdl::manifest::Manifest;
    use crate::mdl::AnalyzedWrenMDL;
    use datafusion::error::Result;

    #[test]
    fn test_create_wren_expr() -> Result<()> {
        let test_data: PathBuf =
            [env!("CARGO_MANIFEST_DIR"), "tests", "data", "mdl.json"]
                .iter()
                .collect();
        let mdl_json = fs::read_to_string(test_data.as_path()).unwrap();
        let mdl = serde_json::from_str::<Manifest>(&mdl_json).unwrap();
        let analyzed_mdl = Arc::new(AnalyzedWrenMDL::analyze(mdl)?);

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
            analyzed_mdl.clone(),
        )?;
        assert_eq!(expr.to_string(), "orders.orderkey + orders.custkey");
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
}
