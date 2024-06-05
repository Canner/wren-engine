use std::collections::{HashSet, VecDeque};
use std::ops::ControlFlow;

use datafusion::sql::sqlparser::ast::visit_expressions;
use datafusion::sql::sqlparser::ast::Expr::{CompoundIdentifier, Identifier};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::{common::Column, sql::TableReference};
use petgraph::algo::is_cyclic_directed;
use petgraph::{EdgeType, Graph};

pub fn to_expr_queue(column: Column) -> VecDeque<String> {
    let mut parts = VecDeque::new();
    if let Some(relation) = column.relation {
        match relation {
            TableReference::Bare { table } => {
                parts.push_back(table.to_string());
            }
            TableReference::Partial { schema, table } => {
                parts.push_back(schema.to_string());
                parts.push_back(table.to_string());
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => {
                parts.push_back(catalog.to_string());
                parts.push_back(schema.to_string());
                parts.push_back(table.to_string());
            }
        }
        parts.push_back(column.name);
    } else {
        column
            .name
            .split('.')
            .for_each(|part| parts.push_back(part.to_string()));
    }
    parts
}

pub fn is_dag<'a, N: 'a, E: 'a, Ty, Ix>(g: &'a Graph<N, E, Ty, Ix>) -> bool
where
    Ty: EdgeType,
    Ix: petgraph::graph::IndexType,
{
    g.is_directed() && !is_cyclic_directed(g)
}

pub fn collect_identifiers(expr: &String) -> HashSet<datafusion::common::Column> {
    let wrapped = format!("select {}", expr);
    let parsed = Parser::parse_sql(&GenericDialect {}, &wrapped).unwrap();
    let statement = parsed[0].clone();
    let mut visited: HashSet<datafusion::common::Column> = HashSet::new();

    visit_expressions(&statement, |expr| {
        match expr {
            Identifier(id) => {
                visited.insert(datafusion::common::Column::from(id.value.clone()));
            }
            CompoundIdentifier(ids) => {
                let name = ids
                    .iter()
                    .map(|id| id.value.clone())
                    .collect::<Vec<String>>()
                    .join(".");
                visited.insert(datafusion::common::Column::new_unqualified(name));
            }
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });
    visited
}
