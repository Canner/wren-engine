use std::collections::VecDeque;

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
            .split(".")
            .for_each(|part| parts.push_back(part.to_string()));
    }
    parts
}

pub fn is_dag<'a, N: 'a, E: 'a, Ty, Ix>(g: &'a Graph<N, E, Ty, Ix>) -> bool
where
    Ty: EdgeType,
    Ix: petgraph::graph::IndexType,
{
    return g.is_directed() && !is_cyclic_directed(g);
}
