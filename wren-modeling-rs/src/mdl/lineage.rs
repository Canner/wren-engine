use core::panic;
use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::common::Column;
use datafusion::sql::sqlparser::ast::visit_expressions;
use datafusion::sql::sqlparser::ast::Expr::{CompoundIdentifier, Identifier};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::TableReference;
use petgraph::Graph;

use crate::mdl::{utils, WrenMDL};

use super::manifest::JoinType;
use super::utils::to_expr_queue;
use super::Dataset;

pub struct Lineage {
    pub source_columns_map: HashMap<Column, HashSet<Column>>,
    pub requried_fields_map: HashMap<Column, HashSet<Column>>,
}

impl Lineage {
    pub fn new(mdl: &WrenMDL) -> Self {
        let source_columns_map = Lineage::collect_source_columns(mdl);
        let requried_fields_map = Lineage::collect_required_fields(mdl, &source_columns_map);
        Lineage {
            source_columns_map,
            requried_fields_map,
        }
    }

    fn collect_source_columns(mdl: &WrenMDL) -> HashMap<Column, HashSet<Column>> {
        let mut source_columns_map = HashMap::new();

        mdl.manifest.models.iter().for_each(|model| {
            model.columns.iter().for_each(|column| {
                if column.is_calculated {
                    let expr: &String = match column.expression {
                        Some(ref exp) => exp,
                        None => return,
                    };
                    let source_columns = Self::collect_identifiers(expr);
                    let qualified_name =
                        Column::from_qualified_name(format!("{}.{}", model.name, column.name));
                    source_columns.iter().for_each(|source_column| {
                        source_columns_map
                            .entry(qualified_name.clone())
                            .or_insert(HashSet::new())
                            .insert(source_column.clone());
                    });
                // relationship columns are not a physical column
                } else if column.relationship.is_none() {
                    let qualified_name =
                        Column::from_qualified_name(format!("{}.{}", model.name, column.name));
                    source_columns_map.insert(qualified_name, HashSet::new());
                }
            });
        });
        source_columns_map
    }

    fn collect_identifiers(expr: &String) -> HashSet<Column> {
        let wrapped = format!("select {}", expr);
        let parsed = Parser::parse_sql(&GenericDialect {}, &wrapped).unwrap();
        let statement = parsed[0].clone();
        let mut visited: HashSet<Column> = HashSet::new();

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
        visited
    }

    fn collect_required_fields(
        mdl: &WrenMDL,
        source_colums_map: &HashMap<Column, HashSet<Column>>,
    ) -> HashMap<Column, HashSet<Column>> {
        let mut requried_fields_map: HashMap<Column, HashSet<Column>> = HashMap::new();
        source_colums_map
            .iter()
            .for_each(|(column, source_columns)| {
                let relation = column.clone().relation.unwrap();
                let mut model_name = if let TableReference::Bare { table } = relation {
                    table.clone()
                } else {
                    panic!("Expected a bare table reference, got {:?}", column.relation);
                };

                let column_ref = mdl.get_column_reference(model_name.as_ref(), &column.name);
                if !column_ref.column.is_calculated {
                    return;
                }

                let mut directed_graph: Graph<Dataset, JoinType> = Graph::new();
                source_columns.iter().for_each(|source_column| {
                    let mut expr_parts = to_expr_queue(source_column.clone());
                    while !expr_parts.is_empty() {
                        let ident = expr_parts.pop_front().unwrap();
                        let source_column_ref = mdl.get_column_reference(&model_name, &ident);
                        let left_vertex = directed_graph.add_node(column_ref.dataset.clone());
                        match source_column_ref.dataset {
                            Dataset::Model(_) => {
                                match source_column_ref.column.relationship.clone() {
                                    Some(rs) => {
                                        if let Some(rs_rf) = mdl.get_relationship(&rs) {
                                            let related_model_name: Arc<str> = rs_rf
                                                .models
                                                .iter()
                                                .find(|m| m.as_str() != model_name.as_ref())
                                                .map(|m| m.as_str().into())
                                                .unwrap();
                                            if related_model_name.as_ref()
                                                != source_column_ref.column.r#type.as_str()
                                            {
                                                panic!(
                                                    "invalid relationship type: {}",
                                                    source_column
                                                );
                                            }
                                            let related_model =
                                                mdl.get_model(related_model_name.as_ref()).unwrap();
                                            let right_vertex = directed_graph
                                                .add_node(Dataset::Model(related_model));
                                            directed_graph.add_edge(
                                                left_vertex,
                                                right_vertex,
                                                rs_rf.join_type,
                                            );
                                            model_name = related_model_name;
                                        } else {
                                            panic!("relationship not found: {}", source_column);
                                        }
                                    }
                                    None => {
                                        if !expr_parts.is_empty() {
                                            panic!("invalid relationship chain: {}", source_column);
                                        }
                                        let value = Column::from_qualified_name(format!(
                                            "{}.{}",
                                            model_name, source_column_ref.column.name
                                        ));
                                        if source_column_ref.column.is_calculated {
                                            todo!("calculated source column not supported")
                                        }
                                        requried_fields_map
                                            .entry(column.clone())
                                            .or_default()
                                            .insert(value);
                                    }
                                }
                            }
                            Dataset::Metric(_) => {
                                todo!("Metric dataset not supported");
                            }
                        }
                    }
                });
                if !utils::is_dag(&directed_graph) {
                    panic!("cyclic dependency detected: {}", column);
                }
            });
        requried_fields_map
    }
}

#[cfg(test)]
mod test {
    use std::{
        fs,
        path::{self, PathBuf},
    };

    use datafusion::common::Column;

    use crate::mdl::WrenMDL;

    #[test]
    fn test_collect_source_columns() {
        let test_data: PathBuf = [env!("CARGO_MANIFEST_DIR"), "tests", "data", "mdl.json"]
            .iter()
            .collect();
        let mdl_json =
            fs::read_to_string(path::Path::new(test_data.as_path())).expect("Unable to read file");
        let manifest = serde_json::from_str::<crate::mdl::manifest::Manifest>(&mdl_json).unwrap();
        let wren_mdl = WrenMDL::new(manifest);
        let lineage = crate::mdl::lineage::Lineage::new(&wren_mdl);
        assert_eq!(lineage.source_columns_map.len(), 9);
        assert_eq!(
            lineage
                .source_columns_map
                .get(&Column::from_qualified_name("customer.custkey_plus"))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            lineage
                .source_columns_map
                .get(&Column::from_qualified_name("orders.orderkey_plus_custkey"))
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            lineage
                .source_columns_map
                .get(&Column::from_qualified_name("orders.hash_orderkey"))
                .unwrap()
                .len(),
            1
        );
        let customer_name: Vec<Column> = lineage
            .source_columns_map
            .get(&Column::from_qualified_name("orders.customer_name"))
            .unwrap()
            .iter()
            .cloned()
            .collect();
        assert_eq!(customer_name.len(), 1);
        assert_eq!(customer_name[0], Column::new_unqualified("customer.name"));
    }

    #[test]
    fn test_collect_required_fields() {
        let test_data: PathBuf = [env!("CARGO_MANIFEST_DIR"), "tests", "data", "mdl.json"]
            .iter()
            .collect();
        let mdl_json =
            fs::read_to_string(path::Path::new(test_data.as_path())).expect("Unable to read file");
        let manifest = serde_json::from_str::<crate::mdl::manifest::Manifest>(&mdl_json).unwrap();
        let wren_mdl = WrenMDL::new(manifest);
        let lineage = crate::mdl::lineage::Lineage::new(&wren_mdl);
        dbg!(lineage.requried_fields_map.clone());
        assert_eq!(lineage.requried_fields_map.len(), 4);
        assert_eq!(
            lineage
                .requried_fields_map
                .get(&Column::from_qualified_name("Customer.custkey_plus"))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            lineage
                .requried_fields_map
                .get(&Column::from_qualified_name("orders.orderkey_plus_custkey"))
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            lineage
                .requried_fields_map
                .get(&Column::from_qualified_name("orders.hash_orderkey"))
                .unwrap()
                .len(),
            1
        );

        let customer_name: Vec<Column> = lineage
            .requried_fields_map
            .get(&Column::from_qualified_name("orders.customer_name"))
            .unwrap()
            .iter()
            .cloned()
            .collect();
        assert_eq!(customer_name.len(), 1);
        assert_eq!(
            customer_name[0],
            Column::from_qualified_name("customer.name")
        );
    }
}
