use core::panic;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::common::Column;
use datafusion::sql::TableReference;
use petgraph::Graph;

use crate::mdl::{utils, WrenMDL};

use super::manifest::{JoinType, Relationship};
use super::utils::to_expr_queue;
use super::Dataset;

pub struct Lineage {
    pub source_columns_map: HashMap<Column, HashSet<Column>>,
    pub required_fields_map: HashMap<Column, HashSet<Column>>,
    pub required_dataset_topo: HashMap<Column, Graph<Dataset, DatasetLink>>,
}

impl Lineage {
    pub fn new(mdl: &WrenMDL) -> Self {
        let source_columns_map = Lineage::collect_source_columns(mdl);
        let RequiredInfo {
            required_fields_map,
            required_dataset_topo,
        } = Lineage::collect_required_fields(mdl, &source_columns_map);
        Lineage {
            source_columns_map,
            required_fields_map,
            required_dataset_topo,
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
                    let source_columns = utils::collect_identifiers(expr);
                    let qualified_name = Column::from_qualified_name(format!(
                        "{}.{}",
                        model.name, column.name
                    ));
                    source_columns.iter().for_each(|source_column| {
                        source_columns_map
                            .entry(qualified_name.clone())
                            .or_insert(HashSet::new())
                            .insert(source_column.clone());
                    });
                // relationship columns are not a physical column
                } else if column.relationship.is_none() {
                    let qualified_name = Column::from_qualified_name(format!(
                        "{}.{}",
                        model.name, column.name
                    ));
                    source_columns_map.insert(qualified_name, HashSet::new());
                }
            });
        });
        source_columns_map
    }
    fn collect_required_fields(
        mdl: &WrenMDL,
        source_colums_map: &HashMap<Column, HashSet<Column>>,
    ) -> RequiredInfo {
        let mut required_fields_map: HashMap<Column, HashSet<Column>> = HashMap::new();
        let mut required_dataset_topo: HashMap<Column, Graph<Dataset, DatasetLink>> =
            HashMap::new();
        source_colums_map
            .iter()
            .for_each(|(column, source_columns)| {
                let relation = column.clone().relation.unwrap();
                let mut model_name = if let TableReference::Bare { table } = relation {
                    table.clone()
                } else {
                    panic!("Expected a bare table reference, got {:?}", column.relation);
                };

                let column_ref =
                    mdl.get_column_reference(model_name.as_ref(), &column.name);
                if !column_ref.column.is_calculated
                    || column_ref.column.relationship.is_some()
                {
                    return;
                }

                let mut directed_graph: Graph<Dataset, DatasetLink> = Graph::new();
                let mut node_index_map = HashMap::new();
                source_columns.iter().for_each(|source_column| {
                    let mut expr_parts = to_expr_queue(source_column.clone());
                    while !expr_parts.is_empty() {
                        let ident = expr_parts.pop_front().unwrap();
                        let source_column_ref =
                            mdl.get_column_reference(&model_name, &ident);
                        let left_vertex = *node_index_map
                            .entry(column_ref.dataset.clone())
                            .or_insert_with(|| {
                                directed_graph.add_node(column_ref.dataset.clone())
                            });
                        match source_column_ref.dataset {
                            Dataset::Model(_) => {
                                match source_column_ref.column.relationship.clone() {
                                    Some(rs) => {
                                        if let Some(rs_rf) = mdl.get_relationship(&rs) {
                                            let related_model_name: Arc<str> = rs_rf
                                                .models
                                                .iter()
                                                .find(|m| {
                                                    m.as_str() != model_name.as_ref()
                                                })
                                                .map(|m| m.as_str().into())
                                                .unwrap();
                                            if related_model_name.as_ref()
                                                != source_column_ref
                                                    .column
                                                    .r#type
                                                    .as_str()
                                            {
                                                panic!(
                                                    "invalid relationship type: {}",
                                                    source_column
                                                );
                                            }

                                            utils::collect_identifiers(&rs_rf.condition)
                                                .iter()
                                                .cloned()
                                                .for_each(|ident| {
                                                    required_fields_map
                                                        .entry(column.clone())
                                                        .or_default()
                                                        .insert(
                                                            Column::from_qualified_name(
                                                                ident.flat_name(),
                                                            ),
                                                        );
                                                });

                                            let related_model = mdl
                                                .get_model(related_model_name.as_ref())
                                                .unwrap();

                                            let right_vertex = *node_index_map
                                                .entry(Dataset::Model(Arc::clone(
                                                    &related_model,
                                                )))
                                                .or_insert_with(|| {
                                                    directed_graph.add_node(
                                                        Dataset::Model(Arc::clone(
                                                            &related_model,
                                                        )),
                                                    )
                                                });
                                            directed_graph.add_edge(
                                                left_vertex,
                                                right_vertex,
                                                get_dataset_link_revers_if_need(
                                                    column_ref.dataset.clone(),
                                                    Dataset::Model(Arc::clone(
                                                        &related_model,
                                                    )),
                                                    rs_rf,
                                                ),
                                            );

                                            model_name = related_model_name;
                                        } else {
                                            panic!(
                                                "relationship not found: {}",
                                                source_column
                                            );
                                        }
                                    }
                                    None => {
                                        if !expr_parts.is_empty() {
                                            panic!(
                                                "invalid relationship chain: {}",
                                                source_column
                                            );
                                        }
                                        let value = Column::from_qualified_name(format!(
                                            "{}.{}",
                                            model_name, source_column_ref.column.name
                                        ));
                                        if source_column_ref.column.is_calculated {
                                            todo!(
                                                "calculated source column not supported"
                                            )
                                        }
                                        required_fields_map
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
                required_dataset_topo.insert(column.clone(), directed_graph);
            });
        RequiredInfo {
            required_fields_map,
            required_dataset_topo,
        }
    }
}

struct RequiredInfo {
    required_fields_map: HashMap<Column, HashSet<Column>>,
    required_dataset_topo: HashMap<Column, Graph<Dataset, DatasetLink>>,
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub struct DatasetLink {
    pub source: Dataset,
    pub target: Dataset,
    pub join_type: JoinType,
    pub condition: String,
}

impl DatasetLink {
    fn new(
        source: Dataset,
        target: Dataset,
        join_type: JoinType,
        condition: String,
    ) -> Self {
        DatasetLink {
            source,
            target,
            join_type,
            condition,
        }
    }
}

fn get_dataset_link_revers_if_need(
    source: Dataset,
    target: Dataset,
    rs: Arc<Relationship>,
) -> DatasetLink {
    let join_type = if rs.models[0] == source.get_name() {
        rs.join_type
    } else {
        match rs.join_type {
            JoinType::OneToMany => JoinType::ManyToOne,
            JoinType::ManyToOne => JoinType::OneToMany,
            _ => rs.join_type,
        }
    };
    DatasetLink::new(source, target, join_type, rs.condition.clone())
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::{
        fs,
        path::{self, PathBuf},
    };

    use datafusion::common::Column;

    use crate::mdl::{Dataset, WrenMDL};

    #[test]
    fn test_collect_source_columns() {
        let test_data: PathBuf =
            [env!("CARGO_MANIFEST_DIR"), "tests", "data", "mdl.json"]
                .iter()
                .collect();
        let mdl_json = fs::read_to_string(path::Path::new(test_data.as_path()))
            .expect("Unable to read file");
        let manifest =
            serde_json::from_str::<crate::mdl::manifest::Manifest>(&mdl_json).unwrap();
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
        let test_data: PathBuf =
            [env!("CARGO_MANIFEST_DIR"), "tests", "data", "mdl.json"]
                .iter()
                .collect();
        let mdl_json = fs::read_to_string(path::Path::new(test_data.as_path()))
            .expect("Unable to read file");
        let manifest =
            serde_json::from_str::<crate::mdl::manifest::Manifest>(&mdl_json).unwrap();
        let wren_mdl = WrenMDL::new(manifest);
        let lineage = crate::mdl::lineage::Lineage::new(&wren_mdl);
        assert_eq!(lineage.required_fields_map.len(), 4);
        assert_eq!(
            lineage
                .required_fields_map
                .get(&Column::from_qualified_name("Customer.custkey_plus"))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            lineage
                .required_fields_map
                .get(&Column::from_qualified_name("orders.orderkey_plus_custkey"))
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            lineage
                .required_fields_map
                .get(&Column::from_qualified_name("orders.hash_orderkey"))
                .unwrap()
                .len(),
            1
        );

        let customer_name = lineage
            .required_fields_map
            .get(&Column::from_qualified_name("orders.customer_name"))
            .unwrap();
        let expected: HashSet<Column> = HashSet::from([
            Column::from_qualified_name("customer.name"),
            Column::from_qualified_name("orders.custkey"),
            Column::from_qualified_name("customer.custkey"),
        ]);

        assert_eq!(customer_name.len(), 3);
        assert_eq!(*customer_name, expected,);
    }

    #[test]
    fn test_required_dataset_topo() {
        let test_data: PathBuf =
            [env!("CARGO_MANIFEST_DIR"), "tests", "data", "mdl.json"]
                .iter()
                .collect();
        let mdl_json = fs::read_to_string(path::Path::new(test_data.as_path()))
            .expect("Unable to read file");
        let manifest =
            serde_json::from_str::<crate::mdl::manifest::Manifest>(&mdl_json).unwrap();
        let wren_mdl = WrenMDL::new(manifest);
        let lineage = crate::mdl::lineage::Lineage::new(&wren_mdl);
        assert_eq!(lineage.required_dataset_topo.len(), 4);
        let customer_name = lineage
            .required_dataset_topo
            .get(&Column::from_qualified_name("orders.customer_name"))
            .unwrap();
        assert_eq!(customer_name.node_count(), 2);
        assert_eq!(customer_name.edge_count(), 1);
        let mut edges = customer_name.edge_indices().collect::<Vec<_>>();
        let edge = customer_name.edge_weight(edges.pop().unwrap()).unwrap();
        assert_eq!(
            edge.source,
            Dataset::Model(wren_mdl.get_model("orders").unwrap())
        );
        assert_eq!(
            edge.target,
            Dataset::Model(wren_mdl.get_model("customer").unwrap())
        );
        assert_eq!(edge.join_type, crate::mdl::manifest::JoinType::ManyToOne);
        assert_eq!(
            edge.condition,
            "customer.custkey = orders.custkey".to_string()
        );
    }
}
