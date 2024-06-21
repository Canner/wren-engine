use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::sync::Arc;

use datafusion::common::{internal_err, plan_err, Column};
use datafusion::error::Result;
use datafusion::sql::TableReference;
use petgraph::Graph;

use crate::logical_plan::utils::from_qualified_name;
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
    pub fn new(mdl: &WrenMDL) -> Result<Self> {
        let source_columns_map = Lineage::collect_source_columns(mdl);
        let RequiredInfo {
            required_fields_map,
            required_dataset_topo,
        } = Lineage::collect_required_fields(mdl, &source_columns_map)?;
        Ok(Lineage {
            source_columns_map,
            required_fields_map,
            required_dataset_topo,
        })
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
                    let qualified_name =
                        from_qualified_name(mdl, model.name(), column.name());
                    source_columns.iter().for_each(|source_column| {
                        source_columns_map
                            .entry(qualified_name.clone())
                            .or_insert(HashSet::new())
                            .insert(Column::new(
                                Some(TableReference::full(
                                    mdl.catalog(),
                                    mdl.schema(),
                                    model.name(),
                                )),
                                &source_column.name,
                            ));
                    });
                // relationship columns are not a physical column
                } else if column.relationship.is_none() {
                    let qualified_name =
                        from_qualified_name(mdl, model.name(), column.name());
                    source_columns_map.insert(qualified_name, HashSet::new());
                }
            });
        });
        source_columns_map
    }
    fn collect_required_fields(
        mdl: &WrenMDL,
        source_colums_map: &HashMap<Column, HashSet<Column>>,
    ) -> Result<RequiredInfo> {
        let mut required_fields_map: HashMap<Column, HashSet<Column>> = HashMap::new();
        let mut required_dataset_topo: HashMap<Column, Graph<Dataset, DatasetLink>> =
            HashMap::new();
        for (column, source_columns) in source_colums_map.iter() {
            let Some(relation) = column.clone().relation else {
                return internal_err!("relation not found: {}", column);
            };
            let mut current_relation = match relation {
                TableReference::Bare { table } => {
                    TableReference::full(mdl.catalog(), mdl.schema(), table)
                }
                TableReference::Partial { schema, table } => {
                    TableReference::full(mdl.catalog(), schema, table)
                }
                TableReference::Full {
                    catalog,
                    schema,
                    table,
                } => TableReference::full(catalog, schema, table),
            };

            let Some(column_ref) = mdl.get_column_reference(column) else {
                return internal_err!("column not found: {}", column);
            };

            // Only analyze the calculated field and the relationship field
            if !column_ref.column.is_calculated
                || column_ref.column.relationship.is_some()
            {
                continue;
            }

            let mut directed_graph: Graph<Dataset, DatasetLink> = Graph::new();
            let mut node_index_map = HashMap::new();
            let mut left_vertex = *node_index_map
                .entry(column_ref.dataset.clone())
                .or_insert_with(|| directed_graph.add_node(column_ref.dataset.clone()));

            for source_column in source_columns.iter() {
                let mut expr_parts = to_expr_queue(source_column.clone());
                while !expr_parts.is_empty() {
                    let ident = expr_parts.pop_front().unwrap();
                    let Some(source_column_ref) = mdl.get_column_reference(&Column::new(
                        Some(current_relation.clone()),
                        ident.clone(),
                    )) else {
                        return plan_err!("source column not found: {}", ident);
                    };
                    match source_column_ref.dataset {
                        Dataset::Model(_) => {
                            if let Some(rs) =
                                source_column_ref.column.relationship.clone()
                            {
                                if let Some(rs_rf) = mdl.get_relationship(&rs) {
                                    let related_model_name = rs_rf
                                        .models
                                        .iter()
                                        .find(|m| m != &current_relation.table())
                                        .cloned()
                                        .unwrap();
                                    if related_model_name
                                        != source_column_ref.column.r#type
                                    {
                                        return plan_err!(
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
                                                .insert(Column::from_qualified_name(
                                                    format!(
                                                        "{}.{}.{}",
                                                        mdl.catalog(),
                                                        mdl.schema(),
                                                        ident.flat_name()
                                                    ),
                                                ));
                                        });

                                    let related_model =
                                        mdl.get_model(&related_model_name).unwrap();

                                    let right_vertex = *node_index_map
                                        .entry(Dataset::Model(Arc::clone(&related_model)))
                                        .or_insert_with(|| {
                                            directed_graph.add_node(Dataset::Model(
                                                Arc::clone(&related_model),
                                            ))
                                        });
                                    directed_graph.add_edge(
                                        left_vertex,
                                        right_vertex,
                                        get_dataset_link_revers_if_need(
                                            source_column_ref.dataset.clone(),
                                            rs_rf,
                                        ),
                                    );

                                    current_relation = TableReference::full(
                                        mdl.catalog(),
                                        mdl.schema(),
                                        related_model_name,
                                    );

                                    left_vertex = right_vertex;
                                } else {
                                    return plan_err!(
                                        "relationship not found: {}",
                                        source_column
                                    );
                                }
                            } else {
                                if !expr_parts.is_empty() {
                                    return plan_err!(
                                        "invalid relationship chain: {}",
                                        source_column
                                    );
                                }
                                let value = Column::new(
                                    Some(current_relation.clone()),
                                    source_column_ref.column.name().to_string(),
                                );
                                if source_column_ref.column.is_calculated {
                                    todo!("calculated source column not supported")
                                }
                                required_fields_map
                                    .entry(column.clone())
                                    .or_default()
                                    .insert(value);
                            }
                        }
                        Dataset::Metric(_) => {
                            todo!("Metric dataset not supported");
                        }
                    }
                }
            }
            if !utils::is_dag(&directed_graph) {
                return plan_err!("cyclic dependency detected: {}", column);
            }
            required_dataset_topo.insert(column.clone(), directed_graph);
        }
        Ok(RequiredInfo {
            required_fields_map,
            required_dataset_topo,
        })
    }
}

struct RequiredInfo {
    required_fields_map: HashMap<Column, HashSet<Column>>,
    required_dataset_topo: HashMap<Column, Graph<Dataset, DatasetLink>>,
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub struct DatasetLink {
    pub join_type: JoinType,
    pub condition: String,
}

impl DatasetLink {
    fn new(join_type: JoinType, condition: String) -> Self {
        DatasetLink {
            join_type,
            condition,
        }
    }
}

impl Display for DatasetLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ON {}", self.join_type, self.condition)
    }
}

fn get_dataset_link_revers_if_need(
    source: Dataset,
    rs: Arc<Relationship>,
) -> DatasetLink {
    let join_type = if rs.models[0] == source.name() {
        rs.join_type
    } else {
        match rs.join_type {
            JoinType::OneToMany => JoinType::ManyToOne,
            JoinType::ManyToOne => JoinType::OneToMany,
            _ => rs.join_type,
        }
    };
    DatasetLink::new(join_type, rs.condition.clone())
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::{
        fs,
        path::{self, PathBuf},
    };

    use datafusion::common::Column;
    use datafusion::error::Result;
    use datafusion::sql::TableReference;

    use crate::mdl::builder::{
        ColumnBuilder, ManifestBuilder, ModelBuilder, RelationshipBuilder,
    };
    use crate::mdl::manifest::{JoinType, Manifest};
    use crate::mdl::{Dataset, WrenMDL};

    #[test]
    fn test_collect_source_columns() -> Result<()> {
        let test_data: PathBuf =
            [env!("CARGO_MANIFEST_DIR"), "tests", "data", "mdl.json"]
                .iter()
                .collect();
        let mdl_json = fs::read_to_string(path::Path::new(test_data.as_path()))
            .expect("Unable to read file");
        let manifest = serde_json::from_str::<Manifest>(&mdl_json).unwrap();
        let wren_mdl = WrenMDL::new(manifest);
        let lineage = crate::mdl::lineage::Lineage::new(&wren_mdl)?;
        assert_eq!(lineage.source_columns_map.len(), 13);
        assert_eq!(
            lineage
                .source_columns_map
                .get(&Column::from_qualified_name(
                    "test.test.customer.custkey_plus"
                ))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            lineage
                .source_columns_map
                .get(&Column::from_qualified_name(
                    "test.test.orders.orderkey_plus_custkey"
                ))
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            lineage
                .source_columns_map
                .get(&Column::from_qualified_name(
                    "test.test.orders.hash_orderkey"
                ))
                .unwrap()
                .len(),
            1
        );
        let customer_name: Vec<Column> = lineage
            .source_columns_map
            .get(&Column::from_qualified_name(
                "test.test.orders.customer_name",
            ))
            .unwrap()
            .iter()
            .cloned()
            .collect();
        assert_eq!(customer_name.len(), 1);
        assert_eq!(
            customer_name[0],
            Column {
                relation: Some(TableReference::full("test", "test", "orders")),
                name: "customer.name".to_string()
            }
        );
        Ok(())
    }

    #[test]
    fn test_collect_required_fields() -> Result<()> {
        let test_data: PathBuf =
            [env!("CARGO_MANIFEST_DIR"), "tests", "data", "mdl.json"]
                .iter()
                .collect();
        let mdl_json = fs::read_to_string(path::Path::new(test_data.as_path()))
            .expect("Unable to read file");
        let manifest = serde_json::from_str::<Manifest>(&mdl_json).unwrap();
        let wren_mdl = WrenMDL::new(manifest);
        let lineage = crate::mdl::lineage::Lineage::new(&wren_mdl)?;
        assert_eq!(lineage.required_fields_map.len(), 5);
        assert_eq!(
            lineage
                .required_fields_map
                .get(&Column::from_qualified_name(
                    "test.test.customer.custkey_plus"
                ))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            lineage
                .required_fields_map
                .get(&Column::from_qualified_name(
                    "test.test.orders.orderkey_plus_custkey"
                ))
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            lineage
                .required_fields_map
                .get(&Column::from_qualified_name(
                    "test.test.orders.hash_orderkey"
                ))
                .unwrap()
                .len(),
            1
        );

        let customer_name = lineage
            .required_fields_map
            .get(&Column::from_qualified_name(
                "test.test.orders.customer_name",
            ))
            .unwrap();
        let expected: HashSet<Column> = HashSet::from([
            Column::from_qualified_name("test.test.customer.name"),
            Column::from_qualified_name("test.test.orders.custkey"),
            Column::from_qualified_name("test.test.customer.custkey"),
        ]);

        assert_eq!(customer_name.len(), 3);
        assert_eq!(*customer_name, expected);
        Ok(())
    }

    #[test]
    fn test_required_dataset_topo() -> Result<()> {
        let manifest = testing_manifest();
        let wren_mdl = WrenMDL::new(manifest);
        let lineage = crate::mdl::lineage::Lineage::new(&wren_mdl)?;
        assert_eq!(lineage.required_dataset_topo.len(), 2);
        let customer_name = lineage
            .required_dataset_topo
            .get(&Column::from_qualified_name("wrenai.default.a.c1"))
            .unwrap();
        assert_eq!(customer_name.node_count(), 3);
        assert_eq!(customer_name.edge_count(), 2);
        let mut iter = customer_name.node_indices();
        let first = iter.next().unwrap();
        let source = customer_name.node_weight(first).unwrap();
        assert_eq!(source, &Dataset::Model(wren_mdl.get_model("a").unwrap()));

        let second = iter.next().unwrap();
        let target = customer_name.node_weight(second).unwrap();
        assert_eq!(target, &Dataset::Model(wren_mdl.get_model("b").unwrap()));
        let first_edge = customer_name.find_edge(first, second).unwrap();
        let edge = customer_name.edge_weight(first_edge).unwrap();
        assert_eq!(edge.join_type, JoinType::OneToOne);
        assert_eq!(edge.condition, "a.a1 = b.a1");

        let third = iter.next().unwrap();
        let target = customer_name.node_weight(third).unwrap();
        assert_eq!(target, &Dataset::Model(wren_mdl.get_model("c").unwrap()));
        let second_edge = customer_name.find_edge(second, third).unwrap();
        let edge = customer_name.edge_weight(second_edge).unwrap();
        assert_eq!(edge.join_type, JoinType::OneToOne);
        assert_eq!(edge.condition, "b.b1 = c.b1");
        Ok(())
    }

    fn testing_manifest() -> Manifest {
        ManifestBuilder::new()
            .model(
                ModelBuilder::new("a")
                    .table_reference("a")
                    .column(ColumnBuilder::new("id", "varchar").build())
                    .column(ColumnBuilder::new("a1", "varchar").build())
                    .column(ColumnBuilder::new("b", "b").relationship("a_b").build())
                    .column(
                        ColumnBuilder::new("c1", "varchar")
                            .calculated(true)
                            .expression("b.c.c1")
                            .build(),
                    )
                    .primary_key("id")
                    .build(),
            )
            .model(
                ModelBuilder::new("b")
                    .table_reference("b")
                    .column(ColumnBuilder::new("id", "varchar").build())
                    .column(ColumnBuilder::new("b1", "varchar").build())
                    .column(ColumnBuilder::new("a1", "varchar").build())
                    .column(ColumnBuilder::new("c", "c").relationship("b_c").build())
                    .column(
                        ColumnBuilder::new("c1", "varchar")
                            .calculated(true)
                            .expression("c.c1")
                            .build(),
                    )
                    .primary_key("id")
                    .build(),
            )
            .model(
                ModelBuilder::new("c")
                    .table_reference("c")
                    .column(ColumnBuilder::new("id", "varchar").build())
                    .column(ColumnBuilder::new("c1", "varchar").build())
                    .column(ColumnBuilder::new("b1", "varchar").build())
                    .primary_key("id")
                    .build(),
            )
            .relationship(
                RelationshipBuilder::new("a_b")
                    .model("a")
                    .model("b")
                    .join_type(JoinType::OneToOne)
                    .condition("a.a1 = b.a1")
                    .build(),
            )
            .relationship(
                RelationshipBuilder::new("b_c")
                    .model("b")
                    .model("c")
                    .join_type(JoinType::OneToOne)
                    .condition("b.b1 = c.b1")
                    .build(),
            )
            .build()
    }
}
