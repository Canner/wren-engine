use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use petgraph::Graph;

use wren_modeling_rs::mdl::manifest::{JoinType, Manifest};
use wren_modeling_rs::mdl::{Dataset, WrenMDL};

fn main() {
    let test_data: PathBuf = [env!("CARGO_MANIFEST_DIR"), "tests", "data", "mdl.json"]
        .iter()
        .collect();
    let mdl_json = fs::read_to_string(test_data.as_path()).unwrap();
    let mdl = serde_json::from_str::<Manifest>(&mdl_json).unwrap();
    let wren_mdl = Arc::new(WrenMDL::new(mdl));

    // let sql = "select * from orders join customer using (custkey)";
    // let planned = mdl::transform_sql(Arc::clone(&wren_mdl), sql).unwrap();
    // println!("unparse to SQL:\n {}", planned);

    let mut directed_graph: Graph<&Dataset, &JoinType> = Graph::new();
    let orders_orderkey = wren_mdl.qualifed_references.get("orders.orderkey").unwrap();
    let customer_custkey = wren_mdl
        .qualifed_references
        .get("customer.custkey")
        .unwrap();
    let orders_vertex = directed_graph.add_node(&orders_orderkey.dataset);
    let customer_vertex = directed_graph.add_node(&customer_custkey.dataset);
    directed_graph.add_edge(orders_vertex, customer_vertex, &JoinType::OneToMany);
}
