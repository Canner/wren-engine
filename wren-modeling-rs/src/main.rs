use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use wren_modeling_rs::mdl::manifest::Manifest;
use wren_modeling_rs::mdl::{self, WrenMDL};

fn main() {
    let test_data: PathBuf = [env!("CARGO_MANIFEST_DIR"), "tests", "data", "mdl.json"]
        .iter()
        .collect();
    let mdl_json = fs::read_to_string(test_data.as_path()).unwrap();
    let mdl = serde_json::from_str::<Manifest>(&mdl_json).unwrap();
    let wren_mdl = Arc::new(WrenMDL::new(mdl));

    let sql = "select orderkey as k1 from orders";
    let planned = mdl::transform_sql(Arc::clone(&wren_mdl), sql).unwrap();
    println!("unparse to SQL:\n {}", planned);
}
