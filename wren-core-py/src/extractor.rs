use crate::errors::CoreError;
use crate::manifest::to_manifest;
use pyo3::{pyclass, pymethods};
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use wren_core::mdl::manifest::{Model, Relationship, View};
use wren_core::mdl::WrenMDL;
use wren_core_base::mdl::Manifest;

#[pyclass]
#[derive(Clone)]
#[pyo3(name = "ManifestExtractor")]
pub struct PyManifestExtractor {
    mdl: Arc<WrenMDL>,
}

#[pymethods]
impl PyManifestExtractor {
    #[new]
    #[pyo3(signature = (mdl_base64=None))]
    pub fn new(mdl_base64: Option<&str>) -> Result<Self, CoreError> {
        mdl_base64
            .ok_or_else(|| CoreError::new("Expected a valid base64 encoded string for the model definition, but got None."))
            .and_then(to_manifest)
            .map(|manifest| Self {
                mdl: WrenMDL::new_ref(manifest),
            })
    }

    /// parse the given SQL and return the list of used table name.
    pub fn resolve_used_table_names(&self, sql: &str) -> Result<Vec<String>, CoreError> {
        resolve_used_table_names(&self.mdl, sql)
    }

    /// Given a used dataset list, extract manifest by removing unused datasets.
    /// If a model is related to another dataset, both datasets will be kept.
    /// The relationship between of them will be kept as well.
    /// A dataset could be model, view.
    pub fn extract_by(&self, used_datasets: Vec<String>) -> Result<Manifest, CoreError> {
        extract_manifest(&self.mdl, &used_datasets)
    }
}

fn resolve_used_table_names(mdl: &WrenMDL, sql: &str) -> Result<Vec<String>, CoreError> {
    let mut config = wren_core::SessionConfig::new();
    config.options_mut().sql_parser.enable_ident_normalization = false;
    let ctx_state = wren_core::SessionContext::new_with_config(config).state();
    ctx_state
        .sql_to_statement(sql, "generic")
        .map_err(CoreError::from)
        .and_then(|stmt| {
            ctx_state
                .resolve_table_references(&stmt)
                .map_err(CoreError::from)
        })
        .map(|tables| {
            tables
                .iter()
                .filter(|t| {
                    t.catalog().is_none_or(|catalog| catalog == mdl.catalog())
                        && t.schema().is_none_or(|schema| schema == mdl.schema())
                })
                .map(|t| t.table().to_string())
                .collect()
        })
}

fn extract_manifest(
    mdl: &WrenMDL,
    used_datasets: &[String],
) -> Result<Manifest, CoreError> {
    let extracted_models = extract_models(mdl, used_datasets);
    let (used_views, models_of_views) = extract_views(mdl, used_datasets);
    let used_models = [extracted_models, models_of_views]
        .concat()
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let used_relationships = extract_relationships(mdl, &used_models);
    Ok(Manifest {
        catalog: mdl.catalog().to_string(),
        schema: mdl.schema().to_string(),
        models: used_models,
        relationships: used_relationships,
        metrics: mdl.metrics().to_vec(),
        views: used_views,
        data_source: mdl.data_source(),
    })
}

fn extract_models(mdl: &WrenMDL, used_datasets: &[String]) -> Vec<Arc<Model>> {
    let mut used_set: HashMap<String, usize> =
        used_datasets.iter().map(|s| (s.clone(), 0)).collect();
    let mut stack: Vec<String> = used_datasets.to_vec();
    while let Some(dataset_name) = stack.pop() {
        if let Some(model) = mdl.get_model(&dataset_name) {
            model
                .columns
                .iter()
                .filter_map(|col| {
                    col.relationship
                        .as_ref()
                        .and_then(|rel_name| mdl.get_relationship(rel_name))
                })
                .flat_map(|rel| rel.models.clone())
                .for_each(|related| {
                    if let Entry::Vacant(vacant) = used_set.entry(related) {
                        let key = vacant.key().clone();
                        vacant.insert(0);
                        stack.push(key);
                    }
                });
        }
    }
    mdl.models()
        .iter()
        .filter(|model| used_set.contains_key(model.name()))
        .cloned()
        .collect()
}

fn extract_views(
    mdl: &WrenMDL,
    used_datasets: &[String],
) -> (Vec<Arc<View>>, Vec<Arc<Model>>) {
    let used_set: HashSet<&str> = used_datasets.iter().map(String::as_str).collect();
    let models = used_set
        .iter()
        .filter_map(|&dataset_name| {
            mdl.get_view(dataset_name).and_then(|view| {
                resolve_used_table_names(mdl, view.statement.as_str())
                    .ok()
                    .map(|used_tables| extract_models(mdl, &used_tables))
            })
        })
        .flatten()
        .collect::<Vec<_>>();
    let views = mdl
        .views()
        .iter()
        .filter(|view| used_set.contains(view.name()))
        .cloned()
        .collect();

    (views, models)
}

fn extract_relationships(
    mdl: &WrenMDL,
    used_models: &[Arc<Model>],
) -> Vec<Arc<Relationship>> {
    let model_names: Vec<_> = used_models.iter().map(|m| m.name.as_str()).collect();
    mdl.relationships()
        .iter()
        .filter(|rel| rel.models.iter().any(|m| model_names.contains(&m.as_str())))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::extractor::PyManifestExtractor;
    use crate::manifest::to_json_base64;
    use rstest::{fixture, rstest};
    use std::iter::Iterator;
    use wren_core::mdl::manifest::{DataSource, JoinType};
    use wren_core_base::mdl::builder::{
        ColumnBuilder, ManifestBuilder, ModelBuilder, RelationshipBuilder, ViewBuilder,
    };

    #[fixture]
    pub fn mdl_base64() -> String {
        let customer = ModelBuilder::new("customer")
            .table_reference("main.customer")
            .column(ColumnBuilder::new("c_custkey", "integer").build())
            .column(
                ColumnBuilder::new("orders", "orders")
                    .relationship("customer_orders")
                    .build(),
            )
            .build();
        let orders = ModelBuilder::new("orders")
            .table_reference("main.orders")
            .column(ColumnBuilder::new("o_orderkey", "integer").build())
            .column(ColumnBuilder::new("o_custkey", "integer").build())
            .column(
                ColumnBuilder::new("lineitems", "Lineitem")
                    .relationship("orders_lineitem")
                    .build(),
            )
            .build();
        let lineitem = ModelBuilder::new("lineitem")
            .table_reference("main.lineitem")
            .column(ColumnBuilder::new("l_orderkey", "integer").build())
            .build();
        let c_o_relationship = RelationshipBuilder::new("customer_orders")
            .model("customer")
            .model("orders")
            .join_type(JoinType::OneToMany)
            .condition("customer.custkey = orders.custkey")
            .build();
        let o_l_relationship = RelationshipBuilder::new("orders_lineitem")
            .model("orders")
            .model("lineitem")
            .join_type(JoinType::OneToMany)
            .condition("orders.orderkey = lineitem.orderkey")
            .build();
        let c_view = ViewBuilder::new("customer_view")
            .statement("SELECT * FROM my_catalog.my_schema.customer")
            .build();
        let part = ModelBuilder::new("part")
            .table_reference("main.part")
            .column(ColumnBuilder::new("p_partkey", "integer").build())
            .build();
        let p_view = ViewBuilder::new("part_view")
            .statement("SELECT * FROM my_catalog.my_schema.part")
            .build();
        let manifest = ManifestBuilder::new()
            .catalog("my_catalog")
            .schema("my_schema")
            .model(customer)
            .model(orders)
            .model(lineitem)
            .model(part)
            .relationship(c_o_relationship)
            .relationship(o_l_relationship)
            .view(c_view)
            .view(p_view)
            .data_source(DataSource::BigQuery)
            .build();
        to_json_base64(manifest).unwrap()
    }

    #[fixture]
    pub fn extractor(mdl_base64: String) -> PyManifestExtractor {
        PyManifestExtractor::new(Option::from(mdl_base64.as_str())).unwrap()
    }

    #[rstest]
    #[case(
        None,
        "Expected a valid base64 encoded string for the model definition, but got None."
    )]
    #[case(Some("xxx"), "Base64 decode error: Invalid padding")]
    #[case(Some("{}"), "Base64 decode error: Invalid symbol 123, offset 0.")]
    #[case(
        Some(""),
        "Serde JSON error: EOF while parsing a value at line 1 column 0"
    )]
    fn test_extractor_with_invalid_manifest(
        #[case] value: Option<&str>,
        #[case] error_message: &str,
    ) {
        match PyManifestExtractor::new(value) {
            Err(err) => {
                assert_eq!(err.to_string(), error_message);
            }
            Ok(_) => panic!("Expected an error but got Ok"),
        }
    }

    #[rstest]
    #[case("SELECT * FROM customer", &["customer"])]
    #[case("SELECT * FROM not_my_catalog.my_schema.customer", &[])]
    #[case("SELECT * FROM my_catalog.not_my_schema.customer", &[])]
    #[case("SELECT * FROM my_catalog.my_schema.customer", &["customer"])]
    #[case("SELECT * FROM my_catalog.my_schema.customer JOIN my_catalog.my_schema.orders ON customer.custkey = orders.custkey", &["customer", "orders"])]
    #[case("SELECT * FROM my_catalog.my_schema.customer_view", &["customer_view"])]
    #[case("WITH t1 as (select * from customer) select * from t1", &["customer"])]
    #[case("WITH customer as (select * from customer) select * from customer", &["customer"])]
    #[case("SELECT * from (select * from customer) as t1", &["customer"])]
    #[case("SELECT * from (select * from customer) as customer", &["customer"])]
    fn test_resolve_used_table_names(
        extractor: PyManifestExtractor,
        #[case] sql: &str,
        #[case] expected: &[&str],
    ) {
        assert_eq!(extractor.resolve_used_table_names(sql).unwrap(), expected);
    }

    #[rstest]
    #[case(&["customer"], &["customer", "lineitem", "orders"])]
    #[case(&["customer_view"], &["customer", "lineitem", "orders"])]
    #[case(&["orders"], &["lineitem", "orders"])]
    #[case(&["lineitem"], &["lineitem"])]
    #[case(&["part_view", "part"], &["part"])]
    fn test_extract_manifest_for_models(
        extractor: PyManifestExtractor,
        #[case] dataset: &[&str],
        #[case] expected_models: &[&str],
    ) {
        assert_eq!(
            extractor
                .extract_by(dataset.iter().map(|s| s.to_string()).collect())
                .unwrap()
                .models
                .iter()
                .map(|m| m.name.as_str())
                .collect::<Vec<_>>(),
            expected_models
        );
    }

    #[rstest]
    #[case(&["customer"], &["customer_orders", "orders_lineitem"])]
    #[case(&["customer_view"], &["customer_orders", "orders_lineitem"])]
    #[case(&["orders"], &["customer_orders", "orders_lineitem"])]
    #[case(&["lineitem"], &["orders_lineitem"])]
    fn test_extract_manifest_for_relationships(
        extractor: PyManifestExtractor,
        #[case] dataset: &[&str],
        #[case] expected_relationships: &[&str],
    ) {
        assert_eq!(
            extractor
                .extract_by(dataset.iter().map(|s| s.to_string()).collect())
                .unwrap()
                .relationships
                .iter()
                .map(|r| r.name.as_str())
                .collect::<Vec<_>>(),
            expected_relationships
        );
    }

    #[rstest]
    #[case(&["customer_view"], &["customer_view"])]
    #[case(&["customer"], &[])]
    #[case(&["orders"], &[])]
    #[case(&["lineitem"], &[])]
    fn test_extract_manifest_for_view(
        extractor: PyManifestExtractor,
        #[case] dataset: &[&str],
        #[case] expected_views: &[&str],
    ) {
        assert_eq!(
            extractor
                .extract_by(dataset.iter().map(|s| s.to_string()).collect())
                .unwrap()
                .views
                .iter()
                .map(|v| v.name.as_str())
                .collect::<Vec<_>>(),
            expected_views
        );
    }
}
