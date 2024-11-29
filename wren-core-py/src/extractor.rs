use crate::errors::CoreError;
use crate::manifest::{to_manifest, PyManifest};
use pyo3::pyfunction;
use std::collections::HashSet;
use std::sync::Arc;
use wren_core::mdl::manifest::{Manifest, Model, Relationship, View};
use wren_core::mdl::WrenMDL;

/// parse the given SQL and return the list of used table name.
#[pyfunction]
#[pyo3(name = "resolve_used_table_names", signature = (mdl_base64, sql), text_signature = "(mdl_base64: str, sql: str)")]
pub fn py_resolve_used_table_names(
    mdl_base64: &str,
    sql: &str,
) -> Result<Vec<String>, CoreError> {
    let manifest = to_manifest(mdl_base64)?;
    resolve_used_table_names(manifest, sql)
}

fn resolve_used_table_names(
    manifest: Manifest,
    sql: &str,
) -> Result<Vec<String>, CoreError> {
    let mdl = WrenMDL::new_ref(manifest);
    let ctx_state = wren_core::SessionContext::new().state();
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
                    t.catalog().unwrap_or_default() == mdl.catalog()
                        && t.schema().unwrap_or_default() == mdl.schema()
                })
                .map(|t| t.table().to_string())
                .collect()
        })
}

/// Given a used dataset list, extract manifest by removing unused datasets.
/// If a model is related to another dataset, both datasets will be kept.
/// The relationship between of them will be kept as well.
/// A dataset could be model, view.
#[pyfunction]
#[pyo3(signature = (mdl_base64, used_datasets), text_signature = "(mdl_base64: str, used_datasets: list[str])")]
pub fn extract_manifest(
    mdl_base64: &str,
    used_datasets: Vec<String>,
) -> Result<PyManifest, CoreError> {
    let manifest = to_manifest(mdl_base64)?;
    let mdl = WrenMDL::new_ref(manifest);
    let used_models = extract_models(&mdl, &used_datasets);
    let (used_views, models_of_views) = extract_views(&mdl, &used_datasets);
    let used_relationships = extract_relationships(&mdl, &used_datasets);
    Ok(PyManifest {
        catalog: mdl.catalog().to_string(),
        schema: mdl.schema().to_string(),
        models: [used_models, models_of_views].concat(),
        relationships: used_relationships,
        metrics: mdl.metrics().to_vec(),
        views: used_views,
    })
}

fn extract_models(mdl: &Arc<WrenMDL>, used_datasets: &[String]) -> Vec<Arc<Model>> {
    let mut used_set: HashSet<String> = used_datasets.iter().cloned().collect();
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
                .filter(|related| used_set.insert(related.clone()))
                .for_each(|related| stack.push(related));
        }
    }
    mdl.models()
        .iter()
        .filter(|model| used_set.contains(model.name()))
        .cloned()
        .collect()
}

fn extract_views(
    mdl: &Arc<WrenMDL>,
    used_datasets: &[String],
) -> (Vec<Arc<View>>, Vec<Arc<Model>>) {
    let used_set: HashSet<&str> = used_datasets.iter().map(String::as_str).collect();
    let stack: Vec<&str> = used_datasets.iter().map(String::as_str).collect();
    let models = stack
        .iter()
        .filter_map(|&dataset_name| {
            mdl.get_view(dataset_name).and_then(|view| {
                resolve_used_table_names(mdl.manifest.clone(), view.statement.as_str())
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
    mdl: &Arc<WrenMDL>,
    used_datasets: &[String],
) -> Vec<Arc<Relationship>> {
    let mut used_set: HashSet<String> = used_datasets.iter().cloned().collect();
    let mut stack: Vec<String> = used_datasets.to_vec();
    while let Some(dataset_name) = stack.pop() {
        if let Some(relationship) = mdl.get_relationship(&dataset_name) {
            for model in &relationship.models {
                if used_set.insert(model.clone()) {
                    stack.push(model.clone());
                }
            }
        }
    }
    mdl.relationships()
        .iter()
        .filter(|rel| rel.models.iter().any(|model| used_set.contains(model)))
        .cloned()
        .collect()
}
