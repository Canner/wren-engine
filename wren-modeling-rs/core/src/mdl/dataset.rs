use std::sync::Arc;
use std::fmt::Display;
use crate::mdl::manifest::{Column, Metric, Model};

impl Model {
    /// Physical columns are columns that can be selected from the model.
    /// e.g. columns that are not a relationship column
    pub fn get_physical_columns(&self) -> Vec<Arc<Column>> {
        self.columns
            .iter()
            .filter(|c| c.relationship.is_none())
            .map(Arc::clone)
            .collect()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn get_column(&self, column_name: &str) -> Option<Arc<Column>> {
        self.columns
            .iter()
            .find(|c| c.name == column_name)
            .map(Arc::clone)
    }

    pub fn primary_key(&self) -> Option<&str> {
        self.primary_key.as_deref()
    }
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub enum Dataset {
    Model(Arc<Model>),
    Metric(Arc<Metric>),
}

impl Dataset {
    pub fn name(&self) -> &str {
        match self {
            Dataset::Model(model) => model.name(),
            Dataset::Metric(metric) => metric.name(),
        }
    }

    pub fn try_as_model(&self) -> Option<Arc<Model>> {
        match self {
            Dataset::Model(model) => Some(Arc::clone(model)),
            _ => None,
        }
    }
}

impl Display for Dataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Dataset::Model(model) => write!(f, "{}", model.name()),
            Dataset::Metric(metric) => write!(f, "{}", metric.name()),
        }
    }
}