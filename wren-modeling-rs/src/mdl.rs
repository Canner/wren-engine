pub mod manifest;

use std::sync::Arc;

use crate::mdl::manifest::{Manifest, Model};

pub struct WrenMDL {
    pub manifest: Manifest,
}

impl WrenMDL {
    pub fn new(manifest: Manifest) -> Self {
        WrenMDL { manifest }
    }

    pub fn get_model(&self, name: &str) -> Option<Arc<Model>> {
        self.manifest
            .models
            .iter()
            .find(|model| model.name == name)
            .map(|model| Arc::clone(model))
    }
}
