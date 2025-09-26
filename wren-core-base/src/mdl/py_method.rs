/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#[cfg(feature = "python-binding")]
mod manifest_python_impl {
    use crate::mdl::manifest::{Manifest, Model, RowLevelAccessControl, SessionProperty};
    use crate::mdl::DataSource;
    use pyo3::{pymethods, PyResult};
    use std::sync::Arc;

    #[pymethods]
    impl Manifest {
        #[getter]
        fn catalog(&self) -> PyResult<String> {
            Ok(self.catalog.clone())
        }

        #[getter]
        fn schema(&self) -> PyResult<String> {
            Ok(self.schema.clone())
        }

        #[getter]
        fn models(&self) -> PyResult<Vec<Model>> {
            Ok(self
                .models
                .iter()
                .map(|m| Arc::unwrap_or_clone(Arc::clone(m)))
                .collect())
        }

        #[getter]
        fn data_source(&self) -> PyResult<Option<DataSource>> {
            Ok(self.data_source)
        }

        fn get_model(&self, name: &str) -> PyResult<Option<Model>> {
            let model = self
                .models
                .iter()
                .find(|m| m.name == name)
                .cloned()
                .map(Arc::unwrap_or_clone);
            Ok(model)
        }
    }

    #[pymethods]
    impl Model {
        #[getter]
        fn get_name(&self) -> PyResult<String> {
            Ok(self.name.clone())
        }
    }

    #[pymethods]
    impl SessionProperty {
        #[new]
        #[pyo3(signature = (name, required = false, default_expr = None))]
        pub fn new(name: String, required: bool, default_expr: Option<String>) -> Self {
            Self {
                normalized_name: name.to_lowercase(),
                name,
                required,
                default_expr,
            }
        }
    }

    #[pymethods]
    impl RowLevelAccessControl {
        #[new]
        #[pyo3(signature = (name, condition, required_properties = vec![]))]
        fn new(name: String, condition: String, required_properties: Vec<SessionProperty>) -> Self {
            Self {
                name,
                condition,
                required_properties,
            }
        }
    }
}
