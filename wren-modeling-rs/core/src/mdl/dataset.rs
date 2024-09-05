use crate::logical_plan::utils::map_data_type;
use crate::mdl::manifest::{Column, Metric, Model};
use crate::mdl::utils::quoted;
use crate::mdl::{RegisterTables, SessionStateRef};
use datafusion::arrow::datatypes::DataType::Utf8;
use datafusion::arrow::datatypes::Field;
use datafusion::common::Result;
use datafusion::common::{not_impl_err, DFSchema};
use datafusion::logical_expr::sqlparser::ast::Expr::CompoundIdentifier;
use datafusion::sql::sqlparser::ast::Expr::Identifier;
use datafusion::sql::sqlparser::ast::{visit_expressions, Expr, Ident};
use std::fmt::Display;
use std::ops::ControlFlow;
use std::sync::Arc;

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

impl Column {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn expression(&self) -> Option<&str> {
        self.expression.as_deref()
    }

    pub fn to_field(&self) -> Result<Field> {
        let data_type = map_data_type(&self.r#type)?;
        Ok(Field::new(&self.name, data_type, self.no_null))
    }

    pub fn to_remote_field(&self, session_state: SessionStateRef) -> Result<Vec<Field>> {
        if self.expression().is_some() {
            let session_state = session_state.read();
            let expr = session_state.sql_to_expr(
                self.expression().unwrap(),
                session_state.config_options().sql_parser.dialect.as_str(),
            )?;
            let columns = Self::collect_columns(expr);
            Ok(columns
                .into_iter()
                .map(|c| Field::new(c.value, Utf8, false))
                .collect())
        } else {
            Ok(vec![self.to_field()?])
        }
    }

    fn collect_columns(expr: Expr) -> Vec<Ident> {
        let mut visited = vec![];
        visit_expressions(&expr, |e| {
            if let CompoundIdentifier(ids) = e {
                ids.iter().cloned().for_each(|id| visited.push(id));
            } else if let Identifier(id) = e {
                visited.push(id.clone());
            }
            ControlFlow::<()>::Continue(())
        });
        visited
    }
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub enum Dataset {
    Model(Arc<Model>),
    Metric(Arc<Metric>),
    UnKnown,
}

impl Dataset {
    pub fn name(&self) -> &str {
        match self {
            Dataset::Model(model) => model.name(),
            Dataset::Metric(metric) => metric.name(),
            Dataset::UnKnown => unimplemented!("Unknown dataset"),
        }
    }

    pub fn try_as_model(&self) -> Option<Arc<Model>> {
        match self {
            Dataset::Model(model) => Some(Arc::clone(model)),
            _ => None,
        }
    }

    pub fn to_qualified_schema(&self) -> Result<DFSchema> {
        match self {
            Dataset::Model(model) => {
                let fields = model
                    .get_physical_columns()
                    .iter()
                    .map(|c| c.to_field())
                    .collect::<Result<Vec<_>>>()?;
                let arrow_schema = datafusion::arrow::datatypes::Schema::new(fields);
                DFSchema::try_from_qualified_schema(quoted(&model.name), &arrow_schema)
            }
            Dataset::Metric(_) => todo!(),
            Dataset::UnKnown => not_impl_err!("Unknown dataset"),
        }
    }

    /// Create the schema with the remote table name
    pub fn to_remote_schema(
        &self,
        register_tables: Option<&RegisterTables>,
        session_state: SessionStateRef,
    ) -> Result<DFSchema> {
        match self {
            Dataset::Model(model) => {
                let schema = register_tables
                    .map(|rt| rt.get(&model.table_reference))
                    .filter(|rt| rt.is_some())
                    .map(|rt| rt.unwrap().schema());

                if let Some(schema) = schema {
                    DFSchema::try_from_qualified_schema(&model.table_reference, &schema)
                } else {
                    let fields: Vec<Field> = model
                        .get_physical_columns()
                        .iter()
                        .filter(|c| !c.is_calculated)
                        .map(|c| c.to_remote_field(Arc::clone(&session_state)))
                        .collect::<Result<Vec<Vec<Field>>>>()?
                        .iter()
                        .flat_map(|c| c.clone())
                        .collect();
                    let arrow_schema = datafusion::arrow::datatypes::Schema::new(fields);

                    DFSchema::try_from_qualified_schema(
                        &model.table_reference,
                        &arrow_schema,
                    )
                }
            }
            Dataset::Metric(_) => todo!(),
            Dataset::UnKnown => not_impl_err!("Unknown dataset"),
        }
    }
}

impl Display for Dataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Dataset::Model(model) => write!(f, "{}", model.name()),
            Dataset::Metric(metric) => write!(f, "{}", metric.name()),
            Dataset::UnKnown => write!(f, "Unknown"),
        }
    }
}
