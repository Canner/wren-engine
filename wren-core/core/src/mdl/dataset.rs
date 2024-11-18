use crate::logical_plan::utils::map_data_type;
use crate::mdl::manifest::{Column, Metric, Model};
use crate::mdl::utils::quoted;
use crate::mdl::{RegisterTables, SessionStateRef};
use datafusion::arrow::datatypes::Field;
use datafusion::common::DFSchema;
use datafusion::common::Result;
use datafusion::logical_expr::sqlparser::ast::Expr::CompoundIdentifier;
use datafusion::sql::sqlparser::ast::Expr::Identifier;
use datafusion::sql::sqlparser::ast::{visit_expressions, Expr, Ident};
use std::fmt::Display;
use std::ops::ControlFlow;
use std::sync::Arc;

impl Model {
    /// Physical columns are columns that can be selected from the model.
    /// All physical columns are visible columns, but not all visible columns are physical columns
    /// e.g. columns that are not a relationship column
    pub fn get_physical_columns(&self) -> Vec<Arc<Column>> {
        self.get_visible_columns()
            .filter(|c| c.relationship.is_none())
            .map(|c| Arc::clone(&c))
            .collect()
    }

    /// Return the name of the model
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the iterator of all visible columns
    pub fn get_visible_columns(&self) -> impl Iterator<Item = Arc<Column>> + '_ {
        self.columns.iter().filter(|f| !f.is_hidden).map(Arc::clone)
    }

    /// Get the specified visible column by name
    pub fn get_column(&self, column_name: &str) -> Option<Arc<Column>> {
        self.get_visible_columns()
            .find(|c| c.name == column_name)
            .map(|c| Arc::clone(&c))
    }

    /// Return the primary key of the model
    pub fn primary_key(&self) -> Option<&str> {
        self.primary_key.as_deref()
    }
}

impl Column {
    /// Return the name of the column
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the expression of the column
    pub fn expression(&self) -> Option<&str> {
        self.expression.as_deref()
    }

    /// Transform the column to a datafusion field
    pub fn to_field(&self) -> Result<Field> {
        let data_type = map_data_type(&self.r#type)?;
        Ok(Field::new(&self.name, data_type, self.not_null))
    }

    /// Transform the column to a datafusion field for a remote table
    pub fn to_remote_field(&self, session_state: SessionStateRef) -> Result<Vec<Field>> {
        if self.expression().is_some() {
            let session_state = session_state.read();
            let expr = session_state.sql_to_expr(
                self.expression().unwrap(),
                session_state.config_options().sql_parser.dialect.as_str(),
            )?;
            let columns = Self::collect_columns(expr);
            columns
                .into_iter()
                .map(|c| Ok(Field::new(c.value, map_data_type(&self.r#type)?, false)))
                .collect::<Result<_>>()
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

    pub fn to_qualified_schema(&self) -> Result<DFSchema> {
        match self {
            Dataset::Model(model) => {
                let fields: Vec<_> = model
                    .get_physical_columns()
                    .iter()
                    .map(|c| c.to_field())
                    .collect::<Result<_>>()?;
                let arrow_schema = datafusion::arrow::datatypes::Schema::new(fields);
                DFSchema::try_from_qualified_schema(quoted(&model.name), &arrow_schema)
            }
            Dataset::Metric(_) => todo!(),
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
                    .map(|rt| rt.get(model.table_reference()))
                    .filter(|rt| rt.is_some())
                    .map(|rt| rt.unwrap().schema());

                if let Some(schema) = schema {
                    DFSchema::try_from_qualified_schema(model.table_reference(), &schema)
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
                        model.table_reference(),
                        &arrow_schema,
                    )
                }
            }
            Dataset::Metric(_) => todo!(),
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
