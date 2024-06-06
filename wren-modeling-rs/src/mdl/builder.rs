#![allow(dead_code)]

use crate::mdl::manifest::{
    Column, JoinType, Manifest, Metric, Model, Relationship, TimeGrain, TimeUnit, View,
};
use std::sync::Arc;

/// A builder for creating a Manifest
pub struct ManifestBuilder {
    pub manifest: Manifest,
}

impl ManifestBuilder {
    pub fn new() -> Self {
        Self {
            manifest: Manifest {
                catalog: "wrenai".to_string(),
                schema: "default".to_string(),
                models: vec![],
                relationships: vec![],
                metrics: vec![],
                views: vec![],
            },
        }
    }

    pub fn with_catalog(mut self, catalog: &str) -> Self {
        self.manifest.catalog = catalog.to_string();
        self
    }

    pub fn with_schema(mut self, schema: &str) -> Self {
        self.manifest.schema = schema.to_string();
        self
    }

    pub fn with_model(mut self, model: Arc<Model>) -> Self {
        self.manifest.models.push(model);
        self
    }

    pub fn with_relationship(mut self, relationship: Arc<Relationship>) -> Self {
        self.manifest.relationships.push(relationship);
        self
    }

    pub fn with_metric(mut self, metric: Arc<Metric>) -> Self {
        self.manifest.metrics.push(metric);
        self
    }

    pub fn with_view(mut self, view: Arc<View>) -> Self {
        self.manifest.views.push(view);
        self
    }

    pub fn build(self) -> Manifest {
        self.manifest
    }
}

pub struct ModelBuilder {
    pub model: Model,
}

impl ModelBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            model: Model {
                name: name.to_string(),
                ref_sql: "".to_string(),
                base_object: "".to_string(),
                table_reference: "".to_string(),
                columns: vec![],
                primary_key: "".to_string(),
                cached: false,
                refresh_time: "".to_string(),
                properties: vec![],
            },
        }
    }

    pub fn with_ref_sql(mut self, ref_sql: &str) -> Self {
        self.model.ref_sql = ref_sql.to_string();
        self
    }

    pub fn with_base_object(mut self, base_object: &str) -> Self {
        self.model.base_object = base_object.to_string();
        self
    }

    pub fn with_table_reference(mut self, table_reference: &str) -> Self {
        self.model.table_reference = table_reference.to_string();
        self
    }

    pub fn with_column(mut self, column: Arc<Column>) -> Self {
        self.model.columns.push(column);
        self
    }

    pub fn with_primary_key(mut self, primary_key: &str) -> Self {
        self.model.primary_key = primary_key.to_string();
        self
    }

    pub fn with_cached(mut self, cached: bool) -> Self {
        self.model.cached = cached;
        self
    }

    pub fn with_refresh_time(mut self, refresh_time: &str) -> Self {
        self.model.refresh_time = refresh_time.to_string();
        self
    }

    pub fn with_property(mut self, key: &str, value: &str) -> Self {
        self.model
            .properties
            .push((key.to_string(), value.to_string()));
        self
    }

    pub fn build(self) -> Arc<Model> {
        Arc::new(self.model)
    }
}

pub struct ColumnBuilder {
    pub column: Column,
}

impl ColumnBuilder {
    pub fn new(name: &str, r#type: &str) -> Self {
        Self {
            column: Column {
                name: name.to_string(),
                r#type: r#type.to_string(),
                relationship: None,
                is_calculated: false,
                no_null: false,
                expression: None,
                properties: vec![],
            },
        }
    }

    pub fn with_relationship(mut self, relationship: &str) -> Self {
        self.column.relationship = Some(relationship.to_string());
        self
    }

    pub fn with_is_calculated(mut self, is_calculated: bool) -> Self {
        self.column.is_calculated = is_calculated;
        self
    }

    pub fn with_no_null(mut self, no_null: bool) -> Self {
        self.column.no_null = no_null;
        self
    }

    pub fn with_expression(mut self, expression: &str) -> Self {
        self.column.expression = Some(expression.to_string());
        self
    }

    pub fn with_property(mut self, key: &str, value: &str) -> Self {
        self.column
            .properties
            .push((key.to_string(), value.to_string()));
        self
    }

    pub fn build(self) -> Arc<Column> {
        Arc::new(self.column)
    }
}

pub struct RelationshipBuilder {
    pub relationship: Relationship,
}

impl RelationshipBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            relationship: Relationship {
                name: name.to_string(),
                models: vec![],
                join_type: JoinType::OneToOne,
                condition: "".to_string(),
                properties: vec![],
            },
        }
    }

    pub fn with_model(mut self, model: &str) -> Self {
        self.relationship.models.push(model.to_string());
        self
    }

    pub fn with_join_type(mut self, join_type: JoinType) -> Self {
        self.relationship.join_type = join_type;
        self
    }

    pub fn with_condition(mut self, condition: &str) -> Self {
        self.relationship.condition = condition.to_string();
        self
    }

    pub fn with_property(mut self, key: &str, value: &str) -> Self {
        self.relationship
            .properties
            .push((key.to_string(), value.to_string()));
        self
    }

    pub fn build(self) -> Arc<Relationship> {
        Arc::new(self.relationship)
    }
}

pub struct MetricBuilder {
    pub metric: Metric,
}

impl MetricBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            metric: Metric {
                name: name.to_string(),
                base_object: "".to_string(),
                dimension: vec![],
                measure: vec![],
                time_grain: vec![],
                cached: false,
                refresh_time: "".to_string(),
                properties: vec![],
            },
        }
    }

    pub fn with_dimension(mut self, dimension: Arc<Column>) -> Self {
        self.metric.dimension.push(dimension);
        self
    }

    pub fn with_measure(mut self, measure: Arc<Column>) -> Self {
        self.metric.measure.push(measure);
        self
    }

    pub fn with_time_grain(mut self, time_grain: TimeGrain) -> Self {
        self.metric.time_grain.push(time_grain);
        self
    }

    pub fn with_cached(mut self, cached: bool) -> Self {
        self.metric.cached = cached;
        self
    }

    pub fn with_refresh_time(mut self, refresh_time: &str) -> Self {
        self.metric.refresh_time = refresh_time.to_string();
        self
    }

    pub fn with_property(mut self, key: &str, value: &str) -> Self {
        self.metric
            .properties
            .push((key.to_string(), value.to_string()));
        self
    }

    pub fn build(self) -> Arc<Metric> {
        Arc::new(self.metric)
    }
}

pub struct TimeGrainBuilder {
    pub time_grain: TimeGrain,
}

impl TimeGrainBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            time_grain: TimeGrain {
                name: name.to_string(),
                ref_column: "".to_string(),
                date_parts: vec![],
            },
        }
    }

    pub fn with_ref_column(mut self, ref_column: &str) -> Self {
        self.time_grain.ref_column = ref_column.to_string();
        self
    }

    pub fn with_date_part(mut self, date_part: TimeUnit) -> Self {
        self.time_grain.date_parts.push(date_part);
        self
    }

    pub fn build(self) -> TimeGrain {
        self.time_grain
    }
}

pub struct ViewBuilder {
    pub view: View,
}

impl ViewBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            view: View {
                name: name.to_string(),
                statement: "".to_string(),
                properties: vec![],
            },
        }
    }

    pub fn with_statement(mut self, statement: &str) -> Self {
        self.view.statement = statement.to_string();
        self
    }

    pub fn with_property(mut self, key: &str, value: &str) -> Self {
        self.view
            .properties
            .push((key.to_string(), value.to_string()));
        self
    }

    pub fn build(self) -> Arc<View> {
        Arc::new(self.view)
    }
}

#[cfg(test)]
mod test {
    use crate::mdl::builder::{
        ColumnBuilder, MetricBuilder, ModelBuilder, RelationshipBuilder, TimeGrainBuilder,
        ViewBuilder,
    };
    use crate::mdl::manifest::{Column, JoinType, Metric, Model, Relationship, TimeUnit, View};
    use std::sync::Arc;

    #[test]
    fn test_column_roundtrip() {
        let expected = ColumnBuilder::new("id", "integer")
            .with_relationship("test")
            .with_is_calculated(true)
            .with_no_null(true)
            .with_expression("test")
            .with_property("key", "value")
            .build();

        let json_str = serde_json::to_string(&expected).unwrap();
        let actual: Arc<Column> = serde_json::from_str(&json_str).unwrap();
        assert_eq!(actual, expected)
    }

    #[test]
    fn test_model_roundtrip() {
        let model = ModelBuilder::new("test")
            .with_ref_sql("SELECT * FROM test")
            .with_base_object("test")
            .with_table_reference("test")
            .with_column(ColumnBuilder::new("id", "integer").build())
            .with_primary_key("id")
            .with_cached(true)
            .with_refresh_time("1h")
            .with_property("key", "value")
            .build();

        let json_str = serde_json::to_string(&model).unwrap();
        let actual: Arc<Model> = serde_json::from_str(&json_str).unwrap();
        assert_eq!(actual, model)
    }

    #[test]
    fn test_relationship_roundtrip() {
        let expected = RelationshipBuilder::new("test")
            .with_model("testA")
            .with_model("testB")
            .with_join_type(JoinType::OneToMany)
            .with_condition("test")
            .with_property("key", "value")
            .build();

        let json_str = serde_json::to_string(&expected).unwrap();
        let actual: Arc<Relationship> = serde_json::from_str(&json_str).unwrap();
        assert_eq!(actual, expected)
    }

    #[test]
    fn test_metric_roundtrip() {
        let model = MetricBuilder::new("test")
            .with_dimension(ColumnBuilder::new("dim", "integer").build())
            .with_measure(ColumnBuilder::new("mea", "integer").build())
            .with_time_grain(
                TimeGrainBuilder::new("tg")
                    .with_ref_column("tg")
                    .with_date_part(TimeUnit::Day)
                    .build(),
            )
            .with_cached(true)
            .with_refresh_time("1h")
            .with_property("key", "value")
            .build();

        let json_str = serde_json::to_string(&model).unwrap();
        let actual: Arc<Metric> = serde_json::from_str(&json_str).unwrap();
        assert_eq!(actual, model)
    }

    #[test]
    fn test_view_roundtrip() {
        let expected = ViewBuilder::new("test")
            .with_statement("SELECT * FROM test")
            .with_property("key", "value")
            .build();

        let json_str = serde_json::to_string(&expected).unwrap();
        let actual: Arc<View> = serde_json::from_str(&json_str).unwrap();
        assert_eq!(actual, expected)
    }

    #[test]
    fn test_manifest_roundtrip() {
        let model = ModelBuilder::new("test")
            .with_ref_sql("SELECT * FROM test")
            .with_base_object("test")
            .with_table_reference("test")
            .with_column(ColumnBuilder::new("id", "integer").build())
            .with_primary_key("id")
            .with_cached(true)
            .with_refresh_time("1h")
            .with_property("key", "value")
            .build();

        let relationship = RelationshipBuilder::new("test")
            .with_model("testA")
            .with_model("testB")
            .with_join_type(JoinType::OneToMany)
            .with_condition("test")
            .with_property("key", "value")
            .build();

        let metric = MetricBuilder::new("test")
            .with_dimension(ColumnBuilder::new("dim", "integer").build())
            .with_measure(ColumnBuilder::new("mea", "integer").build())
            .with_time_grain(
                TimeGrainBuilder::new("tg")
                    .with_ref_column("tg")
                    .with_date_part(TimeUnit::Day)
                    .build(),
            )
            .with_cached(true)
            .with_refresh_time("1h")
            .with_property("key", "value")
            .build();

        let view = ViewBuilder::new("test")
            .with_statement("SELECT * FROM test")
            .with_property("key", "value")
            .build();

        let expected = crate::mdl::builder::ManifestBuilder::new()
            .with_catalog("wrenai")
            .with_schema("default")
            .with_model(model)
            .with_relationship(relationship)
            .with_metric(metric)
            .with_view(view)
            .build();

        let json_str = serde_json::to_string(&expected).unwrap();
        let actual: crate::mdl::manifest::Manifest = serde_json::from_str(&json_str).unwrap();
        assert_eq!(actual, expected)
    }
}
