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

#![allow(dead_code)]

use crate::mdl::manifest::{
    Column, DataSource, JoinType, Manifest, Metric, Model, Relationship, TimeGrain, TimeUnit, View,
};
#[allow(deprecated)]
use crate::mdl::{
    ColumnLevelOperator, ColumnLevelSecurity, NormalizedExpr, RowLevelAccessControl,
    RowLevelOperator, RowLevelSecurity, SessionProperty,
};
use std::sync::Arc;

use super::ColumnLevelAccessControl;

/// A builder for creating a Manifest
pub struct ManifestBuilder {
    pub manifest: Manifest,
}

impl Default for ManifestBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ManifestBuilder {
    pub fn new() -> Self {
        Self {
            manifest: Manifest {
                catalog: "wrenai".to_string(),
                schema: "public".to_string(),
                models: vec![],
                relationships: vec![],
                metrics: vec![],
                views: vec![],
                data_source: None,
            },
        }
    }

    pub fn catalog(mut self, catalog: &str) -> Self {
        self.manifest.catalog = catalog.to_string();
        self
    }

    pub fn schema(mut self, schema: &str) -> Self {
        self.manifest.schema = schema.to_string();
        self
    }

    pub fn model(mut self, model: Arc<Model>) -> Self {
        self.manifest.models.push(model);
        self
    }

    pub fn relationship(mut self, relationship: Arc<Relationship>) -> Self {
        self.manifest.relationships.push(relationship);
        self
    }

    pub fn metric(mut self, metric: Arc<Metric>) -> Self {
        self.manifest.metrics.push(metric);
        self
    }

    pub fn view(mut self, view: Arc<View>) -> Self {
        self.manifest.views.push(view);
        self
    }

    pub fn data_source(mut self, data_source: DataSource) -> Self {
        self.manifest.data_source = Some(data_source);
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
                ref_sql: None,
                base_object: None,
                table_reference: None,
                columns: vec![],
                primary_key: None,
                cached: false,
                refresh_time: None,
                row_level_access_controls: vec![],
            },
        }
    }

    pub fn ref_sql(mut self, ref_sql: &str) -> Self {
        self.model.ref_sql = Some(ref_sql.to_string());
        self
    }

    pub fn base_object(mut self, base_object: &str) -> Self {
        self.model.base_object = Some(base_object.to_string());
        self
    }

    pub fn table_reference(mut self, table_reference: &str) -> Self {
        self.model.table_reference = Some(table_reference.to_string());
        self
    }

    pub fn column(mut self, column: Arc<Column>) -> Self {
        self.model.columns.push(column);
        self
    }

    pub fn primary_key(mut self, primary_key: &str) -> Self {
        self.model.primary_key = Some(primary_key.to_string());
        self
    }

    pub fn cached(mut self, cached: bool) -> Self {
        self.model.cached = cached;
        self
    }

    pub fn refresh_time(mut self, refresh_time: &str) -> Self {
        self.model.refresh_time = Some(refresh_time.to_string());
        self
    }

    pub fn add_row_level_access_control(
        mut self,
        name: &str,
        required_properties: Vec<SessionProperty>,
        condition: &str,
    ) -> Self {
        let rule = RowLevelAccessControl {
            name: name.to_string(),
            required_properties,
            condition: condition.to_string(),
        };
        self.model.row_level_access_controls.push(Arc::new(rule));
        self
    }

    pub fn build(self) -> Arc<Model> {
        Arc::new(self.model)
    }
}

impl SessionProperty {
    pub fn new_required(name: &str) -> Self {
        SessionProperty::new(name.to_string(), true, None)
    }
    pub fn new_optional(name: &str, default_expr: Option<String>) -> Self {
        SessionProperty::new(name.to_string(), false, default_expr)
    }
}
pub struct ColumnBuilder {
    pub column: Column,
}

#[allow(deprecated)]
impl ColumnBuilder {
    pub fn new(name: &str, r#type: &str) -> Self {
        Self {
            column: Column {
                name: name.to_string(),
                r#type: r#type.to_string(),
                relationship: None,
                is_calculated: false,
                is_hidden: false,
                not_null: false,
                expression: None,
                rls: None,
                cls: None,
                column_level_access_control: None,
            },
        }
    }

    pub fn new_calculated(name: &str, r#type: &str) -> Self {
        Self::new(name, r#type).calculated(true)
    }

    pub fn new_relationship(name: &str, r#type: &str, relationship: &str) -> Self {
        Self::new(name, r#type).relationship(relationship)
    }

    pub fn relationship(mut self, relationship: &str) -> Self {
        self.column.relationship = Some(relationship.to_string());
        self
    }

    pub fn calculated(mut self, is_calculated: bool) -> Self {
        self.column.is_calculated = is_calculated;
        self
    }

    pub fn not_null(mut self, not_null: bool) -> Self {
        self.column.not_null = not_null;
        self
    }

    pub fn expression(mut self, expression: &str) -> Self {
        self.column.expression = Some(expression.to_string());
        self
    }

    pub fn hidden(mut self, is_hidden: bool) -> Self {
        self.column.is_hidden = is_hidden;
        self
    }

    #[allow(deprecated)]
    pub fn row_level_security(mut self, name: &str, operator: RowLevelOperator) -> Self {
        self.column.rls = Some(RowLevelSecurity {
            name: name.to_string(),
            operator,
        });
        self
    }

    pub fn column_level_security(
        mut self,
        name: &str,
        operator: ColumnLevelOperator,
        threshold: &str,
    ) -> Self {
        self.column.cls = Some(ColumnLevelSecurity {
            name: name.to_string(),
            operator,
            threshold: NormalizedExpr::new(threshold),
        });
        self
    }

    pub fn column_level_access_control(
        mut self,
        name: &str,
        required_properties: Vec<SessionProperty>,
        operator: ColumnLevelOperator,
        threshold: &str,
    ) -> Self {
        self.column.column_level_access_control = Some(Arc::new(ColumnLevelAccessControl {
            name: name.to_string(),
            required_properties,
            operator,
            threshold: NormalizedExpr::new(threshold),
        }));
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
            },
        }
    }

    pub fn model(mut self, model: &str) -> Self {
        self.relationship.models.push(model.to_string());
        self
    }

    pub fn join_type(mut self, join_type: JoinType) -> Self {
        self.relationship.join_type = join_type;
        self
    }

    pub fn condition(mut self, condition: &str) -> Self {
        self.relationship.condition = condition.to_string();
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
                refresh_time: None,
            },
        }
    }

    pub fn dimension(mut self, dimension: Arc<Column>) -> Self {
        self.metric.dimension.push(dimension);
        self
    }

    pub fn measure(mut self, measure: Arc<Column>) -> Self {
        self.metric.measure.push(measure);
        self
    }

    pub fn time_grain(mut self, time_grain: TimeGrain) -> Self {
        self.metric.time_grain.push(time_grain);
        self
    }

    pub fn cached(mut self, cached: bool) -> Self {
        self.metric.cached = cached;
        self
    }

    pub fn refresh_time(mut self, refresh_time: &str) -> Self {
        self.metric.refresh_time = Some(refresh_time.to_string());
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

    pub fn ref_column(mut self, ref_column: &str) -> Self {
        self.time_grain.ref_column = ref_column.to_string();
        self
    }

    pub fn date_part(mut self, date_part: TimeUnit) -> Self {
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
            },
        }
    }

    pub fn statement(mut self, statement: &str) -> Self {
        self.view.statement = statement.to_string();
        self
    }

    pub fn build(self) -> Arc<View> {
        Arc::new(self.view)
    }
}

#[cfg(test)]
mod test {
    use crate::mdl::builder::{
        ColumnBuilder, ManifestBuilder, MetricBuilder, ModelBuilder, RelationshipBuilder,
        TimeGrainBuilder, ViewBuilder,
    };
    use crate::mdl::manifest::DataSource::MySQL;
    use crate::mdl::manifest::{
        Column, DataSource, JoinType, Manifest, Metric, Model, Relationship, TimeUnit, View,
    };
    use crate::mdl::ColumnLevelOperator;
    #[allow(deprecated)]
    use crate::mdl::RowLevelOperator;
    use crate::mdl::SessionProperty;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;

    #[test]
    #[allow(deprecated)]
    fn test_column_roundtrip() {
        let expected = ColumnBuilder::new("id", "integer")
            .relationship("test")
            .calculated(true)
            .not_null(true)
            .hidden(true)
            .expression("test")
            .row_level_security("SESSION_STATUS", RowLevelOperator::Equals)
            .column_level_security("SESSION_LEVEL", ColumnLevelOperator::Equals, "'NORMAL'")
            .column_level_access_control(
                "rlac",
                vec![SessionProperty::new_required("session_id")],
                ColumnLevelOperator::Equals,
                "'NORMAL'",
            )
            .build();

        let json_str = serde_json::to_string(&expected).unwrap();
        let actual: Arc<Column> = serde_json::from_str(&json_str).unwrap();
        assert_eq!(actual, expected)
    }

    #[test]
    fn test_expression_empty_as_none() {
        let expected = ColumnBuilder::new("id", "integer").expression("").build();

        let json_str = serde_json::to_string(&expected).unwrap();
        let actual: Arc<Column> = serde_json::from_str(&json_str).unwrap();
        assert!(actual.expression.is_none())
    }

    #[test]
    fn test_bool_from_int() {
        let json = r#"
        {
            "name": "id",
            "type": "integer",
            "isCalculated": 1,
            "notNull": 0
        }
        "#;

        let actual: Arc<Column> = serde_json::from_str(json).unwrap();
        assert!(actual.is_calculated);
        assert!(!actual.not_null);

        let json = r#"
        {
            "name": "id",
            "type": "integer",
            "isCalculated": true,
            "notNull": false
        }
        "#;

        let actual: Arc<Column> = serde_json::from_str(json).unwrap();
        assert!(actual.is_calculated);
        assert!(!actual.not_null);
    }

    #[test]
    fn test_model_roundtrip() {
        let model = ModelBuilder::new("test")
            .ref_sql("SELECT * FROM test")
            .base_object("test")
            .table_reference("test")
            .column(ColumnBuilder::new("id", "integer").build())
            .primary_key("id")
            .cached(true)
            .refresh_time("1h")
            .add_row_level_access_control(
                "rule1",
                vec![SessionProperty::new_required("session_id")],
                "id = @session_id",
            )
            .add_row_level_access_control(
                "rule2",
                vec![SessionProperty::new_optional("session_id_optional", None)],
                "id = @session_id_optional",
            )
            .add_row_level_access_control(
                "rule3",
                vec![SessionProperty::new_optional(
                    "session_id_default",
                    Some("1".to_string()),
                )],
                "id = @session_id_default",
            )
            .build();

        let json_str = serde_json::to_string(&model).unwrap();
        let actual: Arc<Model> = serde_json::from_str(&json_str).unwrap();
        assert_eq!(actual, model);

        // test table_reference is null
        let model = ModelBuilder::new("test")
            .ref_sql("SELECT * FROM test")
            .base_object("test")
            .column(ColumnBuilder::new("id", "integer").build())
            .primary_key("id")
            .cached(true)
            .refresh_time("1h")
            .build();

        let json_str = serde_json::to_string(&model).unwrap();
        let actual: Arc<Model> = serde_json::from_str(&json_str).unwrap();
        assert_eq!(actual, model);

        // test only table_reference
        let model = ModelBuilder::new("test")
            .table_reference("test")
            .column(ColumnBuilder::new("id", "integer").build())
            .build();

        let json_str = serde_json::to_string(&model).unwrap();
        let actual: Arc<Model> = serde_json::from_str(&json_str).unwrap();
        assert_eq!(actual, model);

        let model = ModelBuilder::new("test")
            .table_reference(r#""Wren"."Public"."Source""#)
            .column(ColumnBuilder::new("id", "integer").build())
            .build();

        let json_str = serde_json::to_string(&model).unwrap();
        let actual: Arc<Model> = serde_json::from_str(&json_str).unwrap();
        assert_eq!(actual, model);
    }

    #[test]
    fn test_relationship_roundtrip() {
        let expected = RelationshipBuilder::new("test")
            .model("testA")
            .model("testB")
            .join_type(JoinType::OneToMany)
            .condition("test")
            .build();

        let json_str = serde_json::to_string(&expected).unwrap();
        let actual: Arc<Relationship> = serde_json::from_str(&json_str).unwrap();
        assert_eq!(actual, expected)
    }

    #[test]
    fn test_join_type_case_insensitive() {
        let case = ["one_to_one", "ONE_TO_ONE"];
        let expected = JoinType::OneToOne;
        for case in case.iter() {
            assert_serde(&format!("\"{}\"", case), expected);
        }

        let case = ["one_to_many", "ONE_TO_MANY"];
        let expected = JoinType::OneToMany;
        for case in case.iter() {
            assert_serde(&format!("\"{}\"", case), expected);
        }

        let case = ["many_to_one", "MANY_TO_ONE"];
        let expected = JoinType::ManyToOne;
        for case in case.iter() {
            assert_serde(&format!("\"{}\"", case), expected);
        }

        let case = ["many_to_many", "MANY_TO_MANY"];
        let expected = JoinType::ManyToMany;
        for case in case.iter() {
            assert_serde(&format!("\"{}\"", case), expected);
        }
    }

    fn assert_serde(json_str: &str, expected: JoinType) {
        let actual: JoinType = serde_json::from_str(json_str).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_metric_roundtrip() {
        let model = MetricBuilder::new("test")
            .dimension(ColumnBuilder::new("dim", "integer").build())
            .measure(ColumnBuilder::new("mea", "integer").build())
            .time_grain(
                TimeGrainBuilder::new("tg")
                    .ref_column("tg")
                    .date_part(TimeUnit::Day)
                    .build(),
            )
            .cached(true)
            .refresh_time("1h")
            .build();

        let json_str = serde_json::to_string(&model).unwrap();
        let actual: Arc<Metric> = serde_json::from_str(&json_str).unwrap();
        assert_eq!(actual, model)
    }

    #[test]
    fn test_view_roundtrip() {
        let expected = ViewBuilder::new("test")
            .statement("SELECT * FROM test")
            .build();

        let json_str = serde_json::to_string(&expected).unwrap();
        let actual: Arc<View> = serde_json::from_str(&json_str).unwrap();
        assert_eq!(actual, expected)
    }

    #[test]
    fn test_manifest_roundtrip() {
        let model = ModelBuilder::new("test")
            .ref_sql("SELECT * FROM test")
            .base_object("test")
            .table_reference("test")
            .column(ColumnBuilder::new("id", "integer").build())
            .primary_key("id")
            .cached(true)
            .refresh_time("1h")
            .build();

        let relationship = RelationshipBuilder::new("test")
            .model("testA")
            .model("testB")
            .join_type(JoinType::OneToMany)
            .condition("test")
            .build();

        let metric = MetricBuilder::new("test")
            .dimension(ColumnBuilder::new("dim", "integer").build())
            .measure(ColumnBuilder::new("mea", "integer").build())
            .time_grain(
                TimeGrainBuilder::new("tg")
                    .ref_column("tg")
                    .date_part(TimeUnit::Day)
                    .build(),
            )
            .cached(true)
            .refresh_time("1h")
            .build();

        let view = ViewBuilder::new("test")
            .statement("SELECT * FROM test")
            .build();

        let expected = crate::mdl::builder::ManifestBuilder::new()
            .catalog("wrenai")
            .schema("public")
            .model(model)
            .relationship(relationship)
            .metric(metric)
            .view(view)
            .data_source(DataSource::Datafusion)
            .build();

        let json_str = serde_json::to_string(&expected).unwrap();
        let actual: crate::mdl::manifest::Manifest = serde_json::from_str(&json_str).unwrap();
        assert_eq!(actual, expected)
    }

    #[test]
    #[allow(deprecated)]
    fn test_json_serde() {
        let test_data: PathBuf = [env!("CARGO_MANIFEST_DIR"), "tests", "data", "mdl.json"]
            .iter()
            .collect();
        let mdl_json = fs::read_to_string(test_data.as_path()).unwrap();
        let mdl = serde_json::from_str::<Manifest>(&mdl_json).unwrap();

        let expected = ManifestBuilder::new()
            .catalog("test")
            .schema("test")
            .model(
                ModelBuilder::new("customer")
                    .table_reference("customer")
                    .column(ColumnBuilder::new("c_custkey", "integer").build())
                    .column(ColumnBuilder::new("c_name", "varchar").build())
                    .column(
                        ColumnBuilder::new("custkey_plus", "integer")
                            .expression("c_custkey + 1")
                            .calculated(true)
                            .build(),
                    )
                    .column(
                        ColumnBuilder::new("orders", "orders")
                            .relationship("CustomerOrders")
                            .build(),
                    )
                    .add_row_level_access_control(
                        "rule1",
                        vec![SessionProperty::new_required("session_id")],
                        "c_custkey = @session_id",
                    )
                    .add_row_level_access_control(
                        "rule2",
                        vec![SessionProperty::new_optional("session_id_optional", None)],
                        "c_custkey = @session_id_optional",
                    )
                    .add_row_level_access_control(
                        "rule3",
                        vec![SessionProperty::new_optional(
                            "session_id_default",
                            Some("1".to_string()),
                        )],
                        "c_custkey = @session_id_default",
                    )
                    .primary_key("c_custkey")
                    .build(),
            )
            .model(
                ModelBuilder::new("profile")
                    .table_reference("profile")
                    .column(ColumnBuilder::new("p_custkey", "integer").build())
                    .column(ColumnBuilder::new("p_phone", "varchar").build())
                    .column(ColumnBuilder::new("p_sex", "varchar").build())
                    .column(
                        ColumnBuilder::new("customer", "customer")
                            .relationship("CustomerProfile")
                            .build(),
                    )
                    .column(
                        ColumnBuilder::new("totalcost", "integer")
                            .expression("sum(customer.orders.o_totalprice)")
                            .calculated(true)
                            .build(),
                    )
                    .primary_key("p_custkey")
                    .build(),
            )
            .model(
                ModelBuilder::new("orders")
                    .table_reference("orders")
                    .column(ColumnBuilder::new("o_orderkey", "integer").build())
                    .column(ColumnBuilder::new("o_custkey", "integer").build())
                    .column(ColumnBuilder::new("o_totalprice", "integer").build())
                    .column(
                        ColumnBuilder::new("customer", "customer")
                            .relationship("CustomerOrders")
                            .build(),
                    )
                    .column(
                        ColumnBuilder::new("customer_name", "varchar")
                            .expression("customer.c_name")
                            .calculated(true)
                            .build(),
                    )
                    .column(
                        ColumnBuilder::new("orderkey_plus_custkey", "integer")
                            .expression("o_orderkey + o_custkey")
                            .calculated(true)
                            .build(),
                    )
                    .column(
                        ColumnBuilder::new("hash_orderkey", "varchar")
                            .expression("md5(o_orderkey)")
                            .calculated(true)
                            .build(),
                    )
                    .column(
                        ColumnBuilder::new("rls_orderkey", "integer")
                            .row_level_security("SESSION_STATUS", RowLevelOperator::Equals)
                            .expression("o_orderkey")
                            .build(),
                    )
                    .column(
                        ColumnBuilder::new("cls_orderkey", "integer")
                            .column_level_security(
                                "SESSION_LEVEL",
                                ColumnLevelOperator::Equals,
                                "'NORMAL'",
                            )
                            .expression("o_orderkey")
                            .build(),
                    )
                    .primary_key("o_orderkey")
                    .build(),
            )
            .relationship(
                RelationshipBuilder::new("CustomerOrders")
                    .model("customer")
                    .model("orders")
                    .join_type(JoinType::OneToMany)
                    .condition("customer.c_custkey = orders.o_custkey")
                    .build(),
            )
            .relationship(
                RelationshipBuilder::new("CustomerProfile")
                    .model("customer")
                    .model("profile")
                    .join_type(JoinType::OneToOne)
                    .condition("customer.c_custkey = profile.p_custkey")
                    .build(),
            )
            .view(
                ViewBuilder::new("customer_view")
                    .statement("select * from test.test.customer")
                    .build(),
            )
            .data_source(MySQL);
        assert_eq!(mdl, expected.build());
    }

    #[test]
    fn test_session_property_roundtrip() {
        let expected = SessionProperty::new_optional("session_id", Some("1".to_string()));

        let json_str = serde_json::to_string(&expected).unwrap();
        assert!(!json_str.contains(r#"normalizedName"#));
        let actual: SessionProperty = serde_json::from_str(&json_str).unwrap();
        assert_eq!(actual.normalized_name(), actual.name.to_lowercase());
        assert_eq!(actual, expected)
    }
}
