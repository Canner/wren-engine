/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.accio.main.pgcatalog.builder;

import io.accio.base.AccioMDL;
import io.accio.base.dto.JoinType;
import io.accio.base.dto.Manifest;
import io.accio.base.dto.Relationship;
import io.accio.main.TestingMetadata;
import io.accio.main.metadata.Metadata;
import org.testng.annotations.Test;

import java.util.List;

import static io.accio.base.dto.Column.column;
import static io.accio.base.dto.EnumDefinition.enumDefinition;
import static io.accio.base.dto.EnumValue.enumValue;
import static io.accio.base.dto.Metric.metric;
import static io.accio.base.dto.Model.model;
import static io.accio.base.dto.Relationship.SortKey.sortKey;
import static io.accio.base.dto.Relationship.relationship;
import static io.accio.base.dto.TimeGrain.TimeUnit.DAY;
import static io.accio.base.dto.TimeGrain.TimeUnit.MONTH;
import static io.accio.base.dto.TimeGrain.timeGrain;
import static io.accio.base.dto.View.view;
import static io.accio.main.pgcatalog.builder.BigQueryUtils.createOrReplaceAllColumn;
import static io.accio.main.pgcatalog.builder.BigQueryUtils.createOrReplaceAllTable;
import static io.accio.main.pgcatalog.builder.BigQueryUtils.createOrReplacePgTypeMapping;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCreateBigQueryTempTable
{
    private final Metadata metadata;
    private final AccioMDL accioMDL;

    public TestCreateBigQueryTempTable()
    {
        this.metadata = new TestingMetadata();
        this.accioMDL = AccioMDL.fromManifest(
                Manifest.builder()
                        .setCatalog("accio_catalog")
                        .setSchema("accio_schema")
                        .setModels(List.of(
                                model("OrdersModel",
                                        "select * from orders",
                                        List.of(
                                                column("orderkey", "integer", null, true, "the key of each order"),
                                                column("custkey", "integer", null, true),
                                                column("orderstatus", "string", null, true),
                                                column("totalprice", "double", null, true),
                                                column("orderdate", "date", null, true),
                                                column("orderpriority", "string", null, true),
                                                column("clerk", "string", null, true),
                                                column("shippriority", "integer", null, true),
                                                column("comment", "string", null, true),
                                                column("customer", "CustomerModel", "OrdersCustomer", true)),
                                        "orderkey",
                                        "tpch tiny orders table"),
                                model("LineitemModel",
                                        "select * from lineitem",
                                        List.of(
                                                column("orderkey", "integer", null, true),
                                                column("linenumber", "integer", null, true),
                                                column("extendedprice", "integer", null, true))),
                                model("CustomerModel",
                                        "select * from customer",
                                        List.of(
                                                column("custkey", "integer", null, true),
                                                column("name", "string", null, true),
                                                column("address", "string", null, true),
                                                column("nationkey", "integer", null, true),
                                                column("phone", "string", null, true),
                                                column("acctbal", "double", null, true),
                                                column("mktsegment", "string", null, true),
                                                column("comment", "string", null, true),
                                                column("orders", "OrdersModel", "OrdersCustomer", true)),
                                        "custkey")))
                        .setRelationships(List.of(
                                relationship("OrdersCustomer",
                                        List.of("OrdersModel", "CustomerModel"),
                                        JoinType.MANY_TO_ONE,
                                        "OrdersModel.custkey = CustomerModel.custkey",
                                        List.of(sortKey("orderkey", Relationship.SortKey.Ordering.ASC)),
                                        "the relationship between orders and customers")))
                        .setEnumDefinitions(List.of(
                                enumDefinition("OrderStatus", List.of(
                                                enumValue("PENDING", "pending"),
                                                enumValue("PROCESSING", "processing"),
                                                enumValue("SHIPPED", "shipped"),
                                                enumValue("COMPLETE", "complete")),
                                        "the status of an order")))
                        .setMetrics(List.of(metric("Revenue", "OrdersModel",
                                List.of(column("orderkey", "string", null, true)),
                                List.of(column("total", "integer", null, true)),
                                List.of(timeGrain("orderdate", "orderdate", List.of(DAY, MONTH))),
                                true, "the revenue of an order")))
                        .setViews(List.of(view("useMetric", "select * from Revenue", "the view for the revenue metric")))
                        .build());
    }

    @Test
    public void testAllColumns()
    {
        assertThat(createOrReplaceAllColumn(metadata))
                .isEqualTo("CREATE OR REPLACE VIEW `accio_temp.all_columns` AS " +
                        "SELECT 'testing_schema1' as table_schema, col.column_name, col.ordinal_position, col.table_name, ptype.oid as typoid, ptype.typlen " +
                        "FROM `testing_schema1`.INFORMATION_SCHEMA.COLUMNS col " +
                        "LEFT JOIN `accio_temp.pg_type_mapping` mapping ON col.data_type = mapping.bq_type " +
                        "LEFT JOIN `pg_catalog.pg_type` ptype ON mapping.oid = ptype.oid " +
                        "UNION ALL SELECT 'testing_schema2' as table_schema, col.column_name, col.ordinal_position, col.table_name, ptype.oid as typoid, ptype.typlen " +
                        "FROM `testing_schema2`.INFORMATION_SCHEMA.COLUMNS col " +
                        "LEFT JOIN `accio_temp.pg_type_mapping` mapping ON col.data_type = mapping.bq_type " +
                        "LEFT JOIN `pg_catalog.pg_type` ptype ON mapping.oid = ptype.oid;");
    }

    @Test
    public void testAllTables()
    {
        assertThat(createOrReplaceAllTable(accioMDL))
                .isEqualTo("CREATE OR REPLACE VIEW `accio_temp.all_tables` AS " +
                        "SELECT table_catalog, table_schema, table_name FROM `pg_catalog`.INFORMATION_SCHEMA.TABLES " +
                        "UNION ALL SELECT * FROM UNNEST([STRUCT<table_catalog STRING, table_schema STRING, table_name STRING> " +
                        "('accio_catalog', 'accio_schema', 'OrdersModel'), " +
                        "('accio_catalog', 'accio_schema', 'LineitemModel'), " +
                        "('accio_catalog', 'accio_schema', 'CustomerModel'), " +
                        "('accio_catalog', 'accio_schema', 'Revenue')]);");
    }

    @Test
    public void testPgTypeMapping()
    {
        assertThat(createOrReplacePgTypeMapping())
                .isEqualTo("CREATE OR REPLACE VIEW `accio_temp.pg_type_mapping` AS SELECT * FROM " +
                        "UNNEST([STRUCT<bq_type string, oid int64> ('BOOL', 16),('ARRAY<BOOL>', 1000),('BYTES', 17),('ARRAY<BYTES>', 1001),('FLOAT64', 701)," +
                        "('ARRAY<FLOAT64>', 1022),('INT64', 20),('ARRAY<INT64>', 1016),('STRING', 1043),('ARRAY<STRING>', 1015),('DATE', 1082),('ARRAY<DATE>', 1182)," +
                        "('NUMERIC', 1700),('ARRAY<NUMERIC>', 1231),('TIMESTAMP', 1114),('ARRAY<TIMESTAMP>', 1115)]);");
    }
}
