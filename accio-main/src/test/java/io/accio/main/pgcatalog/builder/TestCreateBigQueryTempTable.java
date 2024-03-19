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
import org.testng.annotations.Test;

import java.util.List;

import static io.accio.base.AccioMDL.EMPTY;
import static io.accio.base.dto.Column.column;
import static io.accio.base.dto.EnumDefinition.enumDefinition;
import static io.accio.base.dto.EnumValue.enumValue;
import static io.accio.base.dto.Metric.metric;
import static io.accio.base.dto.Model.model;
import static io.accio.base.dto.Relationship.SortKey.sortKey;
import static io.accio.base.dto.Relationship.relationship;
import static io.accio.base.dto.TimeGrain.timeGrain;
import static io.accio.base.dto.TimeUnit.DAY;
import static io.accio.base.dto.TimeUnit.MONTH;
import static io.accio.base.dto.View.view;
import static io.accio.main.pgcatalog.PgCatalogUtils.ACCIO_TEMP_NAME;
import static io.accio.main.pgcatalog.PgCatalogUtils.PG_CATALOG_NAME;
import static io.accio.main.pgcatalog.builder.BigQueryUtils.createOrReplaceAllColumn;
import static io.accio.main.pgcatalog.builder.BigQueryUtils.createOrReplaceAllTable;
import static io.accio.main.pgcatalog.builder.BigQueryUtils.createOrReplacePgTypeMapping;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCreateBigQueryTempTable
{
    private final AccioMDL accioMDL;

    public TestCreateBigQueryTempTable()
    {
        this.accioMDL = AccioMDL.fromManifest(
                Manifest.builder()
                        .setCatalog("accio_catalog")
                        .setSchema("accio_schema")
                        .setModels(List.of(
                                model("OrdersModel",
                                        "select * from orders",
                                        List.of(
                                                column("orderkey", "int4", null, true, "the key of each order"),
                                                column("custkey", "int4", null, true),
                                                column("orderstatus", "varchar", null, true),
                                                column("totalprice", "float8", null, true),
                                                column("orderdate", "date", null, true),
                                                column("orderpriority", "varchar", null, true),
                                                column("clerk", "varchar", null, true),
                                                column("shippriority", "int4", null, true),
                                                column("comment", "varchar", null, true),
                                                column("customer", "CustomerModel", "OrdersCustomer", true)),
                                        "orderkey",
                                        "tpch tiny orders table"),
                                model("LineitemModel",
                                        "select * from lineitem",
                                        List.of(
                                                column("orderkey", "int4", null, true),
                                                column("linenumber", "int4", null, true),
                                                column("extendedprice", "int4", null, true))),
                                model("CustomerModel",
                                        "select * from customer",
                                        List.of(
                                                column("custkey", "int4", null, true),
                                                column("name", "varchar", null, true),
                                                column("address", "varchar", null, true),
                                                column("nationkey", "int4", null, true),
                                                column("phone", "varchar", null, true),
                                                column("acctbal", "float8", null, true),
                                                column("mktsegment", "varchar", null, true),
                                                column("comment", "varchar", null, true),
                                                column("orders", "OrdersModel", "OrdersCustomer", true)),
                                        "custkey")))
                        .setRelationships(List.of(
                                relationship("OrdersCustomer",
                                        List.of("OrdersModel", "CustomerModel"),
                                        JoinType.MANY_TO_ONE,
                                        "OrdersModel.custkey = CustomerModel.custkey",
                                        List.of(sortKey("orderkey", Relationship.SortKey.Ordering.ASC)))))
                        .setEnumDefinitions(List.of(
                                enumDefinition("OrderStatus", List.of(
                                        enumValue("PENDING", "pending"),
                                        enumValue("PROCESSING", "processing"),
                                        enumValue("SHIPPED", "shipped"),
                                        enumValue("COMPLETE", "complete")))))
                        .setMetrics(List.of(metric("Revenue", "OrdersModel",
                                List.of(column("orderkey", "varchar", null, true)),
                                List.of(column("total", "int4", null, true)),
                                List.of(timeGrain("orderdate", "orderdate", List.of(DAY, MONTH))),
                                true)))
                        .setViews(List.of(view("useMetric", "select * from Revenue")))
                        .build());
    }

    @Test
    public void testAllColumns()
    {
        assertThat(createOrReplaceAllColumn(accioMDL, ACCIO_TEMP_NAME, PG_CATALOG_NAME))
                .isEqualTo("CREATE OR REPLACE VIEW `accio_temp.all_columns` AS " +
                        "SELECT 'pg_catalog' as table_schema, col.table_name, col.column_name, col.ordinal_position, ptype.oid as typoid, ptype.typlen " +
                        "FROM `pg_catalog`.INFORMATION_SCHEMA.COLUMNS col " +
                        "LEFT JOIN `accio_temp.pg_type_mapping` mapping ON col.data_type = mapping.bq_type " +
                        "LEFT JOIN `pg_catalog.pg_type` ptype ON mapping.oid = ptype.oid " +
                        "UNION ALL SELECT * FROM UNNEST([STRUCT<table_schema STRING, table_name STRING, column_name STRING, ordinal_position int64, typoid integer, typlen integer> " +
                        "('accio_schema', 'OrdersModel', 'orderkey', 1, 23, 4), " +
                        "('accio_schema', 'OrdersModel', 'custkey', 2, 23, 4), " +
                        "('accio_schema', 'OrdersModel', 'orderstatus', 3, 1043, -1), " +
                        "('accio_schema', 'OrdersModel', 'totalprice', 4, 701, 8), " +
                        "('accio_schema', 'OrdersModel', 'orderdate', 5, 1082, 4), " +
                        "('accio_schema', 'OrdersModel', 'orderpriority', 6, 1043, -1), " +
                        "('accio_schema', 'OrdersModel', 'clerk', 7, 1043, -1), " +
                        "('accio_schema', 'OrdersModel', 'shippriority', 8, 23, 4), " +
                        "('accio_schema', 'OrdersModel', 'comment', 9, 1043, -1), " +
                        "('accio_schema', 'LineitemModel', 'orderkey', 1, 23, 4), " +
                        "('accio_schema', 'LineitemModel', 'linenumber', 2, 23, 4), " +
                        "('accio_schema', 'LineitemModel', 'extendedprice', 3, 23, 4), " +
                        "('accio_schema', 'CustomerModel', 'custkey', 1, 23, 4), " +
                        "('accio_schema', 'CustomerModel', 'name', 2, 1043, -1), " +
                        "('accio_schema', 'CustomerModel', 'address', 3, 1043, -1), " +
                        "('accio_schema', 'CustomerModel', 'nationkey', 4, 23, 4), " +
                        "('accio_schema', 'CustomerModel', 'phone', 5, 1043, -1), " +
                        "('accio_schema', 'CustomerModel', 'acctbal', 6, 701, 8), " +
                        "('accio_schema', 'CustomerModel', 'mktsegment', 7, 1043, -1), " +
                        "('accio_schema', 'CustomerModel', 'comment', 8, 1043, -1), " +
                        "('accio_schema', 'Revenue', 'orderkey', 1, 1043, -1), " +
                        "('accio_schema', 'Revenue', 'total', 2, 23, 4)]);");

        assertThat(createOrReplaceAllColumn(EMPTY, ACCIO_TEMP_NAME, PG_CATALOG_NAME))
                .isEqualTo("CREATE OR REPLACE VIEW `accio_temp.all_columns` AS " +
                        "SELECT 'pg_catalog' as table_schema, col.table_name, col.column_name, col.ordinal_position, ptype.oid as typoid, ptype.typlen " +
                        "FROM `pg_catalog`.INFORMATION_SCHEMA.COLUMNS col " +
                        "LEFT JOIN `accio_temp.pg_type_mapping` mapping ON col.data_type = mapping.bq_type " +
                        "LEFT JOIN `pg_catalog.pg_type` ptype ON mapping.oid = ptype.oid");
    }

    @Test
    public void testAllColumnsWithType()
    {
        AccioMDL typeMDL = AccioMDL.fromManifest(
                Manifest.builder()
                        .setCatalog("accio_catalog")
                        .setSchema("accio_schema")
                        .setModels(List.of(
                                model("TypeModel",
                                        "select * from type_test",
                                        List.of(
                                                column("c_boolean", "boolean", null, true),
                                                column("c_boolean2", "BOOLEAN", null, true),
                                                column("c_bool", "bool", null, true),
                                                column("c_bool_array", "_bool", null, true),
                                                column("c_bool_array2", "_BOOL", null, true),
                                                column("c_bool_array3", "bool[]", null, true),
                                                column("c_bool_array4", "bool array", null, true),
                                                column("c_boolean_array", "boolean[]", null, true),
                                                column("c_boolean_array2", "boolean array", null, true)),
                                        "c_boolean",
                                        "test type table")))
                        .build());

        assertThat(createOrReplaceAllColumn(typeMDL, ACCIO_TEMP_NAME, PG_CATALOG_NAME))
                .isEqualTo("CREATE OR REPLACE VIEW `accio_temp.all_columns` AS " +
                        "SELECT 'pg_catalog' as table_schema, col.table_name, col.column_name, col.ordinal_position, ptype.oid as typoid, ptype.typlen " +
                        "FROM `pg_catalog`.INFORMATION_SCHEMA.COLUMNS col " +
                        "LEFT JOIN `accio_temp.pg_type_mapping` mapping ON col.data_type = mapping.bq_type " +
                        "LEFT JOIN `pg_catalog.pg_type` ptype ON mapping.oid = ptype.oid " +
                        "UNION ALL SELECT * FROM UNNEST([STRUCT<table_schema STRING, table_name STRING, column_name STRING, ordinal_position int64, typoid integer, typlen integer> " +
                        "('accio_schema', 'TypeModel', 'c_boolean', 1, 16, 1), " +
                        "('accio_schema', 'TypeModel', 'c_boolean2', 2, 16, 1), " +
                        "('accio_schema', 'TypeModel', 'c_bool', 3, 16, 1), " +
                        "('accio_schema', 'TypeModel', 'c_bool_array', 4, 1000, -1), " +
                        "('accio_schema', 'TypeModel', 'c_bool_array2', 5, 1000, -1), " +
                        "('accio_schema', 'TypeModel', 'c_bool_array3', 6, 1000, -1), " +
                        "('accio_schema', 'TypeModel', 'c_bool_array4', 7, 1000, -1), " +
                        "('accio_schema', 'TypeModel', 'c_boolean_array', 8, 1000, -1), " +
                        "('accio_schema', 'TypeModel', 'c_boolean_array2', 9, 1000, -1)]);");
    }

    @Test
    public void testAllTables()
    {
        assertThat(createOrReplaceAllTable(accioMDL, ACCIO_TEMP_NAME, PG_CATALOG_NAME))
                .isEqualTo("CREATE OR REPLACE VIEW `accio_temp.all_tables` AS " +
                        "SELECT table_catalog, 'pg_catalog' AS table_schema, table_name FROM `pg_catalog`.INFORMATION_SCHEMA.TABLES " +
                        "UNION ALL SELECT * FROM UNNEST([STRUCT<table_catalog STRING, table_schema STRING, table_name STRING> " +
                        "('accio_catalog', 'accio_schema', 'OrdersModel'), " +
                        "('accio_catalog', 'accio_schema', 'LineitemModel'), " +
                        "('accio_catalog', 'accio_schema', 'CustomerModel'), " +
                        "('accio_catalog', 'accio_schema', 'Revenue')]);");

        assertThat(createOrReplaceAllTable(EMPTY, ACCIO_TEMP_NAME, PG_CATALOG_NAME))
                .isEqualTo("CREATE OR REPLACE VIEW `accio_temp.all_tables` AS " +
                        "SELECT table_catalog, 'pg_catalog' AS table_schema, table_name FROM `pg_catalog`.INFORMATION_SCHEMA.TABLES");
    }

    @Test
    public void testPgTypeMapping()
    {
        assertThat(createOrReplacePgTypeMapping(ACCIO_TEMP_NAME))
                .isEqualTo("CREATE OR REPLACE VIEW `accio_temp.pg_type_mapping` AS SELECT * FROM " +
                        "UNNEST([STRUCT<bq_type string, oid int64> ('BOOL', 16),('ARRAY<BOOL>', 1000),('BYTES', 17),('ARRAY<BYTES>', 1001),('FLOAT64', 701)," +
                        "('ARRAY<FLOAT64>', 1022),('INT64', 20),('ARRAY<INT64>', 1016),('STRING', 1043),('ARRAY<STRING>', 1015),('DATE', 1082),('ARRAY<DATE>', 1182)," +
                        "('NUMERIC', 1700),('ARRAY<NUMERIC>', 1231),('TIMESTAMP', 1114),('ARRAY<TIMESTAMP>', 1115)]);");
    }
}
