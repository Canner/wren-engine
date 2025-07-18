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

package io.wren.testing;

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.wren.base.dto.JoinType;
import io.wren.base.dto.Manifest;
import io.wren.base.dto.View;
import io.wren.main.web.dto.DryPlanDtoV2;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.util.Base64;
import java.util.List;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.wren.base.config.WrenConfig.DataSourceType.DUCKDB;
import static io.wren.base.config.WrenConfig.WREN_DATASOURCE_TYPE;
import static io.wren.base.config.WrenConfig.WREN_DIRECTORY;
import static io.wren.base.config.WrenConfig.WREN_ENABLE_DYNAMIC_FIELDS;
import static io.wren.base.dto.Column.calculatedColumn;
import static io.wren.base.dto.Column.column;
import static io.wren.base.dto.Model.model;
import static io.wren.base.dto.Relationship.relationship;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMDLResourceV2
        extends RequireWrenServer
{
    private static final JsonCodec<Manifest> MANIFEST_JSON_CODEC = jsonCodec(Manifest.class);

    @Override
    protected TestingWrenServer createWrenServer()
            throws Exception
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put(WREN_DIRECTORY, Files.createTempDirectory("mdl").toAbsolutePath().toString())
                .put(WREN_DATASOURCE_TYPE, DUCKDB.name())
                .put(WREN_ENABLE_DYNAMIC_FIELDS, "true");
        TestingWrenServer testing = TestingWrenServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
        return testing;
    }

    @Override
    protected void prepare()
    {
        initDuckDB();
    }

    @Test
    public void testDryPlan()
    {
        Manifest manifest = Manifest.builder()
                .setCatalog("wrenai")
                .setSchema("tpch")
                .setModels(List.of(
                        model("Customer", "SELECT * FROM tpch.customer",
                                List.of(column("custkey", "integer", null, false, "c_custkey"),
                                        column("name", "varchar", null, false, "c_name"))),
                        model("Orders", "SELECT * FROM tpch.orders",
                                List.of(column("orderkey", "integer", null, false, "o_orderkey"),
                                        column("custkey", "integer", null, false, "o_custkey"),
                                        column("customer", "Customer", "CustomerOrders", false),
                                        calculatedColumn("customer_name", "varchar", "customer.name")),
                                "orderkey")))
                .setRelationships(List.of(relationship("CustomerOrders", List.of("Customer", "Orders"), JoinType.ONE_TO_MANY, "Customer.custkey = Orders.custkey")))
                .build();

        String manifestStr = base64Encode(toJson(manifest));

        DryPlanDtoV2 dryPlanDto = new DryPlanDtoV2(manifestStr, "select orderkey from Orders limit 200");
        String dryPlan = dryPlanV2(dryPlanDto);
        assertThat(dryPlan).isEqualTo("""
                WITH
                  "Orders" AS (
                   SELECT
                     "Orders"."orderkey" "orderkey"
                   , "Orders"."custkey" "custkey"
                   FROM
                     (
                      SELECT
                        "Orders"."orderkey" "orderkey"
                      , "Orders"."custkey" "custkey"
                      FROM
                        (
                         SELECT
                           o_orderkey "orderkey"
                         , o_custkey "custkey"
                         FROM
                           (
                            SELECT *
                            FROM
                              tpch.orders
                         )  "Orders"
                      )  "Orders"
                   )  "Orders"
                )\s
                SELECT orderkey
                FROM
                  Orders
                LIMIT 200
                """);

        dryPlanDto = new DryPlanDtoV2(manifestStr, "select customer_name from Orders limit 200");
        dryPlan = dryPlanV2(dryPlanDto);
        assertThat(dryPlan).isEqualTo("""
                WITH
                  "Customer" AS (
                   SELECT
                     "Customer"."custkey" "custkey"
                   , "Customer"."name" "name"
                   FROM
                     (
                      SELECT
                        "Customer"."custkey" "custkey"
                      , "Customer"."name" "name"
                      FROM
                        (
                         SELECT
                           c_custkey "custkey"
                         , c_name "name"
                         FROM
                           (
                            SELECT *
                            FROM
                              tpch.customer
                         )  "Customer"
                      )  "Customer"
                   )  "Customer"
                )\s
                , "Orders" AS (
                   SELECT
                     "Orders"."orderkey" "orderkey"
                   , "Orders"."custkey" "custkey"
                   , "Orders_relationsub"."customer_name" "customer_name"
                   FROM
                     (
                      SELECT
                        "Orders"."orderkey" "orderkey"
                      , "Orders"."custkey" "custkey"
                      FROM
                        (
                         SELECT
                           o_orderkey "orderkey"
                         , o_custkey "custkey"
                         FROM
                           (
                            SELECT *
                            FROM
                              tpch.orders
                         )  "Orders"
                      )  "Orders"
                   )  "Orders"
                   LEFT JOIN (
                      SELECT
                        "Orders"."orderkey"
                      , "Customer"."name" "customer_name"
                      FROM
                        (
                         SELECT
                           o_orderkey "orderkey"
                         , o_custkey "custkey"
                         FROM
                           (
                            SELECT *
                            FROM
                              tpch.orders
                         )  "Orders"
                      )  "Orders"
                      LEFT JOIN "Customer" ON ("Customer"."custkey" = "Orders"."custkey")
                   )  "Orders_relationsub" ON ("Orders"."orderkey" = "Orders_relationsub"."orderkey")
                )\s
                SELECT customer_name
                FROM
                  Orders
                LIMIT 200
                """);
    }

    @Test
    public void testSetManyToMany()
    {
        Manifest manifest = Manifest.builder()
                .setCatalog("wrenai")
                .setSchema("tpch")
                .setModels(List.of(
                        model("Customer", "SELECT * FROM tpch.customer",
                                List.of(column("custkey", "integer", null, false, "c_custkey"),
                                        column("name", "varchar", null, false, "c_name"))),
                        model("Orders", "SELECT * FROM tpch.orders",
                                List.of(column("orderkey", "integer", null, false, "o_orderkey"),
                                        column("custkey", "integer", null, false, "o_custkey"),
                                        column("customer", "Customer", "CustomerOrders", false),
                                        calculatedColumn("customer_name", "varchar", "customer.name")),
                                "orderkey")))
                .setRelationships(List.of(relationship("CustomerOrders", List.of("Customer", "Orders"), JoinType.MANY_TO_MANY, "Customer.custkey = Orders.custkey")))
                .build();

        String manifestStr = base64Encode(toJson(manifest));
        DryPlanDtoV2 dryPlanDto = new DryPlanDtoV2(manifestStr, "select orderkey from Orders limit 200");
        String dryPlan = dryPlanV2(dryPlanDto);
        assertThat(dryPlan).isEqualTo("""
                WITH
                  "Orders" AS (
                   SELECT
                     "Orders"."orderkey" "orderkey"
                   , "Orders"."custkey" "custkey"
                   FROM
                     (
                      SELECT
                        "Orders"."orderkey" "orderkey"
                      , "Orders"."custkey" "custkey"
                      FROM
                        (
                         SELECT
                           o_orderkey "orderkey"
                         , o_custkey "custkey"
                         FROM
                           (
                            SELECT *
                            FROM
                              tpch.orders
                         )  "Orders"
                      )  "Orders"
                   )  "Orders"
                )\s
                SELECT orderkey
                FROM
                  Orders
                LIMIT 200
                """);

    }

    @Test
    public void testPlanCountWithClause()
    {
        Manifest manifest = Manifest.builder()
                .setCatalog("wrenai")
                .setSchema("tpch")
                .setModels(List.of(
                        model("Customer", "SELECT * FROM tpch.customer",
                                List.of(column("custkey", "integer", null, false, "c_custkey"),
                                        column("name", "varchar", null, false, "c_name"))),
                        model("Orders", "SELECT * FROM tpch.orders",
                                List.of(column("orderkey", "integer", null, false, "o_orderkey"),
                                        column("custkey", "integer", null, false, "o_custkey"),
                                        column("customer", "Customer", "CustomerOrders", false),
                                        calculatedColumn("customer_name", "varchar", "customer.name")),
                                "orderkey")))
                .setRelationships(List.of(relationship("CustomerOrders", List.of("Customer", "Orders"), JoinType.MANY_TO_MANY, "Customer.custkey = Orders.custkey")))
                .build();
        String manifestStr = base64Encode(toJson(manifest));
        DryPlanDtoV2 dryPlanDto = new DryPlanDtoV2(manifestStr, "select count(*) from (with orders_custkey as (select custkey from \"Orders\") select * from orders_custkey) ");
        String dryPlan = dryPlanV2(dryPlanDto);
        assertThat(dryPlan).isEqualTo("""
                WITH
                  "Orders" AS (
                   SELECT
                     "Orders"."orderkey" "orderkey"
                   , "Orders"."custkey" "custkey"
                   FROM
                     (
                      SELECT
                        "Orders"."orderkey" "orderkey"
                      , "Orders"."custkey" "custkey"
                      FROM
                        (
                         SELECT
                           o_orderkey "orderkey"
                         , o_custkey "custkey"
                         FROM
                           (
                            SELECT *
                            FROM
                              tpch.orders
                         )  "Orders"
                      )  "Orders"
                   )  "Orders"
                )\s
                SELECT count(*)
                FROM
                  (
                   WITH
                     orders_custkey AS (
                      SELECT custkey
                      FROM
                        "Orders"
                   )\s
                   SELECT *
                   FROM
                     orders_custkey
                )\s
                """);
    }

    @Test
    public void testCountView()
    {
        Manifest manifest = Manifest.builder()
                .setCatalog("wrenai")
                .setSchema("tpch")
                .setModels(List.of(
                        model("Customer", "SELECT * FROM tpch.customer",
                                List.of(column("custkey", "integer", null, false, "c_custkey"),
                                        column("name", "varchar", null, false, "c_name")))))
                .setViews(List.of(new View("view1", "select * from Customer", ImmutableMap.of())))
                .build();
        String manifestStr = base64Encode(toJson(manifest));
        DryPlanDtoV2 dryPlanDto = new DryPlanDtoV2(manifestStr, "select count(*) from view1");
        String dryPlan = dryPlanV2(dryPlanDto);
        assertThat(dryPlan).isEqualTo("""
                WITH
                  "Customer" AS (
                   SELECT
                     "Customer"."custkey" "custkey"
                   , "Customer"."name" "name"
                   FROM
                     (
                      SELECT
                        "Customer"."custkey" "custkey"
                      , "Customer"."name" "name"
                      FROM
                        (
                         SELECT
                           c_custkey "custkey"
                         , c_name "name"
                         FROM
                           (
                            SELECT *
                            FROM
                              tpch.customer
                         )  "Customer"
                      )  "Customer"
                   )  "Customer"
                )\s
                , "view1" AS (
                   SELECT *
                   FROM
                     Customer
                )\s
                SELECT count(*)
                FROM
                  view1
                """);
    }

    @Test
    public void testDefaultNullOrdering()
    {
        Manifest manifest = Manifest.builder()
                .setCatalog("wrenai")
                .setSchema("tpch")
                .setModels(List.of(
                        model("customer", "SELECT * FROM tpch.customer",
                                List.of(column("custkey", "integer", null, false, "c_custkey"),
                                        column("name", "varchar", null, false, "c_name")))))
                .build();
        String manifestStr = base64Encode(toJson(manifest));
        DryPlanDtoV2 dryPlanDto = new DryPlanDtoV2(manifestStr, "select * from customer order by custkey");
        String dryPlan = dryPlanV2(dryPlanDto);
        assertThat(dryPlan).isEqualTo("""
                WITH
                  "customer" AS (
                   SELECT
                     "customer"."custkey" "custkey"
                   , "customer"."name" "name"
                   FROM
                     (
                      SELECT
                        "customer"."custkey" "custkey"
                      , "customer"."name" "name"
                      FROM
                        (
                         SELECT
                           c_custkey "custkey"
                         , c_name "name"
                         FROM
                           (
                            SELECT *
                            FROM
                              tpch.customer
                         )  "customer"
                      )  "customer"
                   )  "customer"
                )\s
                SELECT *
                FROM
                  customer
                ORDER BY custkey ASC NULLS LAST
                """);

        dryPlanDto = new DryPlanDtoV2(manifestStr, "select * from customer order by custkey desc");
        dryPlan = dryPlanV2(dryPlanDto);
        assertThat(dryPlan).isEqualTo("""
                WITH
                  "customer" AS (
                   SELECT
                     "customer"."custkey" "custkey"
                   , "customer"."name" "name"
                   FROM
                     (
                      SELECT
                        "customer"."custkey" "custkey"
                      , "customer"."name" "name"
                      FROM
                        (
                         SELECT
                           c_custkey "custkey"
                         , c_name "name"
                         FROM
                           (
                            SELECT *
                            FROM
                              tpch.customer
                         )  "customer"
                      )  "customer"
                   )  "customer"
                )\s
                SELECT *
                FROM
                  customer
                ORDER BY custkey DESC NULLS LAST
                """);
        dryPlanDto = new DryPlanDtoV2(manifestStr, "select * from customer order by custkey asc nulls first");
        dryPlan = dryPlanV2(dryPlanDto);
        assertThat(dryPlan).isEqualTo("""
                WITH
                  "customer" AS (
                   SELECT
                     "customer"."custkey" "custkey"
                   , "customer"."name" "name"
                   FROM
                     (
                      SELECT
                        "customer"."custkey" "custkey"
                      , "customer"."name" "name"
                      FROM
                        (
                         SELECT
                           c_custkey "custkey"
                         , c_name "name"
                         FROM
                           (
                            SELECT *
                            FROM
                              tpch.customer
                         )  "customer"
                      )  "customer"
                   )  "customer"
                )\s
                SELECT *
                FROM
                  customer
                ORDER BY custkey ASC NULLS FIRST
                """);
    }

    private String toJson(Manifest manifest)
    {
        return MANIFEST_JSON_CODEC.toJson(manifest);
    }

    private String base64Encode(String str)
    {
        return Base64.getEncoder().encodeToString(str.getBytes(UTF_8));
    }
}
