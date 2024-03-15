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

package io.accio.base.dto;

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static io.accio.base.AccioTypes.INTEGER;
import static io.accio.base.AccioTypes.VARCHAR;
import static io.accio.base.dto.Manifest.MANIFEST_JSON_CODEC;
import static io.accio.base.dto.Relationship.SortKey;
import static io.accio.base.dto.TimeUnit.DAY;
import static io.accio.base.dto.TimeUnit.MONTH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestManifestSerDe
{
    private static final JsonCodec<Map<String, Object>> STRING_TO_STRING_MAP_CODEC = JsonCodec.mapJsonCodec(String.class, Object.class);

    @Test
    public void testSerDeRoundTrip()
    {
        Manifest original = createManifest();
        String json = MANIFEST_JSON_CODEC.toJson(original);
        Manifest copy = MANIFEST_JSON_CODEC.fromJson(json);
        assertThat(original).isEqualTo(copy);
    }

    private static Manifest createManifest()
    {
        return Manifest.builder()
                .setCatalog("test-catalog")
                .setSchema("test-schema")
                .setModels(List.of(
                        new Model("OrdersModel",
                                "select * from orders",
                                null,
                                List.of(
                                        new Column("orderkey", "integer", null, false, true, null,
                                                "the key of each order", ImmutableMap.of("description", "the key of each order")),
                                        new Column("custkey", "integer", null, false, true, null, null, null),
                                        new Column("orderstatus", "string", null, false, true, null, null, null),
                                        new Column("totalprice", "double", null, false, true, null, null, null),
                                        new Column("orderdate", "date", null, false, true, null, null, null),
                                        new Column("orderpriority", "string", null, false, true, null, null, null),
                                        new Column("clerk", "string", null, false, true, null, null, null),
                                        new Column("shippriority", "integer", null, false, true, null, null, null),
                                        new Column("comment", "string", null, false, true, null, null, null),
                                        new Column("customer", "CustomerModel", "OrdersCustomer", false, true, null, null, null)),
                                "orderkey",
                                false,
                                null,
                                "tpch tiny orders table",
                                ImmutableMap.of("description", "tpch tiny orders table")),
                        new Model("LineitemModel",
                                "select * from lineitem",
                                null,
                                List.of(
                                        new Column("orderkey", "integer", null, false, true, null, null, null),
                                        new Column("linenumber", "integer", null, false, true, null, null, null),
                                        new Column("extendedprice", "integer", null, false, true, null, null, null)),
                                null,
                                false,
                                null,
                                null,
                                null),
                        new Model("CustomerModel",
                                "select * from customer",
                                null,
                                List.of(
                                        new Column("custkey", "integer", null, false, true, null, null, null),
                                        new Column("name", "string", null, false, true, null, null, null),
                                        new Column("address", "string", null, false, true, null, null, null),
                                        new Column("nationkey", "integer", null, false, true, null, null, null),
                                        new Column("phone", "string", null, false, true, null, null, null),
                                        new Column("acctbal", "double", null, false, true, null, null, null),
                                        new Column("mktsegment", "string", null, false, true, null, null, null),
                                        new Column("comment", "string", null, false, true, null, null, null),
                                        new Column("orders", "OrdersModel", "OrdersCustomer", false, true, null, null, null),
                                        // calculated field
                                        new Column("orders_totalprice", VARCHAR, null, true, false, "SUM(orders.totalprice)", null, null)),
                                "custkey",
                                false,
                                null,
                                null,
                                null)))
                .setRelationships(List.of(
                        new Relationship("OrdersCustomer",
                                List.of("OrdersModel", "CustomerModel"),
                                JoinType.MANY_TO_ONE,
                                "OrdersModel.custkey = CustomerModel.custkey",
                                List.of(new SortKey("orderkey", Relationship.SortKey.Ordering.ASC)),
                                "the relationship between orders and customers",
                                ImmutableMap.of("description", "the relationship between orders and customers"))))
                .setEnumDefinitions(List.of(
                        new EnumDefinition("OrderStatus", List.of(
                                new EnumValue("PENDING", "pending", ImmutableMap.of("description", "pending")),
                                new EnumValue("PROCESSING", "processing", null),
                                new EnumValue("SHIPPED", "shipped", null),
                                new EnumValue("COMPLETE", "complete", null)),
                                "the status of an order",
                                ImmutableMap.of("description", "the status of an order"))))
                .setMetrics(List.of(
                        new Metric("Revenue",
                                "OrdersModel",
                                List.of(new Column("orderkey", "string", null, false, true, null, null, null)),
                                List.of(new Column("total", "integer", null, false, true, null, null, null)),
                                List.of(new TimeGrain("orderdate", "orderdate", List.of(DAY, MONTH))),
                                true,
                                null,
                                "the revenue of an order",
                                ImmutableMap.of("description", "the revenue of an order"))))
                .setViews(List.of(
                        new View("useMetric",
                                "select * from Revenue",
                                "the view for the revenue metric",
                                ImmutableMap.of("description", "the view for the revenue metric"))))
                .setCumulativeMetrics(List.of(
                        new CumulativeMetric("DailyRevenue",
                                "Orders",
                                new Measure("totalprice", INTEGER, "sum", "totalprice", ImmutableMap.of("description", "totalprice")),
                                new Window("orderdate", "orderdate", TimeUnit.DAY, "1994-01-01", "1994-12-31", ImmutableMap.of("description", "orderdate")),
                                false,
                                null,
                                "daily revenue",
                                ImmutableMap.of("description", "daily revenue")),
                        new CumulativeMetric("WeeklyRevenue",
                                "Orders",
                                new Measure("totalprice", INTEGER, "sum", "totalprice", null),
                                new Window("orderdate", "orderdate", TimeUnit.WEEK, "1994-01-01", "1994-12-31", null),
                                false,
                                null,
                                null,
                                null)))
                .setDateSpine(new DateSpine(TimeUnit.DAY, "1970-01-01", "2077-12-31", ImmutableMap.of("description", "a date spine")))
                .setMacros(List.of(new Macro("test", "(a: Expression) => a + 1", ImmutableMap.of("description", "a macro"))))
                .build();
    }

    @Test
    public void testEmptyHandle()
    {
        assertThatThrownBy(() -> {
            Map<String, Object> json = Map.of("catalog", "", "schema", "test");
            MANIFEST_JSON_CODEC.fromJson(STRING_TO_STRING_MAP_CODEC.toJson(json));
        }).getCause().hasMessageFindingMatch("catalog is null or empty");

        assertThatThrownBy(() -> {
            Map<String, Object> json = Map.of("catalog", "test", "schema", "");
            MANIFEST_JSON_CODEC.fromJson(STRING_TO_STRING_MAP_CODEC.toJson(json));
        }).getCause().hasMessageFindingMatch("schema is null or empty");

        assertThatThrownBy(() -> {
            Map<String, Object> json = Map.of("catalog", "test", "schema", "test", "models", List.of(Map.of("name", "")));
            MANIFEST_JSON_CODEC.fromJson(STRING_TO_STRING_MAP_CODEC.toJson(json));
        }).getCause().hasMessageFindingMatch("name is null or empty");

        assertThatThrownBy(() -> {
            Map<String, Object> json = Map.of("catalog", "test", "schema", "test", "models",
                    List.of(Map.of("name", "test", "columns", List.of(Map.of("name", "")))));
            MANIFEST_JSON_CODEC.fromJson(STRING_TO_STRING_MAP_CODEC.toJson(json));
        }).getCause().hasMessageFindingMatch("name is null or empty");

        assertThatThrownBy(() -> {
            Map<String, Object> json = Map.of("catalog", "test", "schema", "test", "models",
                    List.of(Map.of("name", "test", "columns", List.of(Map.of("name", "test", "type", "")))));
            MANIFEST_JSON_CODEC.fromJson(STRING_TO_STRING_MAP_CODEC.toJson(json));
        }).getCause().hasMessageFindingMatch("type is null or empty");

        assertThatThrownBy(() -> {
            Map<String, Object> json = Map.of("catalog", "test", "schema", "test", "relationships",
                    List.of(Map.of("name", "")));
            MANIFEST_JSON_CODEC.fromJson(STRING_TO_STRING_MAP_CODEC.toJson(json));
        }).getCause().hasMessageFindingMatch("name is null or empty");
    }
}
