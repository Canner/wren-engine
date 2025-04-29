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

package io.wren.base.dto;

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.wren.base.WrenTypes;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestManifestSerDe
{
    private static final JsonCodec<Map<String, Object>> STRING_TO_STRING_MAP_CODEC = JsonCodec.mapJsonCodec(String.class, Object.class);

    @Test
    public void testSerDeRoundTrip()
    {
        Manifest original = createManifest();
        String json = Manifest.MANIFEST_JSON_CODEC.toJson(original);
        Manifest copy = Manifest.MANIFEST_JSON_CODEC.fromJson(json);
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
                                null,
                                List.of(
                                        new Column("orderkey", "integer", null, false, true, null),
                                        new Column("custkey", "integer", null, false, true, null),
                                        new Column("orderstatus", "string", null, false, true, null),
                                        new Column("totalprice", "double", null, false, true, null),
                                        new Column("orderdate", "date", null, false, true, null),
                                        new Column("orderpriority", "string", null, false, true, null),
                                        new Column("clerk", "string", null, false, true, null),
                                        new Column("shippriority", "integer", null, false, true, null),
                                        new Column("comment", "string", null, false, true, null),
                                        new Column("customer", "CustomerModel", "OrdersCustomer", false, true, null)),
                                "orderkey",
                                false,
                                null),
                        new Model("LineitemModel",
                                "select * from lineitem",
                                null,
                                null,
                                List.of(
                                        new Column("orderkey", "integer", null, false, true, null),
                                        new Column("linenumber", "integer", null, false, true, null),
                                        new Column("extendedprice", "integer", null, false, true, null)),
                                null,
                                false,
                                null),
                        new Model("CustomerModel",
                                null,
                                null,
                                new TableReference("test-catalog", "test-schema", "customer"),
                                List.of(
                                        new Column("custkey", "integer", null, false, true, null),
                                        new Column("name", "string", null, false, true, null),
                                        new Column("address", "string", null, false, true, null),
                                        new Column("nationkey", "integer", null, false, true, null),
                                        new Column("phone", "string", null, false, true, null),
                                        new Column("acctbal", "double", null, false, true, null),
                                        new Column("mktsegment", "string", null, false, true, null),
                                        new Column("comment", "string", null, false, true, null),
                                        new Column("orders", "OrdersModel", "OrdersCustomer", false, true, null),
                                        // calculated field
                                        new Column("orders_totalprice", WrenTypes.VARCHAR, null, true, false, "SUM(orders.totalprice)")),
                                "custkey",
                                false,
                                null)))
                .setRelationships(List.of(
                        new Relationship("OrdersCustomer",
                                List.of("OrdersModel", "CustomerModel"),
                                JoinType.MANY_TO_ONE,
                                "OrdersModel.custkey = CustomerModel.custkey",
                                List.of(new Relationship.SortKey("orderkey", Relationship.SortKey.Ordering.ASC)))))
                .setEnumDefinitions(List.of(
                        new EnumDefinition("OrderStatus", List.of(
                                new EnumValue("PENDING", "pending"),
                                new EnumValue("PROCESSING", "processing"),
                                new EnumValue("SHIPPED", "shipped"),
                                new EnumValue("COMPLETE", "complete")))))
                .setMetrics(List.of(
                        new Metric("Revenue",
                                "OrdersModel",
                                List.of(new Column("orderkey", "string", null, false, true, null)),
                                List.of(new Column("total", "integer", null, false, true, null)),
                                List.of(new TimeGrain("orderdate", "orderdate", List.of(TimeUnit.DAY, TimeUnit.MONTH))),
                                true,
                                null)))
                .setViews(List.of(
                        new View("useMetric",
                                "select * from Revenue",
                                ImmutableMap.of("description", "the view for the revenue metric"))))
                .setCumulativeMetrics(List.of(
                        new CumulativeMetric("DailyRevenue",
                                "Orders",
                                new Measure("totalprice", WrenTypes.INTEGER, "sum", "totalprice"),
                                new Window("orderdate", "orderdate", TimeUnit.DAY, "1994-01-01", "1994-12-31"),
                                false,
                                null),
                        new CumulativeMetric("WeeklyRevenue",
                                "Orders",
                                new Measure("totalprice", WrenTypes.INTEGER, "sum", "totalprice"),
                                new Window("orderdate", "orderdate", TimeUnit.WEEK, "1994-01-01", "1994-12-31"),
                                false,
                                null)))
                .setDateSpine(new DateSpine(TimeUnit.DAY, "1970-01-01", "2077-12-31"))
                .setMacros(List.of(new Macro("test", "(a: Expression) => a + 1")))
                .build();
    }

    @Test
    public void testEmptyHandle()
    {
        assertThatThrownBy(() -> {
            Map<String, Object> json = Map.of("catalog", "", "schema", "test");
            Manifest.MANIFEST_JSON_CODEC.fromJson(STRING_TO_STRING_MAP_CODEC.toJson(json));
        }).cause().hasMessageFindingMatch("catalog is null or empty");

        assertThatThrownBy(() -> {
            Map<String, Object> json = Map.of("catalog", "test", "schema", "");
            Manifest.MANIFEST_JSON_CODEC.fromJson(STRING_TO_STRING_MAP_CODEC.toJson(json));
        }).cause().hasMessageFindingMatch("schema is null or empty");

        assertThatThrownBy(() -> {
            Map<String, Object> json = Map.of("catalog", "test", "schema", "test", "models", List.of(Map.of("name", "")));
            Manifest.MANIFEST_JSON_CODEC.fromJson(STRING_TO_STRING_MAP_CODEC.toJson(json));
        }).cause().hasMessageFindingMatch("name is null or empty");

        assertThatThrownBy(() -> {
            Map<String, Object> json = Map.of("catalog", "test", "schema", "test", "models",
                    List.of(Map.of("name", "test", "columns", List.of(Map.of("name", "")))));
            Manifest.MANIFEST_JSON_CODEC.fromJson(STRING_TO_STRING_MAP_CODEC.toJson(json));
        }).cause().hasMessageFindingMatch("name is null or empty");

        assertThatThrownBy(() -> {
            Map<String, Object> json = Map.of("catalog", "test", "schema", "test", "models",
                    List.of(Map.of("name", "test", "columns", List.of(Map.of("name", "test", "type", "")))));
            Manifest.MANIFEST_JSON_CODEC.fromJson(STRING_TO_STRING_MAP_CODEC.toJson(json));
        }).cause().hasMessageFindingMatch("type is null or empty");

        assertThatThrownBy(() -> {
            Map<String, Object> json = Map.of("catalog", "test", "schema", "test", "relationships",
                    List.of(Map.of("name", "")));
            Manifest.MANIFEST_JSON_CODEC.fromJson(STRING_TO_STRING_MAP_CODEC.toJson(json));
        }).cause().hasMessageFindingMatch("name is null or empty");
    }
}
