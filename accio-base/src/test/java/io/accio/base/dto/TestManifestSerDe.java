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

import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.util.List;

import static io.accio.base.AccioTypes.INTEGER;
import static io.accio.base.AccioTypes.VARCHAR;
import static io.accio.base.dto.Column.caluclatedColumn;
import static io.accio.base.dto.Column.column;
import static io.accio.base.dto.CumulativeMetric.cumulativeMetric;
import static io.accio.base.dto.EnumDefinition.enumDefinition;
import static io.accio.base.dto.EnumValue.enumValue;
import static io.accio.base.dto.Measure.measure;
import static io.accio.base.dto.Metric.metric;
import static io.accio.base.dto.Model.model;
import static io.accio.base.dto.Relationship.SortKey.sortKey;
import static io.accio.base.dto.Relationship.relationship;
import static io.accio.base.dto.TimeGrain.timeGrain;
import static io.accio.base.dto.TimeUnit.DAY;
import static io.accio.base.dto.TimeUnit.MONTH;
import static io.accio.base.dto.View.view;
import static io.accio.base.dto.Window.window;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestManifestSerDe
{
    private static final JsonCodec<Manifest> MANIFEST_JSON_CODEC = JsonCodec.jsonCodec(Manifest.class);

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
                                        column("orders", "OrdersModel", "OrdersCustomer", true),
                                        caluclatedColumn("orders_totalprice", VARCHAR, "SUM(orders.totalprice)")),
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
                .setCumulativeMetrics(List.of(
                        cumulativeMetric("DailyRevenue",
                                "Orders", measure("totalprice", INTEGER, "sum", "totalprice"),
                                window("orderdate", "orderdate", TimeUnit.DAY, "1994-01-01", "1994-12-31")),
                        cumulativeMetric("WeeklyRevenue",
                                "Orders", measure("totalprice", INTEGER, "sum", "totalprice"),
                                window("orderdate", "orderdate", TimeUnit.WEEK, "1994-01-01", "1994-12-31"))))
                .setDateSpine(new DateSpine(TimeUnit.DAY, "1970-01-01", "2077-12-31"))
                .build();
    }
}
