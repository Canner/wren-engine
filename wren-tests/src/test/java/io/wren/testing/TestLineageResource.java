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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.wren.base.dto.CumulativeMetric;
import io.wren.base.dto.Manifest;
import io.wren.base.dto.Metric;
import io.wren.base.dto.Model;
import io.wren.base.dto.Relationship;
import io.wren.base.dto.TimeUnit;
import io.wren.main.web.dto.ColumnLineageInputDto;
import io.wren.main.web.dto.LineageResult;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.wren.base.WrenTypes.BIGINT;
import static io.wren.base.WrenTypes.DATE;
import static io.wren.base.WrenTypes.INTEGER;
import static io.wren.base.WrenTypes.VARCHAR;
import static io.wren.base.dto.Column.caluclatedColumn;
import static io.wren.base.dto.Column.column;
import static io.wren.base.dto.CumulativeMetric.cumulativeMetric;
import static io.wren.base.dto.JoinType.MANY_TO_ONE;
import static io.wren.base.dto.JoinType.ONE_TO_MANY;
import static io.wren.base.dto.Manifest.MANIFEST_JSON_CODEC;
import static io.wren.base.dto.Measure.measure;
import static io.wren.base.dto.Metric.metric;
import static io.wren.base.dto.Model.model;
import static io.wren.base.dto.Relationship.relationship;
import static io.wren.base.dto.Window.window;
import static io.wren.main.web.dto.LineageResult.columnWithType;
import static io.wren.main.web.dto.LineageResult.lineageResult;
import static io.wren.testing.AbstractTestFramework.addColumnsToModel;
import static io.wren.testing.AbstractTestFramework.withDefaultCatalogSchema;
import static io.wren.testing.WebApplicationExceptionAssert.assertWebApplicationException;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLineageResource
        extends RequireWrenServer
{
    private Model customer;
    private Model orders;
    private Model lineitem;
    private Relationship ordersCustomer;
    private Relationship ordersLineitem;

    private Path mdlDir;

    @Override
    protected TestingWrenServer createWrenServer()
    {
        initData();
        Model newCustomer = addColumnsToModel(
                customer,
                column("orders", "Orders", "OrdersCustomer", true),
                caluclatedColumn("lineitem_price", BIGINT, "sum(orders.lineitem.discount * orders.lineitem.extendedprice)"));
        Model newOrders = addColumnsToModel(
                orders,
                column("customer", "Customer", "OrdersCustomer", true),
                column("lineitem", "Lineitem", "OrdersLineitem", true));
        Model newLineitem = addColumnsToModel(
                lineitem,
                column("orders", "Orders", "OrdersLineitem", true));
        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(newCustomer, newOrders, newLineitem))
                .setRelationships(List.of(ordersCustomer, ordersLineitem))
                .build();
        try {
            mdlDir = Files.createTempDirectory("wrenmdls");
            Path wrenMDLFilePath = mdlDir.resolve("wrenmdl.json");
            Files.write(wrenMDLFilePath, MANIFEST_JSON_CODEC.toJsonBytes(manifest));
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("wren.directory", mdlDir.toAbsolutePath().toString())
                .put("wren.datasource.type", "duckdb");

        return TestingWrenServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
    }

    private void initData()
    {
        customer = model("Customer",
                "select * from main.customer",
                List.of(
                        column("custkey", INTEGER, null, true),
                        column("name", VARCHAR, null, true),
                        column("address", VARCHAR, null, true),
                        column("nationkey", INTEGER, null, true),
                        column("phone", VARCHAR, null, true),
                        column("acctbal", INTEGER, null, true),
                        column("mktsegment", VARCHAR, null, true),
                        column("comment", VARCHAR, null, true)),
                "custkey");
        orders = model("Orders",
                "select * from main.orders",
                List.of(
                        column("orderkey", INTEGER, null, true),
                        column("custkey", INTEGER, null, true),
                        column("orderstatus", VARCHAR, null, true),
                        column("totalprice", INTEGER, null, true),
                        column("orderdate", DATE, null, true),
                        column("orderpriority", VARCHAR, null, true),
                        column("clerk", VARCHAR, null, true),
                        column("shippriority", INTEGER, null, true),
                        column("comment", VARCHAR, null, true),
                        column("lineitem", "Lineitem", "OrdersLineitem", true)),
                "orderkey");
        lineitem = model("Lineitem",
                "select * from main.lineitem",
                List.of(
                        column("orderkey", INTEGER, null, true),
                        column("partkey", INTEGER, null, true),
                        column("suppkey", INTEGER, null, true),
                        column("linenumber", INTEGER, null, true),
                        column("quantity", INTEGER, null, true),
                        column("extendedprice", INTEGER, null, true),
                        column("discount", INTEGER, null, true),
                        column("tax", INTEGER, null, true),
                        column("returnflag", VARCHAR, null, true),
                        column("linestatus", VARCHAR, null, true),
                        column("shipdate", DATE, null, true),
                        column("commitdate", DATE, null, true),
                        column("receiptdate", DATE, null, true),
                        column("shipinstruct", VARCHAR, null, true),
                        column("shipmode", VARCHAR, null, true),
                        column("comment", VARCHAR, null, true),
                        column("orderkey_linenumber", VARCHAR, null, true, "concat(orderkey, '-', linenumber)")),
                "orderkey_linenumber");
        ordersCustomer = relationship("OrdersCustomer", List.of("Orders", "Customer"), MANY_TO_ONE, "Orders.custkey = Customer.custkey");
        ordersLineitem = relationship("OrdersLineitem", List.of("Orders", "Lineitem"), ONE_TO_MANY, "Orders.orderkey = Lineitem.orderkey");
    }

    @Test
    public void testColumnLineageDefaultManifest()
    {
        List<LineageResult> results = getColumnLineage(new ColumnLineageInputDto(null, "Customer", "lineitem_price"));
        assertThat(results.size()).isEqualTo(3);

        List<LineageResult> expected = ImmutableList.<LineageResult>builder()
                .add(lineageResult("Customer", List.of(columnWithType("orders", "Orders"))))
                .add(lineageResult("Orders", List.of(columnWithType("lineitem", "Lineitem"))))
                .add(lineageResult("Lineitem", List.of(columnWithType("extendedprice", INTEGER), columnWithType("discount", INTEGER))))
                .build();

        assertIgnoreOrder(results, expected);
    }

    @Test
    public void testColumnLineageCustomManifest()
    {
        Model newCustomer = addColumnsToModel(
                customer,
                column("orders", "Orders", "OrdersCustomer", true),
                caluclatedColumn("sum_lineitem_price", BIGINT, "sum(orders.lineitem.extendedprice)"));
        Model newOrders = addColumnsToModel(
                orders,
                column("customer", "Customer", "OrdersCustomer", true),
                column("lineitem", "Lineitem", "OrdersLineitem", true));
        Model newLineitem = addColumnsToModel(
                lineitem,
                column("orders", "Orders", "OrdersLineitem", true));
        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(newCustomer, newOrders, newLineitem))
                .setRelationships(List.of(ordersCustomer, ordersLineitem))
                .build();

        List<LineageResult> results = getColumnLineage(new ColumnLineageInputDto(manifest, "Customer", "sum_lineitem_price"));
        assertThat(results.size()).isEqualTo(3);

        List<LineageResult> expected = ImmutableList.<LineageResult>builder()
                .add(lineageResult("Customer", List.of(columnWithType("orders", "Orders"))))
                .add(lineageResult("Orders", List.of(columnWithType("lineitem", "Lineitem"))))
                .add(lineageResult("Lineitem", List.of(columnWithType("extendedprice", INTEGER))))
                .build();

        assertIgnoreOrder(results, expected);
    }

    @Test
    public void testMetricOnModel()
    {
        Model newCustomer = addColumnsToModel(
                customer,
                column("orders", "Orders", "OrdersCustomer", true));
        Metric customerSpending = metric("CustomerSpending", "Customer",
                List.of(column("name", VARCHAR, null, true)),
                List.of(column("spending", BIGINT, null, true, "sum(orders.totalprice)"),
                        column("count", BIGINT, null, true, "count(*)")));
        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(orders, newCustomer))
                .setMetrics(List.of(customerSpending))
                .setRelationships(List.of(ordersCustomer))
                .build();

        List<LineageResult> results = getColumnLineage(new ColumnLineageInputDto(manifest, "CustomerSpending", "spending"));
        assertThat(results.size()).isEqualTo(2);

        List<LineageResult> expected = ImmutableList.<LineageResult>builder()
                .add(lineageResult("Customer", List.of(columnWithType("orders", "Orders"))))
                .add(lineageResult("Orders", List.of(columnWithType("totalprice", INTEGER))))
                .build();

        assertIgnoreOrder(results, expected);
    }

    @Test
    public void testCumulativeMetricOnModel()
    {
        CumulativeMetric dailyRevenue = cumulativeMetric("DailyRevenue", "Orders",
                measure("c_totalprice", INTEGER, "sum", "totalprice"),
                window("c_orderdate", "orderdate", TimeUnit.DAY, "1994-01-01", "1994-12-31"));

        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(orders))
                .setCumulativeMetrics(List.of(dailyRevenue))
                .build();

        List<LineageResult> results = getColumnLineage(new ColumnLineageInputDto(manifest, "DailyRevenue", "c_totalprice"));
        List<LineageResult> expected = ImmutableList.<LineageResult>builder()
                .add(lineageResult("Orders", List.of(columnWithType("totalprice", INTEGER))))
                .build();
        assertIgnoreOrder(results, expected);

        results = getColumnLineage(new ColumnLineageInputDto(manifest, "DailyRevenue", "c_orderdate"));
        expected = ImmutableList.<LineageResult>builder()
                .add(lineageResult("Orders", List.of(columnWithType("orderdate", DATE))))
                .build();
        assertIgnoreOrder(results, expected);
    }

    @Test
    public void testErrorHandling()
    {
        assertWebApplicationException(() -> getColumnLineage(new ColumnLineageInputDto(null, null, "lineitem_price")))
                .hasErrorMessageMatches(".*modelName must be specified.*");
        assertWebApplicationException(() -> getColumnLineage(new ColumnLineageInputDto(null, "Customer", null)))
                .hasErrorMessageMatches(".*columnName must be specified.*");
    }

    @Test
    public void testNonExist()
    {
        Manifest manifest = withDefaultCatalogSchema().build();
        List<LineageResult> results = getColumnLineage(new ColumnLineageInputDto(manifest, "foo", "bar"));
        assertThat(results.size()).isEqualTo(0);
    }

    private void assertIgnoreOrder(List<LineageResult> results, List<LineageResult> expecteds)
    {
        Map<String, Set<?>> resultMap = results.stream().collect(toMap(LineageResult::getDatasetName, m -> new HashSet<>(m.getColumns())));
        expecteds.forEach(expected -> {
            assertThat(resultMap.containsKey(expected.getDatasetName())).isTrue();
            assertThat(resultMap.get(expected.getDatasetName())).isEqualTo(new HashSet<>(expected.getColumns()));
        });
    }
}
