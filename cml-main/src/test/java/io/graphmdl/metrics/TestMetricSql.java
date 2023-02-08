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

package io.graphmdl.metrics;

import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static io.graphmdl.metrics.Metric.Filter.Operator.GREATER_THAN;
import static io.graphmdl.metrics.Metric.Filter.Operator.LESS_THAN;
import static io.graphmdl.metrics.Metric.Filter.Operator.LESS_THAN_OR_EQUAL_TO;
import static io.graphmdl.metrics.Metric.TimeGrain.DAY;
import static io.graphmdl.metrics.Metric.TimeGrain.MONTH;
import static io.graphmdl.metrics.Metric.TimeGrain.QUARTER;
import static io.graphmdl.metrics.MetricSql.findCombinations;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMetricSql
{
    @Test
    public void testFindCombinations()
    {
        List<String> groups = List.of();
        assertThat(findCombinations(groups)).isEqualTo(List.of());

        groups = List.of("a");
        assertThat(findCombinations(groups)).isEqualTo(List.of(List.of("a")));

        groups = List.of("a", "b");
        assertThat(findCombinations(groups)).isEqualTo(List.of(
                List.of("a"), List.of("b"),
                List.of("a", "b")));

        groups = List.of("a", "b", "c");
        assertThat(findCombinations(groups)).isEqualTo(List.of(
                List.of("a"), List.of("b"), List.of("c"),
                List.of("a", "b"), List.of("a", "c"), List.of("b", "c"),
                List.of("a", "b", "c")));
    }

    @Test
    public void testExpandDimensions()
    {
        List<MetricSql.Builder> metricSqls = MetricSql.expandDimensions(List.of(testBuilder()), List.of("dim1", "dim2"));
        assertThat(metricSqls.size()).isEqualTo(3);
        assertThat(metricSqls.stream().map(MetricSql.Builder::getDimensions).collect(toList()))
                .isEqualTo(List.of(List.of("dim1"), List.of("dim2"), List.of("dim1", "dim2")));
    }

    @Test
    public void testExpandTimeGrains()
    {
        List<MetricSql.Builder> metricSqls = MetricSql.expandTimeGrains(List.of(testBuilder()), List.of(MONTH, QUARTER));
        assertThat(metricSqls.size()).isEqualTo(2);
        assertThat(metricSqls.stream().map(MetricSql.Builder::getTimeGrain).collect(toList())).isEqualTo(List.of(MONTH, QUARTER));
    }

    @Test
    public void testExpandFilters()
    {
        Metric.Filter c1GT2 = new Metric.Filter("c1", GREATER_THAN, "2");
        Metric.Filter c2LTE100 = new Metric.Filter("c2", LESS_THAN_OR_EQUAL_TO, "100");
        List<MetricSql.Builder> metricSqls = MetricSql.expandFilters(List.of(testBuilder()), List.of(c1GT2, c2LTE100));
        assertThat(metricSqls.size()).isEqualTo(3);
        assertThat(metricSqls.stream().map(MetricSql.Builder::getFilters).collect(toList()))
                .isEqualTo(List.of(
                        List.of(c1GT2),
                        List.of(c2LTE100),
                        List.of(c1GT2, c2LTE100)));
    }

    @Test
    public void testSql()
    {
        MetricSql metricSql = MetricSql.builder()
                .setBaseMetricName("test")
                .setSource("test_table")
                .setDimensions(List.of("dim1", "dim2"))
                .setType(Metric.Type.COUNT)
                .setSql("*")
                .setTimestamp("ts1")
                .setTimeGrain(MONTH)
                .setFilters(List.of(new Metric.Filter("c1", GREATER_THAN, "2"), new Metric.Filter("c1", LESS_THAN, "10")))
                .build();

        assertThat(metricSql.sql()).isEqualTo(
                "SELECT dim1,dim2," +
                        "CAST(TRUNC(EXTRACT(YEAR FROM ts1)) AS INTEGER) AS _col1, " +
                        "CAST(TRUNC(EXTRACT(MONTH FROM ts1)) AS INTEGER) AS _col2, " +
                        "COUNT(*) AS _col3 " +
                        "FROM test_table " +
                        "WHERE c1 > 2 AND c1 < 10 " +
                        "GROUP BY 1,2,3,4");
    }

    @Test
    public void testMetricSql()
    {
        Metric metric = Metric.builder()
                .setName("test")
                .setSource("canner-cml.tpch_tiny.orders")
                .setType(Metric.Type.AVG)
                .setSql("o_totalprice")
                .setDimensions(Set.of("o_orderstatus", "o_shippriority"))
                .setTimestamp("o_orderdate")
                .setTimeGrains(Set.of(DAY, MONTH))
                .setFilters(Set.of(new Metric.Filter("o_orderkey", GREATER_THAN, "1")))
                .build();

        // 3 (dims) * 2 (time grains) * 1 (filter)
        assertThat(MetricSql.of(metric).size()).isEqualTo(6);
    }

    private static MetricSql.Builder testBuilder()
    {
        return MetricSql.builder()
                .setBaseMetricName("test")
                .setSource("source")
                .setType(Metric.Type.COUNT)
                .setSql("*")
                .setTimestamp("timestamp");
    }
}
