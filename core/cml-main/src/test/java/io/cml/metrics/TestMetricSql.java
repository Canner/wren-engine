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

package io.cml.metrics;

import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static io.cml.metrics.Metric.Filter.Operator.GREATER_THAN;
import static io.cml.metrics.Metric.Filter.Operator.LESS_THAN;
import static io.cml.metrics.Metric.Filter.Operator.LESS_THAN_OR_EQUAL_TO;
import static io.cml.metrics.Metric.TimeGrain.DAY;
import static io.cml.metrics.Metric.TimeGrain.MONTH;
import static io.cml.metrics.MetricSql.findCombinations;
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
        assertThat(MetricSql.expandDimensions(List.of(MetricSql.builder().build()), List.of("dim1", "dim2")))
                .hasSameElementsAs(List.of(
                        MetricSql.builder().setDimensions(List.of("dim1")).build(),
                        MetricSql.builder().setDimensions(List.of("dim2")).build(),
                        MetricSql.builder().setDimensions(List.of("dim1", "dim2")).build()));
    }

    @Test
    public void testExpandTimeGrains()
    {
        assertThat(MetricSql.expandTimeGrains(List.of(MetricSql.builder().build()), List.of(MONTH, Metric.TimeGrain.QUARTER)))
                .hasSameElementsAs(List.of(
                        MetricSql.builder().setTimeGrains(MONTH).build(),
                        MetricSql.builder().setTimeGrains(Metric.TimeGrain.QUARTER).build()));
    }

    @Test
    public void testExpandFilters()
    {
        Metric.Filter c1GT2 = new Metric.Filter("c1", GREATER_THAN, "2");
        Metric.Filter c2LTE100 = new Metric.Filter("c2", LESS_THAN_OR_EQUAL_TO, "100");
        assertThat(MetricSql.expandFilters(List.of(MetricSql.builder().build()), List.of(c1GT2, c2LTE100)))
                .hasSameElementsAs(List.of(
                        MetricSql.builder().setFilters(List.of(c1GT2)).build(),
                        MetricSql.builder().setFilters(List.of(c2LTE100)).build(),
                        MetricSql.builder().setFilters(List.of(c1GT2, c2LTE100)).build()));
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
                .setTimeGrains(MONTH)
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
}
