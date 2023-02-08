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

package io.graphmdl.testing.metric;

import io.graphmdl.metrics.Metric;
import io.graphmdl.testing.AbstractMetricTestingFramework;
import org.testng.annotations.Test;

import java.util.Objects;
import java.util.Set;

import static io.graphmdl.Utils.randomIntString;
import static io.graphmdl.Utils.randomTableSuffix;
import static io.graphmdl.Utils.swallowException;
import static io.graphmdl.metrics.Metric.Filter.Operator.GREATER_THAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMetricResource
        extends AbstractMetricTestingFramework
{
    @Test
    public void testBasicMetricOperators()
    {
        String filter = randomIntString();
        Metric metric = Metric.builder()
                .setName("metric" + randomTableSuffix())
                // '-' is not allowed in database(catalog) name in pg syntax, hence we quoted catalog name here.
                .setSource("\"canner-cml\".tpch_tiny.orders")
                .setType(Metric.Type.AVG)
                .setSql("o_totalprice")
                .setDimensions(Set.of("o_orderstatus"))
                .setTimestamp("o_orderdate")
                .setTimeGrains(Set.of(Metric.TimeGrain.MONTH))
                .setFilters(Set.of(new Metric.Filter("o_orderkey", GREATER_THAN, filter)))
                .build();

        try {
            // test not exist
            assertThat(getAllMetrics().stream().filter(metricDto -> Objects.equals(metricDto.getName(), metric.getName())).findAny()).isEmpty();

            // test create
            createMetric(metric);

            // test getMetric
            assertThat(getAllMetrics().stream().filter(metricDto -> Objects.equals(metricDto.getName(), metric.getName())).findAny()).isPresent();
            assertThat(getOneMetric(metric.getName()).getMetricSqls().size()).isEqualTo(1);

            // test update
            Metric update = Metric.builder(metric).setTimeGrains(Set.of(Metric.TimeGrain.MONTH, Metric.TimeGrain.DAY)).build();
            updateMetric(update);
            assertThat(getOneMetric(metric.getName()).getMetricSqls().size()).isEqualTo(2);

            // test drop
            dropMetric(metric.getName());
            assertThatThrownBy(() -> getOneMetric(metric.getName()))
                    .hasMessageFindingMatch("");
        }
        finally {
            swallowException(() -> dropMetric(metric.getName()));
        }
    }

    @Test
    public void testMetricNotFound()
    {
        Metric metric = Metric.builder()
                .setName("notfound")
                // '-' is not allowed in database(catalog) name in pg syntax, hence we quoted catalog name here.
                .setSource("\"canner-cml\".tpch_tiny.orders")
                .setType(Metric.Type.AVG)
                .setSql("o_totalprice")
                .setDimensions(Set.of("o_orderstatus"))
                .setTimestamp("o_orderdate")
                .setTimeGrains(Set.of(Metric.TimeGrain.MONTH))
                .setFilters(Set.of(new Metric.Filter("o_orderkey", GREATER_THAN, "1")))
                .build();

        assertThatThrownBy(() -> getOneMetric("notfound")).hasMessageFindingMatch(".*HTTP 404 Not Found.*");
        assertThatThrownBy(() -> dropMetric("notfound")).hasMessageFindingMatch(".*HTTP 404 Not Found.*");
        assertThatThrownBy(() -> updateMetric(metric)).hasMessageFindingMatch(".*HTTP 404 Not Found.*");
    }
}
