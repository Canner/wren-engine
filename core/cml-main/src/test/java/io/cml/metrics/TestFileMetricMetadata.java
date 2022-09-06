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

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.cml.metrics.Metric.Filter.Operator.GREATER_THAN;
import static io.cml.metrics.Metric.TimeGrain.DAY;
import static io.cml.metrics.Metric.TimeGrain.MONTH;
import static java.lang.System.getenv;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestFileMetricMetadata
{
    private final MetricMetadata metricMetadata;

    public TestFileMetricMetadata()
    {
        this.metricMetadata = new FileMetricMetadata(Path.of(requireNonNull(getenv("TEST_CML_FILE_METRIC_METADATA_HOME"))));
    }

    @Test
    public void testCreateMetric()
    {
        Metric expected = Metric.builder()
                .setName("test")
                .setSource("canner-cml.tpch_tiny.orders")
                .setType(Metric.Type.AVG)
                .setSql("o_totalprice")
                .setDimensions(Set.of("o_orderstatus", "o_shippriority"))
                .setTimestamp("o_orderdate")
                .setTimeGrains(Set.of(DAY, MONTH))
                .setFilters(Set.of(new Metric.Filter("o_orderkey", GREATER_THAN, "1")))
                .build();

        List<MetricSql> metricSqls = MetricSql.of(expected);

        try {
            metricMetadata.createMetric(expected);
            Optional<Metric> actual = metricMetadata.getMetric(expected.getName());
            assertThat(actual.isPresent()).isTrue();
            assertThat(actual.get()).isEqualTo(expected);
            assertThat(metricMetadata.listMetricSqls(expected.getName()).isEmpty()).isTrue();

            metricSqls.forEach(metricMetadata::createMetricSql);
            assertThat(metricMetadata.listMetricSqls(expected.getName()).size()).isEqualTo(metricSqls.size());

            MetricSql firstSqlExpected = metricSqls.get(0);
            Optional<MetricSql> firstSqlActualOptional = metricMetadata.getMetricSql(firstSqlExpected.getBaseMetricName(), firstSqlExpected.getName());
            assertThat(firstSqlActualOptional.isPresent()).isTrue();
            assertThat(firstSqlActualOptional.get()).isEqualTo(firstSqlExpected);
        }
        finally {
            metricMetadata.dropMetric(expected.getName());
        }
    }

    @Test
    public void testNotExistMetric()
    {
        MetricMetadata fakeMetadata = new FileMetricMetadata(Path.of("/tmp/notfound"));
        assertThatThrownBy(fakeMetadata::listMetrics)
                .hasMessageFindingMatch(".*rootPath is not found.*");
        assertThat(metricMetadata.listMetrics().isEmpty()).isTrue();
        assertThat(metricMetadata.getMetric("notfound").isEmpty()).isTrue();
        assertThatThrownBy(() -> metricMetadata.listMetricSqls("notfound"))
                .hasMessageFindingMatch("metric .* not found");
    }

    @Test
    public void testDropMetric()
    {
        Metric expected = Metric.builder()
                .setName("test")
                .setSource("canner-cml.tpch_tiny.orders")
                .setType(Metric.Type.AVG)
                .setSql("o_totalprice")
                .setDimensions(Set.of("o_orderstatus", "o_shippriority"))
                .setTimestamp("o_orderdate")
                .setTimeGrains(Set.of(DAY, MONTH))
                .setFilters(Set.of(new Metric.Filter("o_orderkey", GREATER_THAN, "1")))
                .build();

        List<MetricSql> metricSqls = MetricSql.of(expected);

        try {
            metricMetadata.createMetric(expected);
            assertThat(metricMetadata.getMetric(expected.getName()).isPresent()).isTrue();
            metricSqls.forEach(metricMetadata::createMetricSql);
            assertThat(metricMetadata.listMetricSqls(expected.getName()).size()).isEqualTo(metricSqls.size());

            metricMetadata.dropMetric(expected.getName());
            assertThat(metricMetadata.getMetric(expected.getName()).isPresent()).isFalse();
            assertThatThrownBy(() -> metricMetadata.dropMetric(expected.getName()))
                    .hasMessageFindingMatch("metric .* not found");
        }
        finally {
            dropMetricQuietly(expected.getName());
        }
    }

    private void dropMetricQuietly(String name)
    {
        try {
            metricMetadata.dropMetric(name);
        }
        catch (Exception ignore) {
        }
    }
}
