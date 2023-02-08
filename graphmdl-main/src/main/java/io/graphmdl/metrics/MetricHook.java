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

import io.graphmdl.metadata.Metadata;
import io.graphmdl.spi.CmlException;
import io.graphmdl.spi.metadata.SchemaTableName;

import javax.inject.Inject;

import java.util.List;

import static io.graphmdl.Utils.swallowException;
import static io.graphmdl.spi.metadata.StandardErrorCode.ALREADY_EXISTS;
import static io.graphmdl.spi.metadata.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;

// TODO: concurrency accessing issue for query, update and delete
//  https://github.com/Canner/canner-metric-layer/issues/113
public class MetricHook
{
    private final MetricStore metricStore;
    private final Metadata metadata;

    @Inject
    public MetricHook(
            MetricStore metricStore,
            Metadata metadata)
    {
        this.metricStore = requireNonNull(metricStore, "metricStore is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public void handleCreate(Metric metric)
    {
        if (metricStore.getMetric(metric.getName()).isPresent()) {
            throw new CmlException(ALREADY_EXISTS, format("metric %s already exist", metric.getName()));
        }

        createMetric(metric);
    }

    private void createMetric(Metric metric)
    {
        List<MetricSql> createdMetricSqls = MetricSql.of(metric).stream()
                .map(this::createRemoteMaterializedView)
                .collect(toUnmodifiableList());

        metricStore.createMetric(metric);
        createdMetricSqls.forEach(metricStore::createMetricSql);
    }

    private MetricSql createRemoteMaterializedView(MetricSql metricSql)
    {
        SchemaTableName schemaTableName = new SchemaTableName(metadata.getMaterializedViewSchema(), metricSql.getName());
        try {
            metadata.createMaterializedView(schemaTableName, metricSql.sql());
            return MetricSql.builder(metricSql)
                    .setStatus(MetricSql.Status.SUCCESS)
                    .build();
        }
        catch (Exception e) {
            return MetricSql.builder(metricSql)
                    .setStatus(MetricSql.Status.FAILED)
                    .setErrorMessage(e.getMessage())
                    .build();
        }
    }

    public void handleUpdate(Metric metric)
    {
        if (metricStore.getMetric(metric.getName()).isEmpty()) {
            throw new CmlException(NOT_FOUND, format("metric %s is not found", metric.getName()));
        }

        dropMetric(metric.getName());
        createMetric(metric);
    }

    private void dropMetric(String metricName)
    {
        List<MetricSql> metricSqls = metricStore.listMetricSqls(metricName);
        metricSqls.stream()
                .map(metricSql -> new SchemaTableName(metadata.getMaterializedViewSchema(), metricSql.getName()))
                .forEach(schemaTableName -> swallowException(() -> metadata.deleteMaterializedView(schemaTableName)));

        metricStore.dropMetric(metricName);
    }

    public void handleDrop(String metricName)
    {
        if (metricStore.getMetric(metricName).isEmpty()) {
            throw new CmlException(NOT_FOUND, format("metric %s is not found", metricName));
        }

        dropMetric(metricName);
    }
}
