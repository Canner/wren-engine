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
package io.graphmdl.preaggregation;

import io.graphmdl.base.CatalogSchemaTableName;
import io.graphmdl.base.GraphMDLException;
import io.graphmdl.base.client.duckdb.DuckdbClient;
import io.graphmdl.base.dto.Metric;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.graphmdl.base.metadata.StandardErrorCode.GENERIC_USER_ERROR;
import static java.util.Objects.requireNonNull;

public class MetricTableMapping
{
    private final DuckdbClient duckdbClient;
    private final ConcurrentMap<CatalogSchemaTableName, MetricTablePair> metricTableMapping = new ConcurrentHashMap<>();

    @Inject
    public MetricTableMapping(DuckdbClient duckdbClient)
    {
        this.duckdbClient = requireNonNull(duckdbClient, "duckdbClient is null");
    }

    public void putMetricTableMapping(CatalogSchemaTableName catalogSchemaTableName, MetricTablePair metricTablePair)
    {
        synchronized (metricTableMapping) {
            if (metricTableMapping.containsKey(catalogSchemaTableName)) {
                MetricTablePair existedMetricTablePair = metricTableMapping.get(catalogSchemaTableName);
                if (existedMetricTablePair.getCreateTime() > metricTablePair.getCreateTime()) {
                    metricTablePair.getTableName().ifPresent(duckdbClient::dropTableQuietly);
                    return;
                }
                existedMetricTablePair.getTableName().ifPresent(duckdbClient::dropTableQuietly);
            }
            metricTableMapping.put(catalogSchemaTableName, metricTablePair);
        }
    }

    public MetricTablePair get(CatalogSchemaTableName metricTable)
    {
        return metricTableMapping.get(metricTable);
    }

    public void remove(CatalogSchemaTableName metricTable)
    {
        metricTableMapping.remove(metricTable);
    }

    public MetricTablePair getPreAggregationMetricTablePair(String catalog, String schema, String table)
    {
        return metricTableMapping.get(new CatalogSchemaTableName(catalog, schema, table));
    }

    public Optional<String> convertToAggregationTable(CatalogSchemaTableName catalogSchemaTableName)
    {
        return metricTableMapping.get(catalogSchemaTableName).getTableName();
    }

    public Set<Map.Entry<CatalogSchemaTableName, MetricTablePair>> entrySet()
    {
        return metricTableMapping.entrySet();
    }

    public static class MetricTablePair
    {
        private final Metric metric;
        private final Optional<String> tableName;
        private final Optional<String> errorMessage;
        private final long createTime;

        protected MetricTablePair(Metric metric, String tableName, long createTime)
        {
            this(metric, Optional.of(tableName), Optional.empty(), createTime);
        }

        protected MetricTablePair(Metric metric, Optional<String> tableName, Optional<String> errorMessage, long createTime)
        {
            this.metric = requireNonNull(metric, "metric is null");
            this.tableName = requireNonNull(tableName, "tableName is null");
            this.errorMessage = requireNonNull(errorMessage, "errorMessage is null");
            this.createTime = createTime;
        }

        public Metric getMetric()
        {
            return metric;
        }

        public String getRequiredTableName()
        {
            return tableName.orElseThrow(() -> new GraphMDLException(GENERIC_USER_ERROR, "Mapping table name is refreshing or not exists"));
        }

        public Optional<String> getTableName()
        {
            return tableName;
        }

        public Optional<String> getErrorMessage()
        {
            return errorMessage;
        }

        public long getCreateTime()
        {
            return createTime;
        }
    }
}
