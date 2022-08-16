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

package io.cml.connector.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.MaterializedViewDefinition;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.Routine;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.http.BaseHttpServiceException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.airlift.log.Logger;
import io.cml.metadata.TableHandle;
import io.cml.spi.CmlException;
import io.cml.spi.Parameter;
import io.cml.spi.metadata.SchemaTableName;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.cml.spi.metadata.StandardErrorCode.AMBIGUOUS_NAME;
import static io.cml.spi.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.cml.spi.metadata.StandardErrorCode.GENERIC_USER_ERROR;
import static io.cml.spi.metadata.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class BigQueryClient
{
    private static final Logger LOG = Logger.get(BigQueryClient.class);
    private static final Set<String> INVALID_QUERY = ImmutableSet.of("invalidQuery", "invalid");

    private final BigQuery bigQuery;
    private final Cache<TableId, Table> mvCache;

    public BigQueryClient(BigQuery bigQuery)
    {
        this.bigQuery = bigQuery;
        this.mvCache =
                CacheBuilder.newBuilder()
                        .expireAfterWrite(Duration.ofHours(1).toMillis(), MILLISECONDS)
                        .build();
    }

    public Iterable<Dataset> listDatasets(String projectId)
    {
        return bigQuery.listDatasets(projectId).iterateAll();
    }

    public Dataset getDataSet(Dataset dataset)
    {
        return bigQuery.getDataset(dataset.getDatasetId());
    }

    public Iterable<Table> listTables(DatasetId datasetId, TableDefinition.Type... types)
    {
        Set<TableDefinition.Type> allowedTypes = ImmutableSet.copyOf(types);
        Iterable<Table> allTables = bigQuery.listTables(datasetId).iterateAll();
        return Streams.stream(allTables)
                .filter(table -> allowedTypes.size() == 0 || allowedTypes.contains(table.getDefinition().getType()))
                .collect(toImmutableList());
    }

    public Iterable<Routine> listRoutines(DatasetId datasetId)
    {
        return bigQuery.listRoutines(datasetId, BigQuery.RoutineListOption.pageSize(100)).iterateAll();
    }

    public void createSchema(DatasetInfo datasetInfo)
    {
        bigQuery.create(datasetInfo);
    }

    public Table getTable(TableHandle tableHandle)
    {
        return getTable(TableId.of(
                tableHandle.getCatalogName().getCatalogName(),
                tableHandle.getSchemaTableName().getSchemaName(),
                tableHandle.getSchemaTableName().getTableName()));
    }

    public String getProjectId()
    {
        return bigQuery.getOptions().getProjectId();
    }

    public Dataset getDataset(String name)
    {
        return bigQuery.getDataset(name);
    }

    public Table getTable(TableId tableId)
    {
        Table mv = mvCache.getIfPresent(tableId);
        if (mv != null) {
            return mv;
        }

        Table table = bigQuery.getTable(tableId);
        // put mv def to mv cache
        if (table != null && table.getDefinition() instanceof MaterializedViewDefinition) {
            mvCache.put(table.getTableId(), table);
        }
        return table;
    }

    public void updateTable(TableInfo tableInfo)
    {
        bigQuery.update(tableInfo);
    }

    public Table getCacheMV(TableId tableId)
    {
        return mvCache.getIfPresent(tableId);
    }

    public TableResult query(String sql, List<Parameter> parameters)
    {
        try {
            QueryJobConfiguration.Builder queryConfigBuilder =
                    QueryJobConfiguration
                            .newBuilder(sql);

            for (Parameter parameter : parameters) {
                queryConfigBuilder.addPositionalParameter(QueryParameterValue.of(parameter.getValue(), BigQueryType.toBqType(parameter.getType())));
            }

            return bigQuery.query(queryConfigBuilder.build());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BigQueryException(BaseHttpServiceException.UNKNOWN_CODE, format("Failed to run the query [%s]", sql), e);
        }
    }

    public JobStatistics.QueryStatistics queryDryRun(Optional<String> datasetIdOptional, String query, List<Parameter> parameters)
    {
        try {
            QueryJobConfiguration.Builder queryConfigBuilder =
                    QueryJobConfiguration
                            .newBuilder(query)
                            .setDryRun(true)
                            .setUseQueryCache(false);

            datasetIdOptional.ifPresent(queryConfigBuilder::setDefaultDataset);

            for (Parameter parameter : parameters) {
                queryConfigBuilder.addPositionalParameter(QueryParameterValue.of(parameter.getValue(), BigQueryType.toBqType(parameter.getType())));
            }

            Job job = bigQuery.create(JobInfo.of(queryConfigBuilder.build()));
            return job.getStatistics();
        }
        catch (BigQueryException e) {
            LOG.error(e);
            if (INVALID_QUERY.contains(e.getReason())) {
                if (e.getMessage().contains("ambiguous at")) {
                    throw new CmlException(AMBIGUOUS_NAME, "There are ambiguous column names", e);
                }
                throw new CmlException(GENERIC_USER_ERROR, format("Invalid statement: %s", query), e);
            }
            throw new CmlException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public void dropTable(SchemaTableName schemaTableName)
    {
        if (!bigQuery.delete(TableId.of(schemaTableName.getSchemaName(), schemaTableName.getTableName()))) {
            throw new CmlException(NOT_FOUND, schemaTableName + " was not found");
        }
    }
}
