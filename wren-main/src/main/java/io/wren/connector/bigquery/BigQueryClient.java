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

package io.wren.connector.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.Routine;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.http.BaseHttpServiceException;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.airlift.log.Logger;
import io.wren.base.CatalogSchemaTableName;
import io.wren.base.Parameter;
import io.wren.base.WrenException;
import io.wren.base.metadata.SchemaTableName;
import io.wren.base.type.PGArray;
import io.wren.base.type.PGType;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.cloud.bigquery.BigQuery.DatasetDeleteOption.deleteContents;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.wren.base.metadata.StandardErrorCode.AMBIGUOUS_NAME;
import static io.wren.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.wren.base.metadata.StandardErrorCode.GENERIC_USER_ERROR;
import static io.wren.base.metadata.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;

public class BigQueryClient
{
    private static final Logger LOG = Logger.get(BigQueryClient.class);
    private static final Set<String> INVALID_QUERY = ImmutableSet.of("invalidQuery", "invalid");

    private final BigQuery bigQuery;

    public BigQueryClient(BigQuery bigQuery)
    {
        this.bigQuery = bigQuery;
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

    public void deleteSchema(DatasetId datasetId)
    {
        bigQuery.delete(datasetId, deleteContents());
    }

    public Table getTable(CatalogSchemaTableName catalogSchemaTableName)
    {
        return getTable(TableId.of(
                catalogSchemaTableName.getCatalogName(),
                catalogSchemaTableName.getSchemaTableName().getSchemaName(),
                catalogSchemaTableName.getSchemaTableName().getTableName()));
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
        return bigQuery.getTable(tableId);
    }

    public void updateTable(TableInfo tableInfo)
    {
        bigQuery.update(tableInfo);
    }

    public TableResult query(String sql, List<Parameter> parameters)
    {
        try {
            QueryJobConfiguration.Builder queryConfigBuilder =
                    QueryJobConfiguration
                            .newBuilder(sql);

            for (Parameter parameter : parameters) {
                queryConfigBuilder.addPositionalParameter(toQueryParameterValue(parameter.getType(), parameter.getValue()));
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
                queryConfigBuilder.addPositionalParameter(toQueryParameterValue(parameter.getType(), parameter.getValue()));
            }

            Job job = bigQuery.create(JobInfo.of(queryConfigBuilder.build()));
            return job.getStatistics();
        }
        catch (BigQueryException e) {
            LOG.error(e);
            if (INVALID_QUERY.contains(e.getReason())) {
                if (e.getMessage().contains("ambiguous at")) {
                    throw new WrenException(AMBIGUOUS_NAME, "There are ambiguous column names", e);
                }
                throw new WrenException(GENERIC_USER_ERROR, format("Invalid statement: %s", query), e);
            }
            throw new WrenException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public void dropTable(SchemaTableName schemaTableName)
    {
        if (!bigQuery.delete(TableId.of(schemaTableName.getSchemaName(), schemaTableName.getTableName()))) {
            throw new WrenException(NOT_FOUND, schemaTableName + " was not found");
        }
    }

    public void dropDatasetWithAllContent(DatasetId datasetId)
    {
        if (!bigQuery.delete(datasetId, deleteContents())) {
            throw new WrenException(NOT_FOUND, datasetId + " was not found");
        }
    }

    private QueryParameterValue toQueryParameterValue(PGType<?> type, Object value)
    {
        if (type instanceof PGArray) {
            PGType<?> innerType = ((PGArray) type).getInnerType();
            return QueryParameterValue.array(
                    (Object[]) BigQueryType.toBqValue(type, value),
                    BigQueryType.toBqType(innerType));
        }
        return QueryParameterValue.of(
                BigQueryType.toBqValue(type, value),
                BigQueryType.toBqType(type));
    }
}
