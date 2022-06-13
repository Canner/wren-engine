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
import com.google.cloud.bigquery.MaterializedViewDefinition;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Routine;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.http.BaseHttpServiceException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.cml.metadata.TableHandle;

import java.time.Duration;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class BigQueryClient
{
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

    public Table getCacheMV(TableId tableId)
    {
        return mvCache.getIfPresent(tableId);
    }

    public TableResult query(String sql)
    {
        try {
            return bigQuery.query(QueryJobConfiguration.of(sql));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BigQueryException(BaseHttpServiceException.UNKNOWN_CODE, format("Failed to run the query [%s]", sql), e);
        }
    }
}
