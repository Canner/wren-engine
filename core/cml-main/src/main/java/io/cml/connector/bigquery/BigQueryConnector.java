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

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Routine;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.Streams;
import io.airlift.log.Logger;
import io.cml.spi.CmlException;
import io.cml.spi.connector.Connector;
import io.cml.spi.metadata.MetadataUtil;
import io.cml.spi.metadata.SchemaTableName;
import io.cml.spi.metadata.TableMetadata;
import io.cml.type.VarcharType;

import javax.inject.Inject;

import java.util.AbstractCollection;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.cml.spi.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.cml.spi.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.cml.spi.metadata.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class BigQueryConnector
        implements Connector
{
    private static final Logger LOG = Logger.get(BigQueryConnector.class);
    private final BigQuery bigQuery;

    @Inject
    public BigQueryConnector(BigQuery bigQuery)
    {
        this.bigQuery = requireNonNull(bigQuery, "bigQuery is null");
    }

    @Override
    public void createSchema(String name)
    {
        bigQuery.create(DatasetInfo.newBuilder(name).build());
    }

    @Override
    public boolean isSchemaExist(String name)
    {
        return getDataset(name).isPresent();
    }

    @Override
    public List<String> listSchemas()
    {
        String querySchemaName = format("SELECT schema_name FROM `region-%s.INFORMATION_SCHEMA.SCHEMATA`", bigQuery.getOptions().getLocation());
        QueryJobConfiguration queryJobConfiguration = QueryJobConfiguration.newBuilder(querySchemaName).build();
        try {
            TableResult result = bigQuery.query(queryJobConfiguration);
            return Streams.stream(result.getValues()).map(fieldValues -> fieldValues.get(0).getStringValue()).collect(toImmutableList());
        }
        catch (InterruptedException ex) {
            LOG.error(ex);
            throw new CmlException(GENERIC_INTERNAL_ERROR, ex);
        }
    }

    @Override
    public List<TableMetadata> listTables(String schemaName)
    {
        Optional<Dataset> dataset = getDataset(schemaName);
        if (dataset.isEmpty()) {
            throw new CmlException(NOT_FOUND, format("Dataset %s is not found", schemaName));
        }
        Page<Table> result = bigQuery.listTables(dataset.get().getDatasetId());
        return Streams.stream(result.iterateAll()).map(table -> {
            MetadataUtil.TableMetadataBuilder builder = tableMetadataBuilder(new SchemaTableName(table.getTableId().getDataset(), table.getTableId().getTable()));
            Table fullTable = bigQuery.getTable(table.getTableId());
            // TODO: type mapping
            fullTable.getDefinition().getSchema().getFields().forEach(field -> builder.column(field.getName(), VarcharType.VARCHAR, null));
            return builder.build();
        }).collect(toImmutableList());
    }

    @Override
    public List<String> listFunctionNames(String schemaName)
    {
        Optional<Dataset> dataset = getDataset(schemaName);
        if (dataset.isEmpty()) {
            throw new CmlException(NOT_FOUND, format("Dataset %s is not found", schemaName));
        }
        Page<Routine> routines = bigQuery.listRoutines(dataset.get().getDatasetId(), BigQuery.RoutineListOption.pageSize(100));
        if (routines == null) {
            throw new CmlException(NOT_FOUND, format("Dataset %s doesn't contain any routines.", dataset.get().getDatasetId()));
        }
        return Streams.stream(routines.iterateAll()).map(routine -> routine.getRoutineId().getRoutine()).collect(toImmutableList());
    }

    @Override
    public boolean directDDL(String sql)
    {
        if (sql == null) {
            return true;
        }
        try {
            QueryJobConfiguration queryJobConfiguration = QueryJobConfiguration.newBuilder(sql).build();
            bigQuery.query(queryJobConfiguration);
            return true;
        }
        catch (InterruptedException ex) {
            LOG.error(ex);
            return false;
        }
        catch (BigQueryException ex) {
            LOG.error(ex);
            LOG.error("Failed SQL: %s", sql);
            throw ex;
        }
    }

    private Optional<Dataset> getDataset(String name)
    {
        return Optional.ofNullable(bigQuery.getDataset(name));
    }

    @Override
    public Iterable<Object[]> directQuery(String sql)
    {
        requireNonNull(sql, "sql can't be null.");
        try {
            QueryJobConfiguration queryJobConfiguration = QueryJobConfiguration.newBuilder(sql).build();
            TableResult results = bigQuery.query(queryJobConfiguration);
            return Streams.stream(results.iterateAll()).map(AbstractCollection::toArray).collect(toImmutableList());
        }
        catch (InterruptedException ex) {
            LOG.error(ex);
            throw new CmlException(GENERIC_INTERNAL_ERROR, ex);
        }
        catch (BigQueryException ex) {
            LOG.error(ex);
            LOG.error("Failed SQL: %s", sql);
            throw ex;
        }
    }
}
