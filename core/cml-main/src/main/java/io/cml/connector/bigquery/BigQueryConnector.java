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

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Routine;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.airlift.log.Logger;
import io.cml.spi.CmlException;
import io.cml.spi.ConnectorRecordIterable;
import io.cml.spi.connector.Connector;
import io.cml.spi.metadata.CatalogName;
import io.cml.spi.metadata.ColumnMetadata;
import io.cml.spi.metadata.MaterializedViewDefinition;
import io.cml.spi.metadata.SchemaTableName;
import io.cml.spi.metadata.TableMetadata;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.google.cloud.bigquery.TableDefinition.Type.MATERIALIZED_VIEW;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.cml.spi.metadata.MetadataUtil.TableMetadataBuilder;
import static io.cml.spi.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.cml.spi.metadata.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class BigQueryConnector
        implements Connector
{
    private static final Logger LOG = Logger.get(BigQueryConnector.class);
    private final BigQueryClient bigQueryClient;

    @Inject
    public BigQueryConnector(BigQueryClient bigQueryClient)
    {
        this.bigQueryClient = requireNonNull(bigQueryClient, "bigQueryClient is null");
    }

    @Override
    public void createSchema(String name)
    {
        bigQueryClient.createSchema(DatasetInfo.newBuilder(name).build());
    }

    @Override
    public boolean isSchemaExist(String name)
    {
        return getDataset(name).isPresent();
    }

    @Override
    public List<String> listSchemas()
    {
        return Streams.stream(bigQueryClient.listDatasets(bigQueryClient.getProjectId()))
                .map(dataset -> dataset.getDatasetId().getDataset())
                .collect(toImmutableList());
    }

    @Override
    public List<TableMetadata> listTables(String schemaName)
    {
        Optional<Dataset> dataset = getDataset(schemaName);
        if (dataset.isEmpty()) {
            throw new CmlException(NOT_FOUND, format("Dataset %s is not found", schemaName));
        }
        Iterable<Table> result = bigQueryClient.listTables(dataset.get().getDatasetId());
        return Streams.stream(result)
                .map(table -> {
                    TableMetadataBuilder builder = tableMetadataBuilder(
                            new SchemaTableName(table.getTableId().getDataset(), table.getTableId().getTable()));
                    Table fullTable = bigQueryClient.getTable(table.getTableId());
                    // TODO: type mapping
                    fullTable.getDefinition().getSchema().getFields()
                            .forEach(field -> builder.column(field.getName(), BigQueryType.toPGType(field.getType().name()), null));
                    return builder.build();
                })
                .collect(toImmutableList());
    }

    @Override
    public List<MaterializedViewDefinition> listMaterializedViews(Optional<String> optSchemaName)
    {
        return optSchemaName.map(ImmutableList::of)
                .orElse(ImmutableList.copyOf(listSchemas()))
                .stream()
                .map(schemaName -> Dataset.of(schemaName).getDatasetId())
                .flatMap(schemaName -> Streams.stream(bigQueryClient.listTables(schemaName, MATERIALIZED_VIEW)))
                .map(table -> bigQueryClient.getTable(table.getTableId())) // get mv info
                .map(table -> new MaterializedViewDefinition(
                        new CatalogName(table.getTableId().getProject()),
                        new SchemaTableName(table.getTableId().getDataset(), table.getTableId().getTable()),
                        ((com.google.cloud.bigquery.MaterializedViewDefinition) table.getDefinition()).getQuery(),
                        table.getDefinition().getSchema().getFields().stream()
                                .map(field ->
                                        ColumnMetadata.builder()
                                                .setName(field.getName())
                                                .setType(BigQueryType.toPGType(field.getType().name()))
                                                .build())
                                .collect(toImmutableList())))
                .collect(toImmutableList());
    }

    @Override
    public List<String> listFunctionNames(String schemaName)
    {
        Optional<Dataset> dataset = getDataset(schemaName);
        if (dataset.isEmpty()) {
            throw new CmlException(NOT_FOUND, format("Dataset %s is not found", schemaName));
        }
        Iterable<Routine> routines = bigQueryClient.listRoutines(dataset.get().getDatasetId());
        if (routines == null) {
            throw new CmlException(NOT_FOUND, format("Dataset %s doesn't contain any routines.", dataset.get().getDatasetId()));
        }
        return Streams.stream(routines).map(routine -> routine.getRoutineId().getRoutine()).collect(toImmutableList());
    }

    @Override
    public void directDDL(String sql)
    {
        try {
            bigQueryClient.query(sql);
        }
        catch (Exception ex) {
            LOG.error(ex, "Failed SQL: %s", sql);
            throw ex;
        }
    }

    private Optional<Dataset> getDataset(String name)
    {
        return Optional.ofNullable(bigQueryClient.getDataset(name));
    }

    @Override
    public ConnectorRecordIterable directQuery(String sql)
    {
        requireNonNull(sql, "sql can't be null.");
        try {
            TableResult results = bigQueryClient.query(sql);
            return BigQueryRecordIterable.of(results);
        }
        catch (BigQueryException ex) {
            LOG.error(ex);
            LOG.error("Failed SQL: %s", sql);
            throw ex;
        }
    }

    @Override
    public String getCatalogName()
    {
        return bigQueryClient.getProjectId();
    }
}
