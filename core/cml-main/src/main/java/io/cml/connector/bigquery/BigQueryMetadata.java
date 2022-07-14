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
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.Routine;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.airlift.log.Logger;
import io.cml.calcite.CmlSchemaUtil;
import io.cml.metadata.ColumnSchema;
import io.cml.metadata.ConnectorTableSchema;
import io.cml.metadata.Metadata;
import io.cml.metadata.TableHandle;
import io.cml.metadata.TableSchema;
import io.cml.spi.CmlException;
import io.cml.spi.Column;
import io.cml.spi.ConnectorRecordIterator;
import io.cml.spi.Parameter;
import io.cml.spi.metadata.CatalogName;
import io.cml.spi.metadata.ColumnMetadata;
import io.cml.spi.metadata.MaterializedViewDefinition;
import io.cml.spi.metadata.SchemaTableName;
import io.cml.spi.metadata.TableMetadata;
import io.cml.sql.QualifiedObjectName;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.google.cloud.bigquery.TableDefinition.Type.MATERIALIZED_VIEW;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.cml.connector.bigquery.BigQueryType.toPGType;
import static io.cml.spi.metadata.MetadataUtil.TableMetadataBuilder;
import static io.cml.spi.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.cml.spi.metadata.StandardErrorCode.GENERIC_USER_ERROR;
import static io.cml.spi.metadata.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class BigQueryMetadata
        implements Metadata
{
    private static final RelDataTypeSystem BIGQUERY_TYPE_SYSTEM =
            new RelDataTypeSystemImpl()
            {
                @Override
                public int getMaxNumericPrecision()
                {
                    // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
                    return 76;
                }

                @Override
                public int getMaxNumericScale()
                {
                    // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
                    return 38;
                }
            };
    private static final Logger LOG = Logger.get(BigQueryMetadata.class);
    private final BigQueryClient bigQueryClient;
    private final BigQueryConfig bigQueryConfig;

    @Inject
    public BigQueryMetadata(BigQueryClient bigQueryClient, BigQueryConfig bigQueryConfig)
    {
        this.bigQueryClient = requireNonNull(bigQueryClient, "bigQueryClient is null");
        this.bigQueryConfig = requireNonNull(bigQueryConfig, "bigQueryConfig is null");
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
        return Streams.stream(bigQueryClient.listDatasets(bigQueryConfig.getLocation()
                        .orElseThrow(() -> new CmlException(GENERIC_USER_ERROR, "bigquery client location should be set"))))
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
                            .forEach(field -> builder.column(field.getName(), toPGType(field.getType().getStandardType()), null));
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
                                                .setType(toPGType(field.getType().getStandardType()))
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
    public TableSchema getTableSchema(TableHandle tableHandle)
    {
        Table table = bigQueryClient.getTable(tableHandle);
        return new TableSchema(
                tableHandle.getCatalogName(),
                new ConnectorTableSchema(
                        tableHandle.getSchemaTableName(),
                        table.getDefinition().getSchema().getFields().stream()
                                .map(field ->
                                        ColumnSchema.builder()
                                                .setName(field.getName())
                                                .setType(toPGType(field.getType().getStandardType()))
                                                .build())
                                .collect(toImmutableList())));
    }

    @Override
    public Optional<TableHandle> getTableHandle(QualifiedObjectName tableName)
    {
        return Optional.of(
                new TableHandle(
                        new CatalogName(tableName.getCatalogName()),
                        new SchemaTableName(tableName.getSchemaName(), tableName.getObjectName())));
    }

    @Override
    public CmlSchemaUtil.Dialect getDialect()
    {
        return CmlSchemaUtil.Dialect.BIGQUERY;
    }

    @Override
    public RelDataTypeSystem getRelDataTypeSystem()
    {
        return BIGQUERY_TYPE_SYSTEM;
    }

    @Override
    public void directDDL(String sql)
    {
        try {
            bigQueryClient.query(sql, ImmutableList.of());
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
    public ConnectorRecordIterator directQuery(String sql, List<Parameter> parameters)
    {
        requireNonNull(sql, "sql can't be null.");
        try {
            TableResult results = bigQueryClient.query(sql, parameters);
            return BigQueryRecordIterator.of(results);
        }
        catch (BigQueryException ex) {
            LOG.error(ex);
            LOG.error("Failed SQL: %s", sql);
            throw ex;
        }
    }

    @Override
    public List<Column> describeQuery(String sql, List<Parameter> parameters)
    {
        JobStatistics.QueryStatistics queryStatistics = bigQueryClient.queryDryRun(Optional.empty(), sql, parameters);
        return Streams.stream(queryStatistics.getSchema().getFields().iterator())
                .map(field -> new Column(field.getName(), toPGType(field.getType().getStandardType())))
                .collect(toImmutableList());
    }

    @Override
    public String getDefaultCatalog()
    {
        return bigQueryClient.getProjectId();
    }
}
