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

package io.accio.main.connector.bigquery;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.Routine;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import io.accio.base.AccioException;
import io.accio.base.Column;
import io.accio.base.ConnectorRecordIterator;
import io.accio.base.Parameter;
import io.accio.base.metadata.SchemaTableName;
import io.accio.base.metadata.TableMetadata;
import io.accio.base.pgcatalog.function.PgFunctionRegistry;
import io.accio.connector.bigquery.BigQueryClient;
import io.accio.connector.bigquery.BigQueryType;
import io.accio.main.metadata.Metadata;
import io.airlift.log.Logger;
import io.trino.sql.tree.QualifiedName;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.accio.base.metadata.StandardErrorCode.GENERIC_USER_ERROR;
import static io.accio.base.metadata.StandardErrorCode.NOT_FOUND;
import static io.accio.main.pgcatalog.PgCatalogUtils.ACCIO_TEMP_NAME;
import static io.accio.main.pgcatalog.PgCatalogUtils.PG_CATALOG_NAME;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class BigQueryMetadata
        implements Metadata
{
    private static final Logger LOG = Logger.get(BigQueryMetadata.class);
    private final BigQueryClient bigQueryClient;

    private final PgFunctionRegistry pgFunctionRegistry;

    private final Map<String, String> pgToBqFunctionNameMappings;

    private final String location;
    private final String metadataSchemaName;
    private final String pgCatalogName;

    @Inject
    public BigQueryMetadata(BigQueryClient bigQueryClient, BigQueryConfig bigQueryConfig)
    {
        this.bigQueryClient = requireNonNull(bigQueryClient, "bigQueryClient is null");
        requireNonNull(bigQueryConfig, "bigQueryConfig is null");
        this.pgToBqFunctionNameMappings = initPgNameToBqFunctions();
        this.location = bigQueryConfig.getLocation()
                .orElseThrow(() -> new AccioException(GENERIC_USER_ERROR, "Location must be set"));
        this.metadataSchemaName = bigQueryConfig.getMetadataSchemaPrefix() + ACCIO_TEMP_NAME;
        this.pgCatalogName = bigQueryConfig.getMetadataSchemaPrefix() + PG_CATALOG_NAME;
        this.pgFunctionRegistry = new PgFunctionRegistry(pgCatalogName);
    }

    /**
     * @return mapping table for pg function which can be replaced by bq function.
     */
    private Map<String, String> initPgNameToBqFunctions()
    {
        // bq native function is not case-sensitive, so it is ok to this kind of SqlFunction ctor here.
        return ImmutableMap.<String, String>builder()
                .put("regexp_like", "regexp_contains")
                .build();
    }

    @Override
    public void createSchema(String name)
    {
        bigQueryClient.createSchema(DatasetInfo.newBuilder(name).setLocation(location).build());
    }

    @Override
    public void dropSchemaIfExists(String name)
    {
        bigQueryClient.deleteSchema(DatasetId.of(name));
    }

    @Override
    public boolean isSchemaExist(String name)
    {
        return getDataset(name).isPresent();
    }

    @Override
    public List<String> listSchemas()
    {
        // TODO: https://github.com/Canner/canner-metric-layer/issues/47
        //  Getting full dataset information is a heavy cost. It's better to find another way to list dataset by region.
        return Streams.stream(bigQueryClient.listDatasets(bigQueryClient.getProjectId()))
                .map(bigQueryClient::getDataSet)
                .filter(dataset -> location.equalsIgnoreCase(dataset.getLocation()))
                .map(dataset -> dataset.getDatasetId().getDataset())
                .collect(toImmutableList());
    }

    @Override
    public List<TableMetadata> listTables(String schemaName)
    {
        Optional<Dataset> dataset = getDataset(schemaName);
        if (dataset.isEmpty()) {
            throw new AccioException(NOT_FOUND, format("Dataset %s is not found", schemaName));
        }
        Iterable<Table> result = bigQueryClient.listTables(dataset.get().getDatasetId());
        return Streams.stream(result)
                .map(table -> {
                    TableMetadata.Builder builder = TableMetadata.builder(
                            new SchemaTableName(table.getTableId().getDataset(), table.getTableId().getTable()));
                    Table fullTable = bigQueryClient.getTable(table.getTableId());
                    // TODO: type mapping
                    fullTable.getDefinition().getSchema().getFields()
                            .forEach(field -> builder.column(field.getName(), BigQueryType.toPGType(field)));
                    return builder.build();
                })
                .collect(toImmutableList());
    }

    @Override
    public List<String> listFunctionNames(String schemaName)
    {
        Optional<Dataset> dataset = getDataset(schemaName);
        if (dataset.isEmpty()) {
            throw new AccioException(NOT_FOUND, format("Dataset %s is not found", schemaName));
        }
        Iterable<Routine> routines = bigQueryClient.listRoutines(dataset.get().getDatasetId());
        if (routines == null) {
            throw new AccioException(NOT_FOUND, format("Dataset %s doesn't contain any routines.", dataset.get().getDatasetId()));
        }
        return Streams.stream(routines).map(routine -> routine.getRoutineId().getRoutine()).collect(toImmutableList());
    }

    @Override
    public QualifiedName resolveFunction(String functionName, int numArgument)
    {
        String funcNameLowerCase = functionName.toLowerCase(ENGLISH);

        if (pgToBqFunctionNameMappings.containsKey(funcNameLowerCase)) {
            return QualifiedName.of(pgToBqFunctionNameMappings.get(funcNameLowerCase));
        }

        // PgFunction is an udf defined in `pg_catalog` dataset. Add dataset prefix to invoke it in global.
        if (pgFunctionRegistry.getFunction(funcNameLowerCase, numArgument).isPresent()) {
            return QualifiedName.of(pgCatalogName, pgFunctionRegistry.getFunction(funcNameLowerCase, numArgument).get().getRemoteName());
        }

        return QualifiedName.of(functionName);
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
        return queryStatistics.getSchema().getFields().stream()
                .map(field -> new Column(field.getName(), BigQueryType.toPGType(field)))
                .collect(toImmutableList());
    }

    @Override
    public String getDefaultCatalog()
    {
        return bigQueryClient.getProjectId();
    }

    @Override
    public boolean isPgCompatible()
    {
        return false;
    }

    @Override
    public String getMetadataSchemaName()
    {
        return metadataSchemaName;
    }

    @Override
    public String getPgCatalogName()
    {
        return pgCatalogName;
    }
}
