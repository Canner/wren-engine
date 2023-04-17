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

package io.graphmdl.main.connector.bigquery;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.Routine;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import io.airlift.log.Logger;
import io.graphmdl.base.Column;
import io.graphmdl.base.ConnectorRecordIterator;
import io.graphmdl.base.GraphMDLException;
import io.graphmdl.base.Parameter;
import io.graphmdl.base.metadata.SchemaTableName;
import io.graphmdl.base.metadata.TableMetadata;
import io.graphmdl.base.type.PGType;
import io.graphmdl.base.type.PGTypes;
import io.graphmdl.connector.bigquery.BigQueryClient;
import io.graphmdl.connector.bigquery.BigQueryType;
import io.graphmdl.connector.bigquery.GcsStorageClient;
import io.graphmdl.main.calcite.CustomCharsetJavaTypeFactoryImpl;
import io.graphmdl.main.calcite.GraphMDLSchemaUtil;
import io.graphmdl.main.metadata.Metadata;
import io.graphmdl.main.pgcatalog.function.PgFunctionRegistry;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.graphmdl.base.metadata.StandardErrorCode.GENERIC_USER_ERROR;
import static io.graphmdl.base.metadata.StandardErrorCode.NOT_FOUND;
import static io.graphmdl.base.metadata.TableMetadata.Builder.builder;
import static io.graphmdl.main.pgcatalog.PgCatalogUtils.PG_CATALOG_NAME;
import static io.graphmdl.main.pgcatalog.function.PgFunction.PG_FUNCTION_PATTERN;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.ARRAY_AGG;
import static org.apache.calcite.sql.type.OperandTypes.ONE_OR_MORE;

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
    private static final String PRE_AGGREGATION_FOLDER = format("pre-agg-%s", randomUUID());
    private final BigQueryClient bigQueryClient;

    private final PgFunctionRegistry pgFunctionRegistry = new PgFunctionRegistry();

    private final RelDataTypeFactory typeFactory;

    private final Map<String, SqlFunction> pgNameToBqFunction;

    private final String location;

    private final GcsStorageClient gcsStorageClient;

    private final Optional<String> bucketName;

    @Inject
    public BigQueryMetadata(BigQueryClient bigQueryClient, BigQueryConfig bigQueryConfig, GcsStorageClient gcsStorageClient)
    {
        this.bigQueryClient = requireNonNull(bigQueryClient, "bigQueryClient is null");
        requireNonNull(bigQueryConfig, "bigQueryConfig is null");
        this.typeFactory = new CustomCharsetJavaTypeFactoryImpl(UTF_8, getRelDataTypeSystem());
        this.pgNameToBqFunction = initPgNameToBqFunctions();
        this.location = bigQueryConfig.getLocation()
                .orElseThrow(() -> new GraphMDLException(GENERIC_USER_ERROR, "Location must be set"));
        this.gcsStorageClient = requireNonNull(gcsStorageClient, "gcsStorageClient is null");
        this.bucketName = bigQueryConfig.getBucketName();
    }

    /**
     * @return mapping table for pg function which can be replaced by bq function.
     */
    private Map<String, SqlFunction> initPgNameToBqFunctions()
    {
        // bq native function is not case-sensitive, so it is ok to this kind of SqlFunction ctor here.
        return ImmutableMap.<String, SqlFunction>builder()
                .put("trunc", new SqlFunction("trunc",
                        SqlKind.OTHER_FUNCTION,
                        ReturnTypes.explicit(typeFactory.createSqlType(SqlTypeName.DECIMAL)),
                        null, ONE_OR_MORE, SqlFunctionCategory.USER_DEFINED_FUNCTION))
                .put("generate_array", new SqlFunction("generate_array",
                        SqlKind.OTHER_FUNCTION,
                        ReturnTypes.explicit(typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.INTEGER), -1)),
                        null, ONE_OR_MORE, SqlFunctionCategory.USER_DEFINED_FUNCTION))
                .put("substr", new SqlFunction("substr",
                        SqlKind.OTHER_FUNCTION,
                        ReturnTypes.explicit(typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                        null, ONE_OR_MORE, SqlFunctionCategory.USER_DEFINED_FUNCTION))
                .put("concat", new SqlFunction("concat",
                        SqlKind.OTHER_FUNCTION,
                        ReturnTypes.explicit(typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                        null, ONE_OR_MORE, SqlFunctionCategory.USER_DEFINED_FUNCTION))
                .put("regexp_like", new SqlFunction("regexp_contains",
                        SqlKind.OTHER_FUNCTION,
                        ReturnTypes.explicit(typeFactory.createSqlType(SqlTypeName.BOOLEAN)),
                        null, ONE_OR_MORE, SqlFunctionCategory.USER_DEFINED_FUNCTION))
                .put("array_length", new SqlFunction("array_length",
                        SqlKind.OTHER_FUNCTION,
                        ReturnTypes.explicit(typeFactory.createSqlType(SqlTypeName.INTEGER)),
                        null, ONE_OR_MORE, SqlFunctionCategory.USER_DEFINED_FUNCTION))
                .put("array_agg", ARRAY_AGG)
                // calcite 1.33.0 add date_trunc
                // https://github.com/apache/calcite/commit/bdb4e1fb5df6fdca237dd0fa92209905c2f94d76
                .put("date_trunc", new SqlFunction("date_trunc",
                        SqlKind.OTHER_FUNCTION,
                        ReturnTypes.DATE_NULLABLE,
                        null, ONE_OR_MORE, SqlFunctionCategory.USER_DEFINED_FUNCTION))
                .build();
    }

    private String withPgCatalogPrefix(String identifier)
    {
        return PG_CATALOG_NAME + "." + identifier;
    }

    @Override
    public void createSchema(String name)
    {
        bigQueryClient.createSchema(DatasetInfo.newBuilder(name).setLocation(location).build());
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
            throw new GraphMDLException(NOT_FOUND, format("Dataset %s is not found", schemaName));
        }
        Iterable<Table> result = bigQueryClient.listTables(dataset.get().getDatasetId());
        return Streams.stream(result)
                .map(table -> {
                    TableMetadata.Builder builder = builder(
                            new SchemaTableName(table.getTableId().getDataset(), table.getTableId().getTable()));
                    Table fullTable = bigQueryClient.getTable(table.getTableId());
                    // TODO: type mapping
                    fullTable.getDefinition().getSchema().getFields()
                            .forEach(field -> builder.column(field.getName(), BigQueryType.toPGType(field.getType().getStandardType())));
                    return builder.build();
                })
                .collect(toImmutableList());
    }

    @Override
    public List<String> listFunctionNames(String schemaName)
    {
        Optional<Dataset> dataset = getDataset(schemaName);
        if (dataset.isEmpty()) {
            throw new GraphMDLException(NOT_FOUND, format("Dataset %s is not found", schemaName));
        }
        Iterable<Routine> routines = bigQueryClient.listRoutines(dataset.get().getDatasetId());
        if (routines == null) {
            throw new GraphMDLException(NOT_FOUND, format("Dataset %s doesn't contain any routines.", dataset.get().getDatasetId()));
        }
        return Streams.stream(routines).map(routine -> routine.getRoutineId().getRoutine()).map(routine -> {
            Matcher matcher = PG_FUNCTION_PATTERN.matcher(routine);
            if (matcher.find()) {
                return matcher.group("functionName");
            }
            throw new IllegalArgumentException(format("The name pattern of %s doesn't match PG_FUNCTION_PATTERN", routine));
        }).collect(toImmutableList());
    }

    @Override
    public String resolveFunction(String functionName, int numArgument)
    {
        String funcNameLowerCase = functionName.toLowerCase(ENGLISH);
        // lookup calcite operator table
        if (SqlStdOperatorTable.instance().getOperatorList().stream().anyMatch(sqlOperator -> sqlOperator.getName().equalsIgnoreCase(functionName))) {
            return functionName;
        }

        if (pgNameToBqFunction.containsKey(funcNameLowerCase)) {
            return pgNameToBqFunction.get(funcNameLowerCase).getName();
        }

        // PgFunction is an udf defined in `pg_catalog` dataset. Add dataset prefix to invoke it in global.
        return withPgCatalogPrefix(pgFunctionRegistry.getPgFunction(funcNameLowerCase, numArgument).getRemoteName());
    }

    @Override
    public GraphMDLSchemaUtil.Dialect getDialect()
    {
        return GraphMDLSchemaUtil.Dialect.BIGQUERY;
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
                .map(field -> {
                    PGType<?> type = BigQueryType.toPGType(field.getType().getStandardType());
                    if (field.getMode().equals(Field.Mode.REPEATED)) {
                        type = PGTypes.getArrayType(type.oid());
                    }
                    return new Column(field.getName(), type);
                })
                .collect(toImmutableList());
    }

    @Override
    public String getDefaultCatalog()
    {
        return bigQueryClient.getProjectId();
    }

    public String getRequiredBucketName()
    {
        return bucketName.orElseThrow(() -> new GraphMDLException(GENERIC_USER_ERROR, "Bucket name must be set"));
    }

    @Override
    public String createPreAggregation(String catalog, String schema, String name, String statement)
    {
        String path = format("%s/%s/%s/%s/%s/%s/*.parquet",
                getRequiredBucketName(),
                PRE_AGGREGATION_FOLDER,
                catalog,
                schema,
                name,
                randomUUID());
        String exportStatement = format("EXPORT DATA OPTIONS(\n" +
                        "  uri='gs://%s',\n" +
                        "  format='PARQUET',\n" +
                        "  overwrite=true) AS %s",
                path,
                statement);
        directDDL(exportStatement);
        return path;
    }

    @Override
    public void cleanPreAggregation()
    {
        bucketName.ifPresent(bucket -> {
            if (!gcsStorageClient.cleanFolders(bucket, PRE_AGGREGATION_FOLDER)) {
                LOG.error("Failed to clean pre-aggregation folder. Please check the bucket %s", getRequiredBucketName());
            }
        });
    }
}
