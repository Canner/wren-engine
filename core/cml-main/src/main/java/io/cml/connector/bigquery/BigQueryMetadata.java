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
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.Routine;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.airlift.log.Logger;
import io.cml.calcite.CalciteTypes;
import io.cml.calcite.CmlSchemaUtil;
import io.cml.calcite.CustomCharsetJavaTypeFactoryImpl;
import io.cml.calcite.QueryProcessor;
import io.cml.metadata.ColumnSchema;
import io.cml.metadata.ConnectorTableSchema;
import io.cml.metadata.Metadata;
import io.cml.metadata.TableHandle;
import io.cml.metadata.TableSchema;
import io.cml.pgcatalog.function.PgFunction;
import io.cml.pgcatalog.function.PgFunctionRegistry;
import io.cml.spi.CmlException;
import io.cml.spi.Column;
import io.cml.spi.ConnectorRecordIterator;
import io.cml.spi.Parameter;
import io.cml.spi.SessionContext;
import io.cml.spi.metadata.CatalogName;
import io.cml.spi.metadata.ColumnMetadata;
import io.cml.spi.metadata.MaterializedViewDefinition;
import io.cml.spi.metadata.SchemaTableName;
import io.cml.spi.metadata.TableMetadata;
import io.cml.spi.type.PGArray;
import io.cml.spi.type.PGType;
import io.cml.spi.type.PGTypes;
import io.cml.sql.QualifiedObjectName;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ListSqlOperatorTable;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.cml.connector.bigquery.BigQueryType.toPGType;
import static io.cml.pgcatalog.PgCatalogUtils.PG_CATALOG_NAME;
import static io.cml.pgcatalog.function.PgFunction.PG_FUNCTION_PATTERN;
import static io.cml.spi.metadata.MetadataUtil.TableMetadataBuilder;
import static io.cml.spi.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.cml.spi.metadata.StandardErrorCode.GENERIC_USER_ERROR;
import static io.cml.spi.metadata.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
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
    private final BigQueryClient bigQueryClient;

    private final PgFunctionRegistry pgFunctionRegistry = new PgFunctionRegistry();

    private final RelDataTypeFactory typeFactory;

    private final List<SqlFunction> supportedBqFunction;

    private final SqlOperatorTable calciteOperatorTable;

    private final String location;

    @Inject
    public BigQueryMetadata(BigQueryClient bigQueryClient, BigQueryConfig bigQueryConfig)
    {
        this.bigQueryClient = requireNonNull(bigQueryClient, "bigQueryClient is null");
        requireNonNull(bigQueryConfig, "bigQueryConfig is null");
        this.typeFactory = new CustomCharsetJavaTypeFactoryImpl(UTF_8, getRelDataTypeSystem());
        this.supportedBqFunction = initBqFunctions();
        this.calciteOperatorTable = initCalciteOperators();
        this.location = bigQueryConfig.getLocation()
                .orElseThrow(() -> new CmlException(GENERIC_USER_ERROR, "Location must be set"));

        String mvSchema = getMaterializedViewSchema();
        if (!listSchemas().contains(mvSchema)) {
            createSchema(mvSchema);
        }
    }

    private List<SqlFunction> initBqFunctions()
    {
        // bq native function is not case-sensitive, so it is ok to this kind of SqlFunction ctor here.
        return ImmutableList.of(
                new SqlFunction("trunc",
                        SqlKind.OTHER_FUNCTION,
                        ReturnTypes.explicit(typeFactory.createSqlType(SqlTypeName.DECIMAL)),
                        null, ONE_OR_MORE, SqlFunctionCategory.USER_DEFINED_FUNCTION),
                new SqlFunction("generate_array",
                        SqlKind.OTHER_FUNCTION,
                        ReturnTypes.explicit(typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.INTEGER), -1)),
                        null, ONE_OR_MORE, SqlFunctionCategory.USER_DEFINED_FUNCTION));
    }

    private ListSqlOperatorTable initCalciteOperators()
    {
        ListSqlOperatorTable listSqlOperatorTable = new ListSqlOperatorTable();
        for (PgFunction pgFunction : pgFunctionRegistry.getPgFunctions()) {
            listSqlOperatorTable.add(toCalciteSqlFunction(pgFunction));
        }
        supportedBqFunction.forEach(listSqlOperatorTable::add);
        return listSqlOperatorTable;
    }

    private SqlFunction toCalciteSqlFunction(PgFunction pgFunction)
    {
        return new SqlFunction(new SqlIdentifier(withPgCatalogPrefix(pgFunction.getRemoteName()), SqlParserPos.ZERO),
                pgFunction.getReturnType().map(type -> ReturnTypes.explicit(toCalciteType(pgFunction.getReturnType().get()))).orElse(null),
                null,
                pgFunction.getArguments().map(ignored -> ONE_OR_MORE).orElse(null),
                pgFunction.getArguments().map(arguments -> pgFunction.getArguments().get().stream().map(argument -> toCalciteType(argument.getType())).collect(Collectors.toList())).orElse(null),
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    private String withPgCatalogPrefix(String identifier)
    {
        return PG_CATALOG_NAME + "." + identifier;
    }

    private RelDataType toCalciteType(PGType<?> pgType)
    {
        if (pgType instanceof PGArray) {
            return typeFactory.createArrayType(toCalciteType(((PGArray) pgType).getInnerType()), -1);
        }
        return typeFactory.createSqlType(CalciteTypes.toCalciteType(pgType));
    }

    @Override
    public SqlOperatorTable getCalciteOperatorTable()
    {
        return calciteOperatorTable;
    }

    @Override
    public RelDataTypeFactory getTypeFactory()
    {
        return typeFactory;
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
    public void createMaterializedView(SchemaTableName schemaTableName, String pgSql)
    {
        try {
            String bqSql = QueryProcessor.of(this).convert(pgSql, SessionContext.builder().enableMVReplacement(false).build());
            bigQueryClient.query(format("CREATE TABLE %s AS %s", schemaTableName.toString(), bqSql), List.of());
            bigQueryClient.updateTable(
                    bigQueryClient.getTable(TableId.of(schemaTableName.getSchemaName(), schemaTableName.getTableName()))
                            .toBuilder().setDescription(pgSql).build());
        }
        catch (Exception ex) {
            try {
                bigQueryClient.dropTable(schemaTableName);
            }
            catch (Exception ex2) {
                LOG.error(ex2, "drop mv failed in createMaterializedView");
            }
            throw ex;
        }
    }

    @Override
    public List<MaterializedViewDefinition> listMaterializedViews()
    {
        return Streams.stream(bigQueryClient.listTables(DatasetId.of(getMaterializedViewSchema())))
                .map(table -> bigQueryClient.getTable(table.getTableId())) // get detailed table info
                // Filter mv that was created, but not update desc yet. There are two steps in
                // BigQueryMetadata#createMaterializedView. 1. Do CTAS query 2. Update mv sql in table desc.
                // There is a gap between step1 & step2. If listMaterializedViews is triggered while mv are
                // created right after step1 and start before step2. We will get mvs that don't contain sql in desc.
                // Add null check `table != null` here since there might exist a case that when we listTables
                // mv still exist, while when we iterate through table list to get detailed info, mv was deleted,
                // and bigQueryClient getTable will return null (i.e. table not found)
                .filter(table -> table != null && table.getDescription() != null)
                .map(table -> new MaterializedViewDefinition(
                        new CatalogName(table.getTableId().getProject()),
                        new SchemaTableName(table.getTableId().getDataset(), table.getTableId().getTable()),
                        table.getDescription(), // we store sql in table desc
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
        // lookup calcite operator table and bigquery supported table
        if (SqlStdOperatorTable.instance().getOperatorList().stream()
                .anyMatch(sqlOperator -> sqlOperator.getName().equalsIgnoreCase(functionName)) ||
                supportedBqFunction.stream().anyMatch(sqlFunction -> sqlFunction.getName().equalsIgnoreCase(functionName))) {
            return functionName;
        }
        // PgFunction is an udf defined in `pg_catalog` dataset. Add dataset prefix to invoke it in global.
        return withPgCatalogPrefix(pgFunctionRegistry.getPgFunction(functionName, numArgument).getRemoteName());
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
                .map(field -> {
                    PGType<?> type = toPGType(field.getType().getStandardType());
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
}
