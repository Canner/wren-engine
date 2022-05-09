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

package io.cml.pgcatalog.builder;

import com.google.common.collect.ImmutableMap;
import io.cml.pgcatalog.table.PgCatalogTable;
import io.cml.spi.connector.Connector;
import io.cml.spi.metadata.ColumnMetadata;
import io.cml.spi.type.PGType;
import io.cml.type.PGArray;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static io.cml.pgcatalog.PgCatalogUtils.CML_TEMP_NAME;
import static io.cml.pgcatalog.PgCatalogUtils.PG_CATALOG_NAME;
import static io.cml.pgcatalog.builder.BigQuerySqls.createOrReplaceAllColumn;
import static io.cml.pgcatalog.builder.BigQuerySqls.createOrReplaceAllTable;
import static io.cml.pgcatalog.builder.PgCatalogBuilderUtils.generatePgTypeRecords;
import static io.cml.type.BigIntType.BIGINT;
import static io.cml.type.BooleanType.BOOLEAN;
import static io.cml.type.BpCharType.BPCHAR;
import static io.cml.type.ByteaType.BYTEA;
import static io.cml.type.CharType.CHAR;
import static io.cml.type.DateType.DATE;
import static io.cml.type.DoubleType.DOUBLE;
import static io.cml.type.InetType.INET;
import static io.cml.type.IntegerType.INTEGER;
import static io.cml.type.JsonType.JSON;
import static io.cml.type.NumericType.NUMERIC;
import static io.cml.type.OidType.OID_INSTANCE;
import static io.cml.type.RealType.REAL;
import static io.cml.type.RegprocType.REGPROC;
import static io.cml.type.SmallIntType.SMALLINT;
import static io.cml.type.TimestampType.TIMESTAMP;
import static io.cml.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIMEZONE;
import static io.cml.type.UuidType.UUID;
import static io.cml.type.VarcharType.NameType.NAME;
import static io.cml.type.VarcharType.TextType.TEXT;
import static io.cml.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public final class BigQueryPgCatalogBuilder
        extends PgCatalogBuilder
{
    @Inject
    public BigQueryPgCatalogBuilder(Connector connector)
    {
        super(connector);
    }

    @Override
    protected Map<String, String> initReplaceMap()
    {
        return ImmutableMap.<String, String>builder()
                .put("hash", "FARM_FINGERPRINT")
                .put("tableName", "table_name")
                .put("schemaName", "table_schema")
                .put("columnName", "column_name")
                .put("typeOid", "typoid")
                .put("typeLen", "typlen")
                .put("columNum", "ordinal_position")
                .put("catalogName", "table_catalog")
                .build();
    }

    @Override
    protected Map<Integer, String> initOidToTypeMap()
    {
        ImmutableMap.Builder<Integer, String> builder = ImmutableMap.<Integer, String>builder()
                .put(BOOLEAN.oid(), "BOOL")
                .put(SMALLINT.oid(), "SMALLINT")
                .put(INTEGER.oid(), "INTEGER")
                .put(BIGINT.oid(), "BIGINT")
                .put(REAL.oid(), "FLOAT64") // BigQuery only has FLOAT64 for floating point type
                .put(DOUBLE.oid(), "FLOAT64")
                .put(NUMERIC.oid(), "NUMERIC")
                .put(VARCHAR.oid(), "STRING")
                .put(CHAR.oid(), "STRING")
                .put(JSON.oid(), "JSON")
                .put(TIMESTAMP.oid(), "TIMESTAMP")
                .put(TIMESTAMP_WITH_TIMEZONE.oid(), "TIMESTAMP")
                .put(TEXT.oid(), "STRING")
                .put(NAME.oid(), "STRING")
                .put(OID_INSTANCE.oid(), "INTEGER")
                .put(DATE.oid(), "DATE")
                .put(BYTEA.oid(), "BYTES")
                .put(BPCHAR.oid(), "STRING")
                .put(INET.oid(), "INET")
                .put(UUID.oid(), "STRING")
                .put(REGPROC.oid(), "STRING");
        // TODO: support record type, hstore
        // .put(EMPTY_RECORD.oid(), "STRUCT")
        // .put(HSTORE.oid(), "STRUCT")

        Map<Integer, String> simpleTypeMap = builder.build();

        for (PGArray pgArray : PGArray.allArray()) {
            String innerType = simpleTypeMap.get(pgArray.getInnerType().oid());
            builder.put(pgArray.oid(), format("ARRAY<%s>", innerType));
        }
        return builder.build();
    }

    @Override
    protected String createPgClass(PgCatalogTable pgCatalogTable)
    {
        getConnector().directDDL(createOrReplaceAllTable(getConnector()));
        StringBuilder builder = new StringBuilder();
        builder.append(format("CREATE OR REPLACE VIEW `%s.%s` AS SELECT ", PG_CATALOG_NAME, pgCatalogTable.getName()));
        for (ColumnMetadata columnMetadata : pgCatalogTable.getTableMetadata().getColumns()) {
            builder.append(format("%s AS `%s`,", columnMetadata.getValue(), columnMetadata.getName()));
        }
        builder.setLength(builder.length() - 1);
        builder.append(format("FROM `%s.all_tables`;", CML_TEMP_NAME));
        return builder.toString();
    }

    @Override
    protected String createPgType(PgCatalogTable pgCatalogTable)
    {
        List<Object[]> typeRecords = generatePgTypeRecords(pgCatalogTable, getOidToTypeMap());
        List<ColumnMetadata> columnMetadata = pgCatalogTable.getTableMetadata().getColumns();

        StringBuilder recordBuilder = new StringBuilder();
        for (Object[] typeRecord : typeRecords) {
            recordBuilder.append("(");
            for (int i = 0; i < columnMetadata.size(); i++) {
                recordBuilder.append(quotedIfNeed(typeRecord[i], columnMetadata.get(i).getType())).append(",");
            }
            recordBuilder.setLength(recordBuilder.length() - 1);
            recordBuilder.append(")").append(",");
        }
        recordBuilder.setLength(recordBuilder.length() - 1);
        return buildPgCatalogTableView(PG_CATALOG_NAME, pgCatalogTable.getName(), buildColumnDefinition(columnMetadata), recordBuilder.toString(), false);
    }

    @Override
    protected String createPgAmTable(PgCatalogTable pgCatalogTable)
    {
        return buildEmptyTableView(pgCatalogTable);
    }

    @Override
    protected String createPgAttributeTable(PgCatalogTable pgCatalogTable)
    {
        getConnector().directDDL(createOrReplaceAllColumn(getConnector()));
        StringBuilder builder = new StringBuilder();
        builder.append(format("CREATE OR REPLACE VIEW `%s.%s` AS SELECT ", PG_CATALOG_NAME, pgCatalogTable.getName()));
        for (ColumnMetadata columnMetadata : pgCatalogTable.getTableMetadata().getColumns()) {
            builder.append(format("%s AS `%s`,", columnMetadata.getValue(), columnMetadata.getName()));
        }
        builder.setLength(builder.length() - 1);
        builder.append(format("FROM `%s.all_columns`;", CML_TEMP_NAME));
        return builder.toString();
    }

    @Override
    protected String createPgAttrdefTable(PgCatalogTable pgCatalogTable)
    {
        return buildEmptyTableView(pgCatalogTable);
    }

    @Override
    protected String createPgConstraintTable(PgCatalogTable pgCatalogTable)
    {
        return buildEmptyTableView(pgCatalogTable);
    }

    @Override
    protected String createPgDatabaseTable(PgCatalogTable pgCatalogTable)
    {
        // TODO get project id from config
        getConnector().directDDL(createOrReplaceAllTable(getConnector()));
        StringBuilder builder = new StringBuilder();
        builder.append(format("CREATE OR REPLACE VIEW `%s.%s` AS SELECT DISTINCT ", PG_CATALOG_NAME, pgCatalogTable.getName()));
        for (ColumnMetadata columnMetadata : pgCatalogTable.getTableMetadata().getColumns()) {
            builder.append(format("%s AS `%s`,", columnMetadata.getValue(), columnMetadata.getName()));
        }
        builder.setLength(builder.length() - 1);
        builder.append(format("FROM `%s.all_tables`;", CML_TEMP_NAME));
        return builder.toString();
    }

    @Override
    protected String createPgDescriptionTable(PgCatalogTable pgCatalogTable)
    {
        return buildEmptyTableView(pgCatalogTable);
    }

    @Override
    protected String createPgEnumTable(PgCatalogTable pgCatalogTable)
    {
        return buildEmptyTableView(pgCatalogTable);
    }

    @Override
    protected String createPgIndexTable(PgCatalogTable pgCatalogTable)
    {
        return buildEmptyTableView(pgCatalogTable);
    }

    @Override
    protected String createPgNamespaceTable(PgCatalogTable pgCatalogTable)
    {
        getConnector().directDDL(createOrReplaceAllTable(getConnector()));
        StringBuilder builder = new StringBuilder();
        builder.append(format("CREATE OR REPLACE VIEW `%s.%s` AS SELECT DISTINCT ", PG_CATALOG_NAME, pgCatalogTable.getName()));
        for (ColumnMetadata columnMetadata : pgCatalogTable.getTableMetadata().getColumns()) {
            builder.append(format("%s AS `%s`,", columnMetadata.getValue(), columnMetadata.getName()));
        }
        builder.setLength(builder.length() - 1);
        builder.append(format("FROM `%s.all_tables`;", CML_TEMP_NAME));
        return builder.toString();
    }

    @Override
    protected String createPgProcTable(PgCatalogTable pgCatalogTable)
    {
        // TODO list bigQuery's function and cml-implemented pg functions
        return buildEmptyTableView(pgCatalogTable);
    }

    @Override
    protected String createPgRangeTable(PgCatalogTable pgCatalogTable)
    {
        return buildEmptyTableView(pgCatalogTable);
    }

    @Override
    protected String createPgRoleTable(PgCatalogTable pgCatalogTable)
    {
        // TODO return user
        return buildEmptyTableView(pgCatalogTable);
    }

    @Override
    protected String createPgSettingsTable(PgCatalogTable pgCatalogTable)
    {
        return buildEmptyTableView(pgCatalogTable);
    }

    @Override
    protected String createPgTablespaceTable(PgCatalogTable pgCatalogTable)
    {
        return buildEmptyTableView(pgCatalogTable);
    }

    @Override
    protected String createCharacterSets(PgCatalogTable pgCatalogTable)
    {
        getConnector().directDDL(createOrReplaceAllTable(getConnector()));
        StringBuilder builder = new StringBuilder();
        builder.append(format("CREATE OR REPLACE VIEW `%s.%s` AS SELECT DISTINCT ", PG_CATALOG_NAME, pgCatalogTable.getName()));
        for (ColumnMetadata columnMetadata : pgCatalogTable.getTableMetadata().getColumns()) {
            builder.append(format("%s AS `%s`,", columnMetadata.getValue(), columnMetadata.getName()));
        }
        builder.setLength(builder.length() - 1);
        builder.append(format("FROM `%s.all_tables`;", CML_TEMP_NAME));
        return builder.toString();
    }

    @Override
    protected String createReferentialConstraints(PgCatalogTable pgCatalogTable)
    {
        return buildEmptyTableView(pgCatalogTable);
    }

    @Override
    protected String createKeyColumnUsage(PgCatalogTable pgCatalogTable)
    {
        return buildEmptyTableView(pgCatalogTable);
    }

    @Override
    protected String createTableConstraints(PgCatalogTable pgCatalogTable)
    {
        return buildEmptyTableView(pgCatalogTable);
    }

    private static String quotedIfNeed(Object value, PGType<?> type)
    {
        if (value == null) {
            return "null";
        }
        if (type.oid() == VARCHAR.oid() || type.oid() == CHAR.oid() || type.oid() == REGPROC.oid()) {
            return "'" + value + "'";
        }
        return value.toString();
    }

    private String buildColumnDefinition(List<ColumnMetadata> columnMetadatas)
    {
        StringBuilder metadataBuilder = new StringBuilder();
        for (ColumnMetadata columnMetadata : columnMetadatas) {
            metadataBuilder.append(columnMetadata.getName())
                    .append(" ")
                    .append(getOidToTypeMap().get(columnMetadata.getType().oid()))
                    .append(",");
        }
        metadataBuilder.setLength(metadataBuilder.length() - 1);
        return metadataBuilder.toString();
    }

    private String buildPgCatalogTableView(String catalogName, String viewName, String columnDefinition, String records, boolean isEmpty)
    {
        String viewDefinition = format("CREATE OR REPLACE VIEW `%s.%s` AS SELECT * FROM UNNEST([STRUCT<%s> %s])", catalogName, viewName, columnDefinition, records);
        return isEmpty ? viewDefinition + " LIMIT 0;" : viewDefinition + ";";
    }

    private String buildEmptyValue(int size)
    {
        String value = IntStream.range(0, size).mapToObj(ignored -> "null").collect(joining(","));
        return format("(%s)", value);
    }

    private String buildEmptyTableView(PgCatalogTable pgCatalogTable)
    {
        List<ColumnMetadata> columnMetadata = pgCatalogTable.getTableMetadata().getColumns();
        return buildPgCatalogTableView(
                PG_CATALOG_NAME,
                pgCatalogTable.getName(),
                buildColumnDefinition(columnMetadata),
                buildEmptyValue(columnMetadata.size()),
                true);
    }
}
