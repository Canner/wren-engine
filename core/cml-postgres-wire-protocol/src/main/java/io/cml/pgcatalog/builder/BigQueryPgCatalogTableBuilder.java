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

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static io.cml.pgcatalog.PgCatalogUtils.CML_TEMP_NAME;
import static io.cml.pgcatalog.PgCatalogUtils.PG_CATALOG_NAME;
import static io.cml.pgcatalog.builder.BigQueryUtils.createOrReplaceAllColumn;
import static io.cml.pgcatalog.builder.BigQueryUtils.createOrReplaceAllTable;
import static io.cml.pgcatalog.builder.BigQueryUtils.getOidToBqType;
import static io.cml.pgcatalog.builder.PgCatalogTableBuilderUtils.generatePgTypeRecords;
import static io.cml.spi.type.CharType.CHAR;
import static io.cml.spi.type.RegprocType.REGPROC;
import static io.cml.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public final class BigQueryPgCatalogTableBuilder
        extends PgCatalogTableBuilder
{
    @Inject
    public BigQueryPgCatalogTableBuilder(Connector connector)
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
        return getOidToBqType();
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
