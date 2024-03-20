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

package io.wren.main.pgcatalog.builder;

import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.common.collect.ImmutableMap;
import io.wren.base.WrenException;
import io.wren.base.WrenMDL;
import io.wren.base.dto.Column;
import io.wren.base.dto.Metric;
import io.wren.base.dto.Model;
import io.wren.base.type.PGArray;
import io.wren.base.type.PGType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.wren.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.wren.base.type.AnyType.ANY;
import static io.wren.base.type.BigIntType.BIGINT;
import static io.wren.base.type.BooleanType.BOOLEAN;
import static io.wren.base.type.BpCharType.BPCHAR;
import static io.wren.base.type.ByteaType.BYTEA;
import static io.wren.base.type.CharType.CHAR;
import static io.wren.base.type.DateType.DATE;
import static io.wren.base.type.DoubleType.DOUBLE;
import static io.wren.base.type.InetType.INET;
import static io.wren.base.type.IntegerType.INTEGER;
import static io.wren.base.type.JsonType.JSON;
import static io.wren.base.type.NumericType.NUMERIC;
import static io.wren.base.type.OidType.OID_INSTANCE;
import static io.wren.base.type.PGArray.BOOL_ARRAY;
import static io.wren.base.type.PGArray.BYTEA_ARRAY;
import static io.wren.base.type.PGArray.DATE_ARRAY;
import static io.wren.base.type.PGArray.FLOAT8_ARRAY;
import static io.wren.base.type.PGArray.INT8_ARRAY;
import static io.wren.base.type.PGArray.NUMERIC_ARRAY;
import static io.wren.base.type.PGArray.TIMESTAMP_ARRAY;
import static io.wren.base.type.PGArray.VARCHAR_ARRAY;
import static io.wren.base.type.PgTypeUtils.pgNameToType;
import static io.wren.base.type.RealType.REAL;
import static io.wren.base.type.RegprocType.REGPROC;
import static io.wren.base.type.SmallIntType.SMALLINT;
import static io.wren.base.type.TimestampType.TIMESTAMP;
import static io.wren.base.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIMEZONE;
import static io.wren.base.type.UuidType.UUID;
import static io.wren.base.type.VarcharType.NameType.NAME;
import static io.wren.base.type.VarcharType.TextType.TEXT;
import static io.wren.base.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public final class BigQueryUtils
{
    private static final Map<PGType<?>, String> pgTypeToBqType;
    private static final Map<String, PGType<?>> bqTypeToPgType;

    static {
        ImmutableMap.Builder<PGType<?>, String> builder = ImmutableMap.<PGType<?>, String>builder()
                .put(BOOLEAN, "BOOL")
                .put(SMALLINT, "SMALLINT")
                .put(INTEGER, "INTEGER")
                .put(BIGINT, "BIGINT")
                .put(REAL, "FLOAT64") // BigQuery only has FLOAT64 for floating point type
                .put(DOUBLE, "FLOAT64")
                .put(NUMERIC, "NUMERIC")
                .put(VARCHAR, "STRING")
                .put(CHAR, "STRING")
                .put(JSON, "JSON")
                .put(TIMESTAMP, "TIMESTAMP")
                .put(TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP")
                .put(TEXT, "STRING")
                .put(NAME, "STRING")
                .put(OID_INSTANCE, "INTEGER")
                .put(DATE, "DATE")
                .put(BYTEA, "BYTES")
                .put(BPCHAR, "STRING")
                .put(INET, "INET")
                .put(UUID, "STRING")
                .put(REGPROC, "INT64")
                .put(ANY, "ANY TYPE");
        // TODO: support record type, hstore
        // .put(EMPTY_RECORD, "STRUCT")
        // .put(HSTORE, "STRUCT")

        ImmutableMap.Builder<String, PGType<?>> bqTypeToPgTypeBuilder = ImmutableMap.<String, PGType<?>>builder()
                .put(StandardSQLTypeName.BOOL.name(), BOOLEAN)
                .put(array(StandardSQLTypeName.BOOL.name()), BOOL_ARRAY)
                .put(StandardSQLTypeName.BYTES.name(), BYTEA)
                .put(array(StandardSQLTypeName.BYTES.name()), BYTEA_ARRAY)
                .put(StandardSQLTypeName.FLOAT64.name(), DOUBLE)
                .put(array(StandardSQLTypeName.FLOAT64.name()), FLOAT8_ARRAY)
                .put(StandardSQLTypeName.INT64.name(), BIGINT)
                .put(array(StandardSQLTypeName.INT64.name()), INT8_ARRAY)
                .put(StandardSQLTypeName.STRING.name(), VARCHAR)
                .put(array(StandardSQLTypeName.STRING.name()), VARCHAR_ARRAY)
                .put(StandardSQLTypeName.DATE.name(), DATE)
                .put(array(StandardSQLTypeName.DATE.name()), DATE_ARRAY)
                .put(StandardSQLTypeName.NUMERIC.name(), NUMERIC)
                .put(array(StandardSQLTypeName.NUMERIC.name()), NUMERIC_ARRAY)
                .put(StandardSQLTypeName.TIMESTAMP.name(), TIMESTAMP)
                .put(array(StandardSQLTypeName.TIMESTAMP.name()), TIMESTAMP_ARRAY);

        Map<PGType<?>, String> simpleTypeMap = builder.build();

        for (PGArray pgArray : PGArray.allArray()) {
            String innerType = simpleTypeMap.get(pgArray.getInnerType());
            String bqArrayType = format("ARRAY<%s>", innerType);
            builder.put(pgArray, bqArrayType);
        }
        bqTypeToPgType = bqTypeToPgTypeBuilder.build();
        pgTypeToBqType = builder.build();
    }

    private static String array(String innerType)
    {
        return "ARRAY<" + innerType + ">";
    }

    private BigQueryUtils() {}

    public static String createOrReplaceAllTable(WrenMDL wrenMDL, String metadataSchema, String pgCatalogName)
    {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(format("CREATE OR REPLACE VIEW `%s.all_tables` AS ", metadataSchema))
                .append(format("SELECT table_catalog, 'pg_catalog' AS table_schema, table_name FROM `%s`.INFORMATION_SCHEMA.TABLES", pgCatalogName));
        if (!getWrenTable(wrenMDL).isEmpty()) {
            stringBuilder.append(" UNION ALL ")
                    .append("SELECT * FROM UNNEST([STRUCT<table_catalog STRING, table_schema STRING, table_name STRING> ")
                    .append(getWrenTable(wrenMDL).stream()
                            .map(tableName -> format("('%s', '%s', '%s')", wrenMDL.getCatalog(), wrenMDL.getSchema(), tableName))
                            .collect(joining(", ")))
                    .append("]);");
        }
        return stringBuilder.toString();
    }

    private static List<String> getWrenTable(WrenMDL wrenMDL)
    {
        List<String> wrenTables = new ArrayList<>();
        wrenTables.addAll(wrenMDL.listModels().stream().map(Model::getName).collect(toList()));
        wrenTables.addAll(wrenMDL.listMetrics().stream().map(Metric::getName).collect(toList()));
        // TODO add view https://github.com/Canner/wren/issues/334
//        wrenTables.addAll(wrenMDL.listViews().stream().map(View::getName).collect(Collectors.toList()));
        return wrenTables;
    }

    /**
     * all_columns should be created after pg_type_mapping created.
     */
    public static String createOrReplaceAllColumn(WrenMDL wrenMDL, String metadataSchema, String pgCatalogName)
    {
        // TODO: we should check if pg_type has created or not.
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(format("CREATE OR REPLACE VIEW `%s.all_columns` AS ", metadataSchema))
                .append(format("SELECT 'pg_catalog' as table_schema, col.table_name, col.column_name, col.ordinal_position, ptype.oid as typoid, ptype.typlen " +
                        "FROM `%s`.INFORMATION_SCHEMA.COLUMNS col " +
                        "LEFT JOIN `%s` mapping ON col.data_type = mapping.bq_type " +
                        "LEFT JOIN `%s.pg_type` ptype ON mapping.oid = ptype.oid", pgCatalogName, metadataSchema + ".pg_type_mapping", pgCatalogName));
        if (!getWrenTable(wrenMDL).isEmpty()) {
            stringBuilder.append(" UNION ALL ")
                    .append("SELECT * FROM UNNEST([STRUCT<table_schema STRING, table_name STRING, column_name STRING, ordinal_position int64, typoid integer, typlen integer> ")
                    .append(listColumnsRecords(wrenMDL))
                    .append("]);");
        }
        return stringBuilder.toString();
    }

    private static String listColumnsRecords(WrenMDL wrenMDL)
    {
        // TODO add view https://github.com/Canner/wren/issues/334
        List<String> records = new ArrayList<>();
        for (Model model : wrenMDL.listModels()) {
            List<Column> columns = model.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                Column col = columns.get(i);
                if (col.getRelationship().isEmpty()) {
                    Optional<PGType<?>> pgType = pgNameToType(col.getType());
                    if (pgType.isPresent()) {
                        records.add(format("('%s', '%s', '%s', %s, %s, %s)", wrenMDL.getSchema(), model.getName(), col.getName(), i + 1, pgType.get().oid(), pgType.get().typeLen()));
                    }
                }
            }
        }
        // TODO Add timegrain as column https://github.com/Canner/wren/issues/342
        for (Metric metric : wrenMDL.listMetrics()) {
            int i = 1;
            List<Column> columns = new ArrayList<>();
            columns.addAll(metric.getDimension());
            columns.addAll(metric.getMeasure());
            for (Column col : columns) {
                Optional<PGType<?>> pgType = pgNameToType(col.getType());
                if (pgType.isPresent()) {
                    records.add(format("('%s', '%s', '%s', %s, %s, %s)", wrenMDL.getSchema(), metric.getName(), col.getName(), i, pgType.get().oid(), pgType.get().typeLen()));
                    i = i + 1;
                }
            }
        }

        // TODO: support cumulative metric
        return String.join(", ", records);
    }

    public static String createOrReplacePgTypeMapping(String metadataSchema)
    {
        String columnDefinition = "bq_type string, oid int64";
        String records = getBqTypeToPgType().entrySet().stream()
                .map(entry -> format("('%s', %s)", entry.getKey(), entry.getValue().oid()))
                .reduce((a, b) -> a + "," + b)
                .orElseThrow(() -> new WrenException(GENERIC_INTERNAL_ERROR, "Build pg_type_mapping failed"));
        return buildPgCatalogTableView(metadataSchema, "pg_type_mapping", columnDefinition, records, false);
    }

    public static String buildPgCatalogTableView(String datasetName, String viewName, String columnDefinition, String records, boolean isEmpty)
    {
        return format("CREATE OR REPLACE VIEW `%s.%s` AS SELECT * FROM UNNEST([STRUCT<%s> %s])%s", datasetName, viewName, columnDefinition, records, isEmpty ? " LIMIT 0;" : ";");
    }

    public static Map<String, PGType<?>> getBqTypeToPgType()
    {
        return bqTypeToPgType;
    }

    public static String toBqType(PGType<?> pgType)
    {
        return pgTypeToBqType.get(pgType);
    }
}
