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

import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.common.collect.ImmutableMap;
import io.cml.metadata.Metadata;
import io.cml.spi.type.PGArray;
import io.cml.spi.type.PGType;

import java.util.List;
import java.util.Map;

import static io.cml.pgcatalog.PgCatalogUtils.CML_TEMP_NAME;
import static io.cml.spi.type.BigIntType.BIGINT;
import static io.cml.spi.type.BooleanType.BOOLEAN;
import static io.cml.spi.type.BpCharType.BPCHAR;
import static io.cml.spi.type.ByteaType.BYTEA;
import static io.cml.spi.type.CharType.CHAR;
import static io.cml.spi.type.DateType.DATE;
import static io.cml.spi.type.DoubleType.DOUBLE;
import static io.cml.spi.type.InetType.INET;
import static io.cml.spi.type.IntegerType.INTEGER;
import static io.cml.spi.type.JsonType.JSON;
import static io.cml.spi.type.NumericType.NUMERIC;
import static io.cml.spi.type.OidType.OID_INSTANCE;
import static io.cml.spi.type.PGArray.BOOL_ARRAY;
import static io.cml.spi.type.PGArray.BYTEA_ARRAY;
import static io.cml.spi.type.PGArray.DATE_ARRAY;
import static io.cml.spi.type.PGArray.FLOAT8_ARRAY;
import static io.cml.spi.type.PGArray.INT8_ARRAY;
import static io.cml.spi.type.PGArray.NUMERIC_ARRAY;
import static io.cml.spi.type.PGArray.TIMESTAMP_ARRAY;
import static io.cml.spi.type.PGArray.VARCHAR_ARRAY;
import static io.cml.spi.type.RealType.REAL;
import static io.cml.spi.type.RegprocType.REGPROC;
import static io.cml.spi.type.SmallIntType.SMALLINT;
import static io.cml.spi.type.TimestampType.TIMESTAMP;
import static io.cml.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIMEZONE;
import static io.cml.spi.type.UuidType.UUID;
import static io.cml.spi.type.VarcharType.NameType.NAME;
import static io.cml.spi.type.VarcharType.TextType.TEXT;
import static io.cml.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

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
                .put(REGPROC, "STRING");
        // TODO: support record type, hstore
        // .put(EMPTY_RECORD, "STRUCT")
        // .put(HSTORE, "STRUCT")

        ImmutableMap.Builder<String, PGType<?>> bqTypeToOidBuilder = ImmutableMap.<String, PGType<?>>builder()
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
            String bqType = format("ARRAY<%s>", innerType);
            builder.put(pgArray, bqType);
        }
        bqTypeToPgType = bqTypeToOidBuilder.build();
        pgTypeToBqType = builder.build();
    }

    private static String array(String innerType)
    {
        return "ARRAY<" + innerType + ">";
    }

    private BigQueryUtils() {}

    public static String createOrReplaceAllTable(Metadata connector)
    {
        // TODO: handle schemas is empty. https://github.com/Canner/canner-metric-layer/issues/52
        List<String> schemas = connector.listSchemas();
        StringBuilder builder = new StringBuilder();
        builder.append(format("CREATE OR REPLACE VIEW `%s.all_tables` AS ", CML_TEMP_NAME));
        for (String schema : schemas) {
            builder.append(format("SELECT * FROM `%s`.INFORMATION_SCHEMA.TABLES UNION ALL ", schema));
        }
        builder.setLength(builder.length() - "UNION ALL ".length());
        builder.append(";");
        return builder.toString();
    }

    /**
     * all_columns should be created after pg_type_mapping created.
     */
    public static String createOrReplaceAllColumn(Metadata connector)
    {
        // TODO: handle schemas is empty. https://github.com/Canner/canner-metric-layer/issues/52
        // TODO: we should check if pg_type has created or not.
        List<String> schemas = connector.listSchemas();
        StringBuilder builder = new StringBuilder();
        builder.append(format("CREATE OR REPLACE VIEW `%s.all_columns` AS ", CML_TEMP_NAME));
        for (String schema : schemas) {
            builder.append(format("SELECT col.column_name, col.ordinal_position, col.table_name, ptype.oid as typoid, ptype.typlen " +
                    "FROM `%s`.INFORMATION_SCHEMA.COLUMNS col " +
                    "LEFT JOIN `%s` mapping ON col.data_type = mapping.bq_type " +
                    "LEFT JOIN `pg_catalog.pg_type` ptype ON mapping.oid = ptype.oid " +
                    "UNION ALL ", schema, CML_TEMP_NAME + ".pg_type_mapping"));
        }
        builder.setLength(builder.length() - "UNION ALL ".length());
        builder.append(";");
        return builder.toString();
    }

    public static String createPgTypeMapping()
    {
        String columnDefinition = "bq_type string, oid int64";
        StringBuilder records = new StringBuilder();
        for (Map.Entry<String, PGType<?>> entry : getBqTypeToPgType().entrySet()) {
            records.append(format("('%s', %s),", entry.getKey(), entry.getValue().oid()));
        }
        records.setLength(records.length() - 1);
        return buildPgCatalogTableView(CML_TEMP_NAME, "pg_type_mapping", columnDefinition, records.toString(), false);
    }

    public static String buildPgCatalogTableView(String datasetName, String viewName, String columnDefinition, String records, boolean isEmpty)
    {
        String viewDefinition = format("CREATE OR REPLACE VIEW `%s.%s` AS SELECT * FROM UNNEST([STRUCT<%s> %s])", datasetName, viewName, columnDefinition, records);
        return isEmpty ? viewDefinition + " LIMIT 0;" : viewDefinition + ";";
    }

    public static Map<PGType<?>, String> getPgTypeToBqType()
    {
        return pgTypeToBqType;
    }

    public static Map<String, PGType<?>> getBqTypeToPgType()
    {
        return bqTypeToPgType;
    }
}
