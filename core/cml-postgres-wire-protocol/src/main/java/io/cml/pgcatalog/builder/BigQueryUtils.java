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
import io.cml.spi.connector.Connector;
import io.cml.type.PGArray;

import java.util.List;
import java.util.Map;

import static io.cml.pgcatalog.PgCatalogUtils.CML_TEMP_NAME;
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

public final class BigQueryUtils
{
    private static final Map<Integer, String> oidToBqType;

    static {
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
        oidToBqType = builder.build();
    }

    private BigQueryUtils() {}

    public static String createOrReplaceAllTable(Connector connector)
    {
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

    public static String createOrReplaceAllColumn(Connector connector)
    {
        // TODO: we should check if pg_type has created or not.
        List<String> schemas = connector.listSchemas();
        StringBuilder builder = new StringBuilder();
        builder.append(format("CREATE OR REPLACE VIEW `%s.all_columns` AS ", CML_TEMP_NAME));
        for (String schema : schemas) {
            builder.append(format("SELECT col.column_name, col.ordinal_position, ptype.oid as typoid, ptype.typlen " +
                    "FROM `%s`.INFORMATION_SCHEMA.COLUMNS col, `pg_catalog.pg_type` ptype " +
                    "WHERE col.data_type = ptype.remotetype UNION ALL ", schema));
        }
        builder.setLength(builder.length() - "UNION ALL ".length());
        builder.append(";");
        return builder.toString();
    }

    public static Map<Integer, String> getOidToBqType()
    {
        return oidToBqType;
    }
}
