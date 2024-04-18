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

import com.google.common.collect.ImmutableMap;
import io.wren.base.type.PGArray;
import io.wren.base.type.PGType;

import java.util.Map;

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

public final class BigQueryUtils
{
    private static final Map<PGType<?>, String> pgTypeToBqType;

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

        Map<PGType<?>, String> simpleTypeMap = builder.build();

        for (PGArray pgArray : PGArray.allArray()) {
            String innerType = simpleTypeMap.get(pgArray.getInnerType());
            String bqArrayType = format("ARRAY<%s>", innerType);
            builder.put(pgArray, bqArrayType);
        }
        pgTypeToBqType = builder.build();
    }

    private BigQueryUtils() {}

    public static String toBqType(PGType<?> pgType)
    {
        return pgTypeToBqType.get(pgType);
    }
}
