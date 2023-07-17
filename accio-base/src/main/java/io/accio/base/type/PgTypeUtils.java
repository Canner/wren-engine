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

package io.accio.base.type;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

import static io.accio.base.type.BigIntType.BIGINT;
import static io.accio.base.type.BooleanType.BOOLEAN;
import static io.accio.base.type.ByteaType.BYTEA;
import static io.accio.base.type.CharType.CHAR;
import static io.accio.base.type.DateType.DATE;
import static io.accio.base.type.DoubleType.DOUBLE;
import static io.accio.base.type.IntegerType.INTEGER;
import static io.accio.base.type.IntervalType.INTERVAL;
import static io.accio.base.type.JsonType.JSON;
import static io.accio.base.type.NumericType.NUMERIC;
import static io.accio.base.type.PGTypes.getArrayType;
import static io.accio.base.type.RealType.REAL;
import static io.accio.base.type.RecordType.EMPTY_RECORD;
import static io.accio.base.type.SmallIntType.SMALLINT;
import static io.accio.base.type.TimestampType.TIMESTAMP;
import static io.accio.base.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;

public final class PgTypeUtils
{
    private PgTypeUtils() {}

    private static final Map<String, PGType<?>> pgNameToTypeMap;

    static {
        ImmutableMap.Builder<String, PGType<?>> pgNameToTypeMapBuilder = ImmutableMap.<String, PGType<?>>builder();

        // typName
        Map<String, PGType<?>> typNameBuilder = ImmutableMap.<String, PGType<?>>builder()
                .put("bool", BOOLEAN)
                .put("int2", SMALLINT)
                .put("int4", INTEGER)
                .put("int8", BIGINT)
                .put("float4", REAL)
                .put("float8", DOUBLE)
                .put("numeric", NUMERIC)
                .put("varchar", VARCHAR)
                .put("char", CHAR)
                .put("date", DATE)
                .put("timestamp", TIMESTAMP)
                .put("json", JSON)
                .put("bytea", BYTEA)
                .put("record", EMPTY_RECORD)
                .put("interval", INTERVAL)
                .build();
        // TODO
//            .put("name", NAME)
//            .put("text", TEXT)
//            .put("inet", INET)
//            .put("uuid", UUID)
//            .put("timestamptz", TIMESTAMP_WITH_TIMEZONE)
//            .put("timestamp with time zone", TIMESTAMP_WITH_TIMEZONE)
        pgNameToTypeMapBuilder.putAll(typNameBuilder);

        // alias name
        Map<String, PGType<?>> aliasNameBuilder = ImmutableMap.<String, PGType<?>>builder()
                .put("boolean", BOOLEAN)
                .put("smallint", SMALLINT)
                .put("int", INTEGER)
                .put("integer", INTEGER)
                .put("bigint", BIGINT)
                .put("real", REAL)
                .put("decimal", NUMERIC)
                .put("double precision", DOUBLE)
                .put("character", CHAR)
                .put("character varying", VARCHAR)
                .build();
        pgNameToTypeMapBuilder.putAll(aliasNameBuilder);

        // array name
        typNameBuilder.forEach((name, type) -> {
            // TODO Support interval array
            if (type == INTERVAL) {
                return;
            }
            PGType<?> arrayType = getArrayType(type.oid());
            pgNameToTypeMapBuilder.put("_" + name, arrayType);
            pgNameToTypeMapBuilder.put(name + "[]", arrayType);
            pgNameToTypeMapBuilder.put(name + " array", arrayType);
        });

        aliasNameBuilder.forEach((name, type) -> {
            PGType<?> arrayType = getArrayType(type.oid());
            pgNameToTypeMapBuilder.put(name + "[]", arrayType);
            pgNameToTypeMapBuilder.put(name + " array", arrayType);
        });

        pgNameToTypeMap = pgNameToTypeMapBuilder.build();
    }

    public static Optional<PGType<?>> pgNameToType(String name)
    {
        String lowerCaseName = name.toLowerCase(ENGLISH);
        return pgNameToTypeMap.containsKey(lowerCaseName) ? Optional.of(pgNameToTypeMap.get(lowerCaseName)) : Optional.empty();
    }
}
