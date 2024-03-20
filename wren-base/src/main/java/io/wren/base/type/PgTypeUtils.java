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

package io.wren.base.type;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

import static io.wren.base.type.BooleanType.BOOLEAN;
import static io.wren.base.type.ByteaType.BYTEA;
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
                .put("int2", SmallIntType.SMALLINT)
                .put("int4", IntegerType.INTEGER)
                .put("int8", BigIntType.BIGINT)
                .put("float4", RealType.REAL)
                .put("float8", DoubleType.DOUBLE)
                .put("numeric", NumericType.NUMERIC)
                .put("varchar", VarcharType.VARCHAR)
                .put("char", CharType.CHAR)
                .put("date", DateType.DATE)
                .put("timestamp", TimestampType.TIMESTAMP)
                .put("json", JsonType.JSON)
                .put("bytea", BYTEA)
                .put("record", RecordType.EMPTY_RECORD)
                .put("interval", IntervalType.INTERVAL)
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
                .put("smallint", SmallIntType.SMALLINT)
                .put("int", IntegerType.INTEGER)
                .put("integer", IntegerType.INTEGER)
                .put("bigint", BigIntType.BIGINT)
                .put("real", RealType.REAL)
                .put("decimal", NumericType.NUMERIC)
                .put("double precision", DoubleType.DOUBLE)
                .put("character", CharType.CHAR)
                .put("character varying", VarcharType.VARCHAR)
                .build();
        pgNameToTypeMapBuilder.putAll(aliasNameBuilder);

        // array name
        typNameBuilder.forEach((name, type) -> {
            // TODO Support interval array
            if (type == IntervalType.INTERVAL) {
                return;
            }
            PGType<?> arrayType = PGTypes.getArrayType(type.oid());
            pgNameToTypeMapBuilder.put("_" + name, arrayType);
            pgNameToTypeMapBuilder.put(name + "[]", arrayType);
            pgNameToTypeMapBuilder.put(name + " array", arrayType);
        });

        aliasNameBuilder.forEach((name, type) -> {
            PGType<?> arrayType = PGTypes.getArrayType(type.oid());
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
