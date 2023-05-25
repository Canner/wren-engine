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
package io.graphmdl.base.client.duckdb;

import com.google.common.collect.ImmutableMap;
import io.graphmdl.base.GraphMDLException;
import io.graphmdl.base.type.PGType;

import java.sql.Types;
import java.util.Map;
import java.util.Optional;

import static io.graphmdl.base.metadata.StandardErrorCode.NOT_SUPPORTED;
import static io.graphmdl.base.type.BigIntType.BIGINT;
import static io.graphmdl.base.type.BooleanType.BOOLEAN;
import static io.graphmdl.base.type.ByteaType.BYTEA;
import static io.graphmdl.base.type.DateType.DATE;
import static io.graphmdl.base.type.DoubleType.DOUBLE;
import static io.graphmdl.base.type.IntegerType.INTEGER;
import static io.graphmdl.base.type.NumericType.NUMERIC;
import static io.graphmdl.base.type.RealType.REAL;
import static io.graphmdl.base.type.SmallIntType.SMALLINT;
import static io.graphmdl.base.type.TimestampType.TIMESTAMP;
import static io.graphmdl.base.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIMEZONE;
import static io.graphmdl.base.type.TinyIntType.TINYINT;
import static io.graphmdl.base.type.VarcharType.VARCHAR;

public final class DuckdbType
{
    public static final DuckdbType DUCKDB_TYPE = new DuckdbType();
    // other types LIST, ENUM, HUGEINT, UTINYINT, USMALLINT, STRUCT, UUID, JSON, UINTEGER, UBIGINT, INTERVAL, MAP
    private final Map<Integer, PGType<?>> duckdbTypeToPgTypeMap = ImmutableMap.<Integer, PGType<?>>builder()
            .put(Types.BOOLEAN, BOOLEAN)
            .put(Types.BLOB, BYTEA)
            .put(Types.TINYINT, TINYINT)
            .put(Types.SMALLINT, SMALLINT)
            .put(Types.INTEGER, INTEGER)
            .put(Types.BIGINT, BIGINT)
            .put(Types.FLOAT, REAL)
            .put(Types.DOUBLE, DOUBLE)
            .put(Types.DECIMAL, NUMERIC)
            .put(Types.VARCHAR, VARCHAR)
            .put(Types.DATE, DATE)
            .put(Types.TIMESTAMP, TIMESTAMP)
            .put(Types.TIMESTAMP_WITH_TIMEZONE, TIMESTAMP_WITH_TIMEZONE)
            .build();

    public PGType<?> toPGType(int type)
    {
        return Optional.ofNullable(duckdbTypeToPgTypeMap.get(type))
                .orElseThrow(() -> new GraphMDLException(NOT_SUPPORTED, "Unsupported Type: " + type));
    }

    private DuckdbType() {}
}
