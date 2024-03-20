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
package io.wren.base.client.duckdb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.wren.base.WrenException;
import io.wren.base.metadata.StandardErrorCode;
import io.wren.base.type.BigIntType;
import io.wren.base.type.BooleanType;
import io.wren.base.type.ByteaType;
import io.wren.base.type.DateType;
import io.wren.base.type.DoubleType;
import io.wren.base.type.IntegerType;
import io.wren.base.type.NumericType;
import io.wren.base.type.PGArray;
import io.wren.base.type.PGType;
import io.wren.base.type.RealType;
import io.wren.base.type.SmallIntType;
import io.wren.base.type.TimestampType;
import io.wren.base.type.TimestampWithTimeZoneType;
import io.wren.base.type.TinyIntType;
import io.wren.base.type.VarcharType;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;

public final class DuckdbTypes
{
    // other types LIST, ENUM, HUGEINT, UTINYINT, USMALLINT, STRUCT, UUID, JSON, UINTEGER, UBIGINT, INTERVAL, MAP
    public static final DuckdbType BOOELAN = new DuckdbType(Types.BOOLEAN, "BOOLEAN");
    public static final DuckdbType BIGINT = new DuckdbType(Types.BIGINT, "BIGINT");
    public static final DuckdbType BIT = new DuckdbType(Types.BIT, "BIT");
    public static final DuckdbType BLOB = new DuckdbType(Types.BLOB, "BLOB");
    public static final DuckdbType DATE = new DuckdbType(Types.DATE, "DATE");
    public static final DuckdbType DOUBLE = new DuckdbType(Types.DOUBLE, "DOUBLE");
    public static final DuckdbType REAL = new DuckdbType(Types.REAL, "REAL");
    public static final DuckdbType FLOAT = new DuckdbType(Types.FLOAT, "FLOAT");
    public static final DuckdbType DECIMAL = new DuckdbType(Types.DECIMAL, "DECIMAL");
    public static final DuckdbType INTEGER = new DuckdbType(Types.INTEGER, "INTEGER");
    public static final DuckdbType SMALLINT = new DuckdbType(Types.SMALLINT, "SMALLINT");
    public static final DuckdbType TINYINT = new DuckdbType(Types.TINYINT, "TINYINT");
    // TODO: check
    public static final DuckdbType INTERVAL = new DuckdbType(Types.OTHER, "INTERVAL");
    public static final DuckdbType TIME = new DuckdbType(Types.TIME, "TIME");
    public static final DuckdbType TIMESTAMP = new DuckdbType(Types.TIMESTAMP, "TIMESTAMP");
    public static final DuckdbType TIMESTAMP_WITH_TIMEZONE = new DuckdbType(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP WITH TIMEZONE");
    public static final DuckdbType VARCHAR = new DuckdbType(Types.VARCHAR, "VARCHAR");

    private static final List<DuckdbType> duckdbTypes = ImmutableList.of(
            BOOELAN,
            BIGINT,
            BIT,
            BLOB,
            DATE,
            DOUBLE,
            REAL,
            FLOAT,
            DECIMAL,
            INTEGER,
            SMALLINT,
            TINYINT,
            INTERVAL,
            TIME,
            TIMESTAMP,
            TIMESTAMP_WITH_TIMEZONE,
            VARCHAR);

    private static final Map<Integer, PGType<?>> duckdbTypeToPgTypeMap = ImmutableMap.<Integer, PGType<?>>builder()
            .put(BOOELAN.getJdbcType(), BooleanType.BOOLEAN)
            .put(BLOB.getJdbcType(), ByteaType.BYTEA)
            .put(TINYINT.getJdbcType(), TinyIntType.TINYINT)
            .put(SMALLINT.getJdbcType(), SmallIntType.SMALLINT)
            .put(INTEGER.getJdbcType(), IntegerType.INTEGER)
            .put(BIGINT.getJdbcType(), BigIntType.BIGINT)
            .put(FLOAT.getJdbcType(), RealType.REAL)
            .put(DOUBLE.getJdbcType(), DoubleType.DOUBLE)
            .put(DECIMAL.getJdbcType(), NumericType.NUMERIC)
            .put(VARCHAR.getJdbcType(), VarcharType.VARCHAR)
            .put(DATE.getJdbcType(), DateType.DATE)
            .put(TIMESTAMP.getJdbcType(), TimestampType.TIMESTAMP)
            .put(TIMESTAMP_WITH_TIMEZONE.getJdbcType(), TimestampWithTimeZoneType.TIMESTAMP_WITH_TIMEZONE)
            .put(Types.ARRAY, PGArray.VARCHAR_ARRAY)
            .build();

    private static final Map<PGType<?>, DuckdbType> pgTypeToDuckdbTypeMap = ImmutableMap.<PGType<?>, DuckdbType>builder()
            .put(BooleanType.BOOLEAN, BOOELAN)
            .put(ByteaType.BYTEA, BLOB)
            .put(TinyIntType.TINYINT, TINYINT)
            .put(SmallIntType.SMALLINT, SMALLINT)
            .put(IntegerType.INTEGER, INTEGER)
            .put(BigIntType.BIGINT, BIGINT)
            .put(RealType.REAL, FLOAT)
            .put(DoubleType.DOUBLE, DOUBLE)
            .put(NumericType.NUMERIC, DECIMAL)
            .put(VarcharType.VARCHAR, VARCHAR)
            .put(DateType.DATE, DATE)
            .put(TimestampType.TIMESTAMP, TIMESTAMP)
            .put(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIMEZONE, TIMESTAMP_WITH_TIMEZONE)
            .build();

    public static PGType<?> toPGType(int type)
    {
        return Optional.ofNullable(duckdbTypeToPgTypeMap.get(type))
                .orElseThrow(() -> new WrenException(StandardErrorCode.NOT_SUPPORTED, "Unsupported Type: " + type));
    }

    public static DuckdbType toDuckdbType(PGType<?> type)
    {
        return Optional.ofNullable(pgTypeToDuckdbTypeMap.get(type))
                .orElseThrow(() -> new WrenException(StandardErrorCode.NOT_SUPPORTED, "Unsupported Type: " + type));
    }

    public static List<String> getDuckDBTypeNames()
    {
        return duckdbTypes.stream().map(DuckdbType::getName).collect(toImmutableList());
    }

    private DuckdbTypes() {}
}
