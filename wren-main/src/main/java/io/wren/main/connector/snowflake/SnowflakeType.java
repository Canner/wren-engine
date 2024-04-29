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

package io.wren.main.connector.snowflake;

import io.wren.base.WrenException;
import io.wren.base.type.BooleanType;
import io.wren.base.type.ByteaType;
import io.wren.base.type.DateType;
import io.wren.base.type.DoubleType;
import io.wren.base.type.IntegerType;
import io.wren.base.type.NumericType;
import io.wren.base.type.PGType;
import io.wren.base.type.RealType;
import io.wren.base.type.SmallIntType;
import io.wren.base.type.TimestampType;
import io.wren.base.type.TimestampWithTimeZoneType;
import io.wren.base.type.TinyIntType;
import io.wren.base.type.VarcharType;

import java.sql.Types;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.wren.base.metadata.StandardErrorCode.NOT_SUPPORTED;

public enum SnowflakeType
{
    BOOLEAN(Types.BOOLEAN, BooleanType.BOOLEAN),
    BIGINT(Types.BIGINT, NumericType.NUMERIC),
    BINARY(Types.BINARY, ByteaType.BYTEA),
    DATE(Types.DATE, DateType.DATE),
    DOUBLE(Types.DOUBLE, DoubleType.DOUBLE),
    DECIMAL(Types.DECIMAL, NumericType.NUMERIC),
    FLOAT(Types.FLOAT, RealType.REAL),
    SMALLINT(Types.SMALLINT, SmallIntType.SMALLINT),
    INTEGER(Types.INTEGER, IntegerType.INTEGER),
    TINYINT(Types.TINYINT, TinyIntType.TINYINT),
    TIMESTAMP(Types.TIMESTAMP, TimestampType.TIMESTAMP),
    TIMESTAMP_WITH_TIMEZONE(Types.TIMESTAMP_WITH_TIMEZONE, TimestampWithTimeZoneType.TIMESTAMP_WITH_TIMEZONE),
    VARCHAR(Types.VARCHAR, VarcharType.VARCHAR);

    private final int jdbcType;
    private final PGType<?> pgType;

    SnowflakeType(int jdbcType, PGType<?> pgType)
    {
        this.jdbcType = jdbcType;
        this.pgType = pgType;
    }

    int getJdbcType()
    {
        return jdbcType;
    }

    PGType<?> getPGType()
    {
        return pgType;
    }

    private static final Map<Integer, PGType<?>> jdbcToPgTypeMap = buildJdbcToPGTypeMap();
    private static final Map<PGType<?>, String> pgTypeToSFTypeMap = buildPGToSFTypeMap();

    public static PGType<?> toPGType(int type)
    {
        return Optional.ofNullable(jdbcToPgTypeMap.get(type))
                .orElseThrow(() -> new WrenException(NOT_SUPPORTED, "Unsupported Type: " + type));
    }

    public static String toSFType(PGType<?> type)
    {
        return Optional.ofNullable(pgTypeToSFTypeMap.get(type))
                .orElseThrow(() -> new WrenException(NOT_SUPPORTED, "Unsupported Type: " + type));
    }

    private static Map<Integer, PGType<?>> buildJdbcToPGTypeMap()
    {
        return Arrays.stream(SnowflakeType.values())
                .collect(toImmutableMap(SnowflakeType::getJdbcType, SnowflakeType::getPGType));
    }

    private static Map<PGType<?>, String> buildPGToSFTypeMap()
    {
        return Arrays.stream(SnowflakeType.values())
                .filter(type -> type != DECIMAL)
                .collect(toImmutableMap(SnowflakeType::getPGType, Enum::name));
    }
}
