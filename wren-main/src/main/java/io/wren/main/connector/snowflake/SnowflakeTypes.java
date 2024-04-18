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

import com.google.common.collect.ImmutableMap;
import io.wren.base.WrenException;
import io.wren.base.type.BigIntType;
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
import java.util.Map;
import java.util.Optional;

import static io.wren.base.metadata.StandardErrorCode.NOT_SUPPORTED;

public class SnowflakeTypes
{
    public static final SnowflakeType BOOLEAN = new SnowflakeType(Types.BOOLEAN, "BOOLEAN");
    public static final SnowflakeType BIGINT = new SnowflakeType(Types.BIGINT, "BIGINT");
    public static final SnowflakeType BINARY = new SnowflakeType(Types.BINARY, "BINARY");
    public static final SnowflakeType DATE = new SnowflakeType(Types.DATE, "DATE");
    public static final SnowflakeType DOUBLE = new SnowflakeType(Types.DOUBLE, "DOUBLE");
    public static final SnowflakeType DECIMAL = new SnowflakeType(Types.DECIMAL, "DECIMAL");
    public static final SnowflakeType FLOAT = new SnowflakeType(Types.FLOAT, "FLOAT");
    public static final SnowflakeType SMALLINT = new SnowflakeType(Types.SMALLINT, "SMALLINT");
    public static final SnowflakeType INTEGER = new SnowflakeType(Types.INTEGER, "INTEGER");
    public static final SnowflakeType TINYINT = new SnowflakeType(Types.TINYINT, "TINYINT");
    public static final SnowflakeType TIMESTAMP = new SnowflakeType(Types.TIMESTAMP, "TIMESTAMP");
    public static final SnowflakeType TIMESTAMP_WITH_TIMEZONE = new SnowflakeType(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP WITH TIMEZONE");
    public static final SnowflakeType VARCHAR = new SnowflakeType(Types.VARCHAR, "VARCHAR");

    private static final Map<Integer, PGType<?>> sfTypeToPgTypeMap = ImmutableMap.<Integer, PGType<?>>builder()
            .put(BOOLEAN.getJdbcType(), BooleanType.BOOLEAN)
            .put(BIGINT.getJdbcType(), BigIntType.BIGINT)
            .put(BINARY.getJdbcType(), ByteaType.BYTEA)
            .put(DATE.getJdbcType(), DateType.DATE)
            .put(DOUBLE.getJdbcType(), DoubleType.DOUBLE)
            .put(DECIMAL.getJdbcType(), NumericType.NUMERIC)
            .put(FLOAT.getJdbcType(), RealType.REAL)
            .put(SMALLINT.getJdbcType(), SmallIntType.SMALLINT)
            .put(INTEGER.getJdbcType(), IntegerType.INTEGER)
            .put(TINYINT.getJdbcType(), TinyIntType.TINYINT)
            .put(TIMESTAMP.getJdbcType(), TimestampType.TIMESTAMP)
            .put(TIMESTAMP_WITH_TIMEZONE.getJdbcType(), TimestampWithTimeZoneType.TIMESTAMP_WITH_TIMEZONE)
            .put(VARCHAR.getJdbcType(), VarcharType.VARCHAR)
            .build();

    private static final Map<PGType<?>, String> pgTypeToSFTypeMap = ImmutableMap.<PGType<?>, String>builder()
            .put(BooleanType.BOOLEAN, BOOLEAN.getName())
            .put(BigIntType.BIGINT, BIGINT.getName())
            .put(ByteaType.BYTEA, BINARY.getName())
            .put(DateType.DATE, DATE.getName())
            .put(DoubleType.DOUBLE, DOUBLE.getName())
            .put(NumericType.NUMERIC, DECIMAL.getName())
            .put(RealType.REAL, FLOAT.getName())
            .put(SmallIntType.SMALLINT, SMALLINT.getName())
            .put(IntegerType.INTEGER, INTEGER.getName())
            .put(TinyIntType.TINYINT, TINYINT.getName())
            .put(TimestampType.TIMESTAMP, TIMESTAMP.getName())
            .put(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIMEZONE, TIMESTAMP_WITH_TIMEZONE.getName())
            .put(VarcharType.VARCHAR, VARCHAR.getName())
            .build();

    private SnowflakeTypes() {}

    public static PGType<?> toPGType(int type)
    {
        return Optional.ofNullable(sfTypeToPgTypeMap.get(type))
                .orElseThrow(() -> new WrenException(NOT_SUPPORTED, "Unsupported Type: " + type));
    }

    public static String toSFType(PGType<?> type)
    {
        return Optional.ofNullable(pgTypeToSFTypeMap.get(type))
                .orElseThrow(() -> new WrenException(NOT_SUPPORTED, "Unsupported Type: " + type));
    }
}
