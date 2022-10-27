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

package io.cml.connector.jdbc;

import com.google.common.collect.ImmutableMap;
import io.cml.spi.CmlException;
import io.cml.spi.type.BigIntType;
import io.cml.spi.type.BooleanType;
import io.cml.spi.type.DateType;
import io.cml.spi.type.DoubleType;
import io.cml.spi.type.IntegerType;
import io.cml.spi.type.PGType;
import io.cml.spi.type.RealType;
import io.cml.spi.type.SmallIntType;
import io.cml.spi.type.TimestampType;
import io.cml.spi.type.TinyIntType;
import io.cml.spi.type.VarcharType;

import java.sql.Types;
import java.util.Map;
import java.util.Optional;

import static io.cml.spi.metadata.StandardErrorCode.NOT_SUPPORTED;
import static io.cml.spi.type.ByteaType.BYTEA;

public class JdbcType
{
    private static final Map<Integer, PGType<?>> jdbcTypeToPgTypeMap;
    private static final Map<PGType<?>, Integer> pgTypeToJdbcTypeMap;

    private JdbcType() {}

    static {
        jdbcTypeToPgTypeMap = ImmutableMap.<Integer, PGType<?>>builder()
                .put(Types.BOOLEAN, BooleanType.BOOLEAN)
                .put(Types.BIGINT, BigIntType.BIGINT)
                .put(Types.INTEGER, IntegerType.INTEGER)
                .put(Types.SMALLINT, SmallIntType.SMALLINT)
                .put(Types.TINYINT, TinyIntType.TINYINT)
                .put(Types.VARCHAR, VarcharType.VARCHAR)
                .put(Types.DOUBLE, DoubleType.DOUBLE)
                .put(Types.DATE, DateType.DATE)
                .put(Types.VARBINARY, BYTEA)
                .put(Types.TIMESTAMP, TimestampType.TIMESTAMP)
                .build();

        pgTypeToJdbcTypeMap = ImmutableMap.<PGType<?>, Integer>builder()
                .put(BooleanType.BOOLEAN, Types.BOOLEAN)
                .put(BigIntType.BIGINT, Types.BIGINT)
                .put(IntegerType.INTEGER, Types.INTEGER)
                .put(SmallIntType.SMALLINT, Types.SMALLINT)
                .put(TinyIntType.TINYINT, Types.TINYINT)
                .put(VarcharType.VARCHAR, Types.VARCHAR)
                .put(DoubleType.DOUBLE, Types.DOUBLE)
                .put(RealType.REAL, Types.REAL)
                .put(DateType.DATE, Types.DATE)
                .put(BYTEA, Types.VARBINARY)
                .put(TimestampType.TIMESTAMP, Types.TIMESTAMP)
                .build();
    }

    public static PGType<?> toPGType(Integer jdbcType)
    {
        return Optional.ofNullable(jdbcTypeToPgTypeMap.get(jdbcType))
                .orElseThrow(() -> new CmlException(NOT_SUPPORTED, "Unsupported Type: " + jdbcType));
    }

    public static Integer toJdbcType(PGType<?> pgType)
    {
        return Optional.ofNullable(pgTypeToJdbcTypeMap.get(pgType))
                .orElseThrow(() -> new CmlException(NOT_SUPPORTED, "Unsupported Type: " + pgType.typName()));
    }
}
