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

package io.cml.connector.canner;

import com.google.common.collect.ImmutableMap;
import io.cml.spi.CmlException;
import io.cml.spi.type.BigIntType;
import io.cml.spi.type.BooleanType;
import io.cml.spi.type.DateType;
import io.cml.spi.type.DoubleType;
import io.cml.spi.type.IntegerType;
import io.cml.spi.type.PGType;
import io.cml.spi.type.PGTypes;
import io.cml.spi.type.RealType;
import io.cml.spi.type.SmallIntType;
import io.cml.spi.type.TimestampType;
import io.cml.spi.type.TinyIntType;
import io.cml.spi.type.VarcharType;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.cml.spi.metadata.StandardErrorCode.NOT_SUPPORTED;
import static io.cml.spi.type.ByteaType.BYTEA;
import static io.cml.spi.type.StandardTypes.TINYINT;

public final class CannerType
{
    private static final String BOOLEAN = "boolean";
    private static final String SMALLINT = "smallint";
    private static final String INTEGER = "integer";
    private static final String BIGINT = "bigint";
    private static final String REAL = "real";
    private static final String DOUBLE = "double";
    private static final String DECIMAL = "decimal";
    private static final String VARCHAR = "varchar";
    private static final String CHAR = "char";
    private static final String VARBINARY = "varbinary";
    private static final String JSON = "json";
    private static final String UUID = "uuid";
    private static final String DATE = "date";
    private static final String TIME = "time";
    private static final String TIME_WITH_TIME_ZONE = "time_with_time_zone";
    private static final String TIMESTAMP = "timestamp";
    private static final String TIMESTAMP_WITH_TIMEZONE = "timestamp_with_timezone";
    private static final String ARRAY = "array";
    private static final String IPADDRESS = "ipaddress";
    private static final String OBJECT_ID = "object_id";
    private static final String MAP = "map";
    private static final String ROW = "row";
    private static final String UNKNOWN = "unknown";
    private static final String REGPROC = "regproc";
    private static final String REGCLASS = "regclass";
    private static final String REGTYPE = "regtype";
    private static final String REGNAMESPACE = "regnamespace";
    private static final String REGOPERATOR = "regoperator";
    private static final String REGOPER = "regoper";
    private static final String REGPROCEDURE = "regprocedure";

    private CannerType() {}

    private static final Pattern TYPE_ARGUMENT_PATTERN = Pattern.compile("(?<typeName>[a-zA-Z_]+)(\\((?<argument>[0-9,A-Za-z_]+)\\))?");
    private static final Map<String, PGType<?>> cannerTypeToPgTypeMap;
    private static final Map<PGType<?>, String> pgTypeToCannerTypeMap;

    static {
        cannerTypeToPgTypeMap = ImmutableMap.<String, PGType<?>>builder()
                .put(BOOLEAN, BooleanType.BOOLEAN)
                .put(BIGINT, BigIntType.BIGINT)
                .put(INTEGER, IntegerType.INTEGER)
                .put(SMALLINT, SmallIntType.SMALLINT)
                .put(TINYINT, TinyIntType.TINYINT)
                .put(VARCHAR, VarcharType.VARCHAR)
                .put(DOUBLE, DoubleType.DOUBLE)
                .put(DATE, DateType.DATE)
                .put(VARBINARY, BYTEA)
                .put(TIMESTAMP, TimestampType.TIMESTAMP)
                .build();

        pgTypeToCannerTypeMap = ImmutableMap.<PGType<?>, String>builder()
                .put(BooleanType.BOOLEAN, BOOLEAN)
                .put(BigIntType.BIGINT, BIGINT)
                .put(IntegerType.INTEGER, INTEGER)
                .put(SmallIntType.SMALLINT, SMALLINT)
                .put(TinyIntType.TINYINT, TINYINT)
                .put(VarcharType.VARCHAR, VARCHAR)
                .put(DoubleType.DOUBLE, DOUBLE)
                .put(RealType.REAL, REAL)
                .put(DateType.DATE, DATE)
                .put(BYTEA, VARBINARY)
                .put(TimestampType.TIMESTAMP, TIMESTAMP)
                .build();
    }

    public static PGType<?> toPGType(String cannerType)
    {
        // remove type argument
        Matcher matcher = TYPE_ARGUMENT_PATTERN.matcher(cannerType);
        if (matcher.find()) {
            if (cannerType.startsWith("array")) {
                return Optional.of(PGTypes.oidToPgType(cannerTypeToPgTypeMap.get(matcher.group("argument")).typArray()))
                        .orElseThrow(() -> new CmlException(NOT_SUPPORTED, "Unsupported Type: " + cannerType));
            }

            return Optional.ofNullable(cannerTypeToPgTypeMap.get(matcher.group("typeName")))
                    .orElseThrow(() -> new CmlException(NOT_SUPPORTED, "Unsupported Type: " + cannerType));
        }
        throw new CmlException(NOT_SUPPORTED, "Unsupported Type: " + cannerType);
    }

    public static String toCannerType(PGType<?> pgType)
    {
        return Optional.ofNullable(pgTypeToCannerTypeMap.get(pgType))
                .orElseThrow(() -> new CmlException(NOT_SUPPORTED, "Unsupported Type: " + pgType.typName()));
    }
}
