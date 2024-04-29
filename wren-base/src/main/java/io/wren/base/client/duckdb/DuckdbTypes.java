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
import io.wren.base.type.BigIntType;
import io.wren.base.type.BooleanType;
import io.wren.base.type.ByteaType;
import io.wren.base.type.CharType;
import io.wren.base.type.DateType;
import io.wren.base.type.DoubleType;
import io.wren.base.type.IntegerType;
import io.wren.base.type.IntervalType;
import io.wren.base.type.JsonType;
import io.wren.base.type.NumericType;
import io.wren.base.type.PGType;
import io.wren.base.type.PGTypes;
import io.wren.base.type.RealType;
import io.wren.base.type.SmallIntType;
import io.wren.base.type.TimestampType;
import io.wren.base.type.TimestampWithTimeZoneType;
import io.wren.base.type.TinyIntType;
import io.wren.base.type.VarcharType;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.wren.base.metadata.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Locale.ENGLISH;

public final class DuckdbTypes
{
    // other types LIST, ENUM, UTINYINT, USMALLINT, STRUCT, UUID, JSON, UINTEGER, UBIGINT, INTERVAL, MAP
    public static final DuckdbType BOOLEAN = new DuckdbType(Types.BOOLEAN, "BOOLEAN");
    public static final DuckdbType BIGINT = new DuckdbType(Types.BIGINT, "BIGINT");
    public static final DuckdbType HUGEINT = new DuckdbType(Types.DECIMAL, "HUGEINT");
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
    public static final DuckdbType JSON = new DuckdbType(Types.STRUCT, "JSON");

    private static final Map<String, DuckdbType> duckdbTypes = ImmutableMap.<String, DuckdbType>builder()
            .put(BOOLEAN.getName(), BOOLEAN)
            .put(BIGINT.getName(), BIGINT)
            .put(BIT.getName(), BIT)
            .put(BLOB.getName(), BLOB)
            .put(DATE.getName(), DATE)
            .put(DOUBLE.getName(), DOUBLE)
            .put(REAL.getName(), REAL)
            .put(FLOAT.getName(), FLOAT)
            .put(DECIMAL.getName(), DECIMAL)
            .put(INTEGER.getName(), INTEGER)
            .put(SMALLINT.getName(), SMALLINT)
            .put(TINYINT.getName(), TINYINT)
            .put(INTERVAL.getName(), INTERVAL)
            .put(TIME.getName(), TIME)
            .put(TIMESTAMP.getName(), TIMESTAMP)
            .put(TIMESTAMP_WITH_TIMEZONE.getName(), TIMESTAMP_WITH_TIMEZONE)
            .put(VARCHAR.getName(), VARCHAR)
            .put(JSON.getName(), JSON)
            .put(HUGEINT.getName(), HUGEINT)
            .build();

    private static final Map<Integer, PGType<?>> duckdbTypeToPgTypeMap = ImmutableMap.<Integer, PGType<?>>builder()
            .put(BOOLEAN.getJdbcType(), BooleanType.BOOLEAN)
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
            .put(JSON.getJdbcType(), JsonType.JSON)
            .build();

    private static final Map<PGType<?>, DuckdbType> pgTypeToDuckdbTypeMap = ImmutableMap.<PGType<?>, DuckdbType>builder()
            .put(BooleanType.BOOLEAN, BOOLEAN)
            .put(ByteaType.BYTEA, BLOB)
            .put(TinyIntType.TINYINT, TINYINT)
            .put(SmallIntType.SMALLINT, SMALLINT)
            .put(IntegerType.INTEGER, INTEGER)
            .put(BigIntType.BIGINT, BIGINT)
            .put(RealType.REAL, FLOAT)
            .put(DoubleType.DOUBLE, DOUBLE)
            .put(NumericType.NUMERIC, DECIMAL)
            .put(VarcharType.VARCHAR, VARCHAR)
            .put(CharType.CHAR, VARCHAR)
            .put(DateType.DATE, DATE)
            .put(TimestampType.TIMESTAMP, TIMESTAMP)
            .put(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIMEZONE, TIMESTAMP_WITH_TIMEZONE)
            .put(IntervalType.INTERVAL, INTERVAL)
            .put(JsonType.JSON, JSON)
            .build();

    /**
     * getColumnType only return LIST without inner type if the type is INT[]
     * But getColumnTypeName will return `INT[]`
     */
    public static PGType<?> toPGType(ResultSetMetaData metaData, int i)
            throws SQLException
    {
        try {
            return DuckdbTypes.toPGType(metaData.getColumnType(i));
        }
        catch (WrenException e) {
            return DuckdbTypes.toPGType(metaData.getColumnTypeName(i));
        }
    }

    public static PGType<?> toPGType(String typeName)
    {
        Optional<? extends PGType<?>> pgType = getDuckDBType(getEqualTypeName(typeName))
                .map(DuckdbType::getJdbcType)
                .map(duckdbTypeToPgTypeMap::get);

        if (typeName.endsWith("[]")) {
            pgType = pgType
                    .map(PGType::oid)
                    .map(PGTypes::getArrayType);
        }

        return pgType.orElseThrow(() -> new WrenException(NOT_SUPPORTED, "Unsupported Type: " + typeName));
    }

    private static String getEqualTypeName(String typeName)
    {
        if (typeName.toLowerCase(ENGLISH).startsWith("decimal")) {
            return DECIMAL.toString();
        }
        if (typeName.toLowerCase(ENGLISH).startsWith("timestamp")) {
            return TIMESTAMP.toString();
        }
        if (typeName.endsWith("[]")) {
            return typeName.substring(0, typeName.length() - 2);
        }
        // TODO: MAP(INT, VARCHAR)
        // TODO: STRUCT(i INT, j VARCHAR)
        // TODO: UNION(num INT, text VARCHAR)
        return typeName;
    }

    public static PGType<?> toPGType(int type)
    {
        return Optional.ofNullable(duckdbTypeToPgTypeMap.get(type))
                .orElseThrow(() -> new WrenException(NOT_SUPPORTED, "Unsupported Type: " + type));
    }

    public static DuckdbType toDuckdbType(PGType<?> type)
    {
        return Optional.ofNullable(pgTypeToDuckdbTypeMap.get(type))
                .orElseThrow(() -> new WrenException(NOT_SUPPORTED, "Unsupported Type: " + type));
    }

    public static List<String> getDuckDBTypeNames()
    {
        return ImmutableList.copyOf(duckdbTypes.keySet());
    }

    public static Optional<DuckdbType> getDuckDBType(String name)
    {
        return Optional.ofNullable(duckdbTypes.get(name));
    }

    private DuckdbTypes() {}
}
