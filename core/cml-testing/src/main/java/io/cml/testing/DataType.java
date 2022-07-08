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

package io.cml.testing;

import io.cml.spi.type.BigIntType;
import io.cml.spi.type.BooleanType;
import io.cml.spi.type.ByteaType;
import io.cml.spi.type.DoubleType;
import io.cml.spi.type.IntegerType;
import io.cml.spi.type.NumericType;
import io.cml.spi.type.PGType;
import io.cml.spi.type.RealType;
import io.cml.spi.type.SmallIntType;
import io.cml.spi.type.TinyIntType;
import io.cml.spi.type.VarcharType;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.io.BaseEncoding.base16;
import static io.cml.spi.type.DateType.DATE;
import static java.lang.String.format;

public class DataType<T>
{
    private final String insertType;
    private final PGType<?> pgResultType;
    private final Function<T, String> toLiteral;
    private final Function<T, String> toCmlLiteral;

    public static DataType<Boolean> booleanDataType()
    {
        return dataType("boolean", BooleanType.BOOLEAN);
    }

    public static DataType<Long> bigintDataType()
    {
        return dataType("bigint", BigIntType.BIGINT);
    }

    public static DataType<Integer> integerDataType()
    {
        return dataType("integer", IntegerType.INTEGER);
    }

    public static DataType<Short> smallintDataType()
    {
        return dataType("smallint", SmallIntType.SMALLINT);
    }

    public static DataType<Byte> tinyintDataType()
    {
        return dataType("tinyint", TinyIntType.TINYINT);
    }

    public static DataType<Float> realDataType()
    {
        return dataType("real", RealType.REAL,
                value -> {
                    if (Float.isFinite(value)) {
                        return value.toString();
                    }
                    if (Float.isNaN(value)) {
                        return "nan()";
                    }
                    return format("%sinfinity()", value > 0 ? "+" : "-");
                });
    }

    public static DataType<Double> doubleDataType()
    {
        return dataType("double", DoubleType.DOUBLE,
                value -> {
                    if (Double.isFinite(value)) {
                        return value.toString();
                    }
                    if (Double.isNaN(value)) {
                        return "nan()";
                    }
                    return format("%sinfinity()", value > 0 ? "+" : "-");
                });
    }

    public static DataType<byte[]> byteaDataType()
    {
        return dataType("bytea", ByteaType.BYTEA, DataType::binaryLiteral);
    }

    public static DataType<String> varcharDataType(int size)
    {
        return varcharDataType(size, "");
    }

    public static DataType<String> varcharDataType(int size, String properties)
    {
        return varcharDataType(Optional.of(size), properties);
    }

    public static DataType<String> varcharDataType()
    {
        return varcharDataType(Optional.empty(), "");
    }

    private static DataType<String> varcharDataType(Optional<Integer> length, String properties)
    {
        String prefix = length.map(size -> "varchar(" + size + ")").orElse("varchar");
        String suffix = properties.isEmpty() ? "" : " " + properties;
        return stringDataType(prefix + suffix, VarcharType.VARCHAR);
    }

    public static DataType<String> stringDataType(String insertType, PGType<?> trinoResultType)
    {
        return dataType(insertType, trinoResultType, DataType::formatStringLiteral);
    }

    public static DataType<BigDecimal> decimalDataType(int precision, int scale)
    {
        String databaseType = format("decimal(%s, %s)", precision, scale);
        return dataType(
                databaseType,
                NumericType.NUMERIC);
    }

    public static DataType<LocalDate> dateDataType()
    {
        return dataType(
                "date",
                DATE,
                DateTimeFormatter.ofPattern("'DATE '''uuuu-MM-dd''")::format);
    }

    public static String formatStringLiteral(String value)
    {
        return "'" + value.replace("'", "''") + "'";
    }

    /**
     * Formats bytes using SQL standard format for binary string literal
     */
    public static String binaryLiteral(byte[] value)
    {
        return "X'" + base16().encode(value) + "'";
    }

    private static <T> DataType<T> dataType(String insertType, PGType<?> cmlResultType)
    {
        return new DataType<>(insertType, cmlResultType, Object::toString, Object::toString);
    }

    public static <T> DataType<T> dataType(String insertType, PGType<?> cmlResultType, Function<T, String> toLiteral)
    {
        return new DataType<>(insertType, cmlResultType, toLiteral, toLiteral);
    }

    private DataType(String insertType, PGType<?> pgResultType, Function<T, String> toLiteral, Function<T, String> toCmlLiteral)
    {
        this.insertType = insertType;
        this.pgResultType = pgResultType;
        this.toLiteral = toLiteral;
        this.toCmlLiteral = toCmlLiteral;
    }

    public String toLiteral(T inputValue)
    {
        if (inputValue == null) {
            return "NULL";
        }
        return toLiteral.apply(inputValue);
    }

    public String toCmlLiteral(T inputValue)
    {
        if (inputValue == null) {
            return "NULL";
        }
        return toCmlLiteral.apply(inputValue);
    }

    public String getInsertType()
    {
        return insertType;
    }

    public PGType<?> getPgResultType()
    {
        return pgResultType;
    }
}
