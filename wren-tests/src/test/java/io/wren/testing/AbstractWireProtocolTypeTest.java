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

package io.wren.testing;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.wren.base.type.PGArray;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.wren.base.type.PGArray.BOOL_ARRAY;
import static io.wren.base.type.PGArray.BYTEA_ARRAY;
import static io.wren.base.type.PGArray.CHAR_ARRAY;
import static io.wren.base.type.PGArray.DATE_ARRAY;
import static io.wren.base.type.PGArray.FLOAT4_ARRAY;
import static io.wren.base.type.PGArray.FLOAT8_ARRAY;
import static io.wren.base.type.PGArray.INT2_ARRAY;
import static io.wren.base.type.PGArray.INT4_ARRAY;
import static io.wren.base.type.PGArray.INT8_ARRAY;
import static io.wren.base.type.PGArray.JSON_ARRAY;
import static io.wren.base.type.PGArray.NUMERIC_ARRAY;
import static io.wren.base.type.PGArray.TIMESTAMP_ARRAY;
import static io.wren.base.type.PGArray.VARCHAR_ARRAY;
import static io.wren.testing.DataType.bigintDataType;
import static io.wren.testing.DataType.booleanDataType;
import static io.wren.testing.DataType.byteaDataType;
import static io.wren.testing.DataType.charDataType;
import static io.wren.testing.DataType.dataType;
import static io.wren.testing.DataType.dateDataType;
import static io.wren.testing.DataType.decimalDataType;
import static io.wren.testing.DataType.doubleDataType;
import static io.wren.testing.DataType.integerDataType;
import static io.wren.testing.DataType.intervalType;
import static io.wren.testing.DataType.jsonDataType;
import static io.wren.testing.DataType.nameDataType;
import static io.wren.testing.DataType.realDataType;
import static io.wren.testing.DataType.smallintDataType;
import static io.wren.testing.DataType.textDataType;
import static io.wren.testing.DataType.timestampDataType;
import static io.wren.testing.DataType.varcharDataType;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.apache.commons.codec.binary.Hex.encodeHexString;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractWireProtocolTypeTest
        extends AbstractWireProtocolTest
{
    private final LocalDateTime beforeEpoch = LocalDateTime.of(1958, 1, 1, 13, 18, 3, 123_000_000);
    private final LocalDateTime epoch = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
    private final LocalDateTime afterEpoch = LocalDateTime.of(2019, 3, 18, 10, 1, 17, 987_000_000);

    private final LocalDateTime timeDoubledInJvmZone = LocalDateTime.of(2018, 10, 28, 1, 33, 17, 456_000_000);
    private final LocalDateTime timeDoubledInVilnius = LocalDateTime.of(2018, 10, 28, 3, 33, 33, 333_000_000);
    private final LocalDateTime timeGapInJvmZone1 = LocalDateTime.of(1970, 1, 1, 0, 13, 42);
    private final LocalDateTime timeGapInJvmZone2 = LocalDateTime.of(2018, 4, 1, 2, 13, 55, 123_000_000);
    private final LocalDateTime timeGapInVilnius = LocalDateTime.of(2018, 3, 25, 3, 17, 17);
    private final LocalDateTime timeGapInKathmandu = LocalDateTime.of(1986, 1, 1, 0, 13, 7);

    @Test
    public void testBoolean()
    {
        createTypeTest()
                .addInput(booleanDataType(), true)
                .addInput(booleanDataType(), false)
                .executeSuite();
    }

    @Test
    public void testSmallint()
    {
        createTypeTest()
                .addInput(smallintDataType(), (short) 32_456, value -> (int) value)
                .executeSuite();
    }

    @Test
    public void testInteger()
    {
        createTypeTest()
                .addInput(integerDataType(), 1_234_567_890)
                .executeSuite();
    }

    @Test
    public void tesBigint()
    {
        createTypeTest()
                .addInput(bigintDataType(), 123_456_789_012L)
                .executeSuite();
    }

    @Test
    public void testDouble()
    {
        createTypeTest()
                .addInput(doubleDataType(), 123.45d)
                .executeSuite();
    }

    @Test
    public void testReal()
    {
        createTypeTest()
                .addInput(realDataType(), 123.45f)
                .executeSuite();
    }

    @Test
    public void testChar()
    {
        createTypeTest()
                .addInput(charDataType(), "c")
                .addInput(charDataType(9), "test char")
                .executeSuite();
    }

    @Test
    public void testBytea()
    {
        createTypeTest()
                .addInput(byteaDataType(), "hello", value -> value.getBytes(UTF_8))
                .addInput(byteaDataType(), "\\x68656c6c6f", ignored -> "hello".getBytes(UTF_8))
                .executeSuite();
    }

    @Test
    public void testJson()
    {
        jsonTestCases(jsonDataType())
                .executeSuite();
    }

    private WireProtocolTypeTest jsonTestCases(DataType<String> jsonDataType)
    {
        return createTypeTest()
                .addInput(jsonDataType, "{}")
                .addInput(jsonDataType, null)
                .addInput(jsonDataType, "null")
                .addInput(jsonDataType, "123.4")
                .addInput(jsonDataType, "\"abc\"")
                .addInput(jsonDataType, "\"text with \\\" quotations and ' apostrophes\"")
                .addInput(jsonDataType, "\"\"")
                .addInput(jsonDataType, "{\"a\":1,\"b\":2}")
                .addInput(jsonDataType, "{\"a\":[1,2,3],\"b\":{\"aa\":11,\"bb\":[{\"a\":1,\"b\":2},{\"a\":0}]}}")
                .addInput(jsonDataType, "[]");
    }

    @Test
    public void testCreatedParameterizedVarchar()
    {
        varcharDataTypeTest()
                .executeSuite();
    }

    private WireProtocolTypeTest varcharDataTypeTest()
    {
        return createTypeTest()
                .addInput(varcharDataType(10), "text_a")
                .addInput(varcharDataType(255), "text_b")
                .addInput(varcharDataType(65535), "text_d")
                .addInput(varcharDataType(10485760), "text_f")
                .addInput(varcharDataType(), "unbounded")
                .addInput(textDataType(), "wren_text")
                .addInput(nameDataType(), "wren_name");
    }

    @Test
    public void testCreatedParameterizedVarcharUnicode()
    {
        unicodeVarcharDateTypeTest()
                .executeSuite();
    }

    private WireProtocolTypeTest unicodeVarcharDateTypeTest()
    {
        return unicodeDataTypeTest(DataType::varcharDataType)
                .addInput(varcharDataType(), "\u041d\u0443, \u043f\u043e\u0433\u043e\u0434\u0438!");
    }

    private WireProtocolTypeTest unicodeDataTypeTest(Function<Integer, DataType<String>> dataTypeFactory)
    {
        String sampleUnicodeText = "\u653b\u6bbb\u6a5f\u52d5\u968a";
        String sampleFourByteUnicodeCharacter = "\uD83D\uDE02";

        return createTypeTest()
                .addInput(dataTypeFactory.apply(sampleUnicodeText.length()), sampleUnicodeText)
                .addInput(dataTypeFactory.apply(32), sampleUnicodeText)
                .addInput(dataTypeFactory.apply(20000), sampleUnicodeText)
                .addInput(dataTypeFactory.apply(1), sampleFourByteUnicodeCharacter);
    }

    @Test
    public void testDecimal()
    {
        createTypeTest()
                .addInput(decimalDataType(), new BigDecimal("0.123"))
                .addInput(decimalDataType(3, 0), new BigDecimal("0"))
                .addInput(decimalDataType(3, 0), new BigDecimal("193"))
                .addInput(decimalDataType(3, 0), new BigDecimal("19"))
                .addInput(decimalDataType(3, 0), new BigDecimal("-193"))
                .addInput(decimalDataType(3, 1), new BigDecimal("10.0"))
                .addInput(decimalDataType(3, 1), new BigDecimal("10.1"))
                .addInput(decimalDataType(3, 1), new BigDecimal("-10.1"))
                .addInput(decimalDataType(4, 2), new BigDecimal("2.00"))
                .addInput(decimalDataType(4, 2), new BigDecimal("2.30"))
                .addInput(decimalDataType(24, 2), new BigDecimal("2.00"))
                .addInput(decimalDataType(24, 2), new BigDecimal("2.30"))
                .addInput(decimalDataType(24, 2), new BigDecimal("123456789.30"))
                .addInput(decimalDataType(24, 4), new BigDecimal("12345678901234567890.3100"))
                .addInput(decimalDataType(30, 5), new BigDecimal("3141592653589793238462643.38327"))
                .addInput(decimalDataType(30, 5), new BigDecimal("-3141592653589793238462643.38327"))
                .addInput(decimalDataType(30, 0), new BigDecimal("9223372036854775807"))
                .addInput(decimalDataType(38, 0), new BigDecimal("27182818284590452353602874713526624977"))
                .addInput(decimalDataType(38, 9), new BigDecimal("27182818284590452353602874713.526624977"))
                .addInput(decimalDataType(39, 9), new BigDecimal("271828182845904523536028747130.526624977"))
                .addInput(decimalDataType(38, 10), new BigDecimal("2718281828459045235360287471.3526624977"))
                .executeSuite();
    }

    @Test
    public void testArray()
    {
        // basic types
        createTypeTest()
                .addInput(arrayDataType(booleanDataType(), BOOL_ARRAY), asList(true, false))
                .addInput(arrayDataType(smallintDataType(), INT2_ARRAY), asList((short) 1, (short) 2))
                .addInput(arrayDataType(integerDataType(), INT4_ARRAY), asList(1, 2, 1_234_567_890))
                .addInput(arrayDataType(bigintDataType(), INT8_ARRAY), asList(123_456_789_012L, 1_234_567_890L))
                .addInput(arrayDataType(realDataType(), FLOAT4_ARRAY), asList(123.45f, 1.2345f))
                .addInput(arrayDataType(doubleDataType(), FLOAT8_ARRAY), asList(123.45d, 1.2345d))
                .addInput(arrayDataType(decimalDataType(3, 1), NUMERIC_ARRAY), asList(new BigDecimal("1.0"), new BigDecimal("1.1")))
                .addInput(arrayDataType(varcharDataType(), VARCHAR_ARRAY), asList("hello", "world"))
                .addInput(arrayDataType(charDataType(), CHAR_ARRAY), asList("h", "w"))
                .addInput(arrayDataType(byteaDataType(), BYTEA_ARRAY), asList("hello", "world"), value -> asList("\\x68656c6c6f", "\\x776f726c64"))
                .addInput(arrayDataType(jsonDataType(), JSON_ARRAY), asList("{\"a\": \"apple\"}", "{\"b\": \"banana\"}"))
                .addInput(arrayDataType(timestampDataType(3), TIMESTAMP_ARRAY),
                        asList(LocalDateTime.of(2019, 1, 1, 1, 1, 1, 1_000_000),
                                LocalDateTime.of(2019, 1, 1, 1, 1, 1, 2_000_000)),
                        value -> value.stream().map(Timestamp::valueOf).collect(toList()))
                .addInput(arrayDataType(dateDataType(), DATE_ARRAY), asList(LocalDate.of(2019, 1, 1), LocalDate.of(2019, 1, 2)),
                        value -> value.stream().map(Date::valueOf).collect(toList()))
                // TODO support interval
                .executeSuite();
    }

    @Test
    public void testDate()
    {
        ZoneId jvmZone = ZoneId.systemDefault();
        checkState(jvmZone.getId().equals("America/Bahia_Banderas"), "This test assumes certain JVM time zone");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInJvmZone = LocalDate.of(1970, 1, 1);
        checkIsGap(jvmZone, dateOfLocalTimeChangeForwardAtMidnightInJvmZone.atStartOfDay());

        ZoneId someZone = ZoneId.of("Europe/Vilnius");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone = LocalDate.of(1983, 4, 1);
        checkIsGap(someZone, dateOfLocalTimeChangeForwardAtMidnightInSomeZone.atStartOfDay());
        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDate.of(1983, 10, 1);
        checkIsDoubled(someZone, dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.atStartOfDay().minusMinutes(1));
        Function<LocalDate, ?> toJdbcTimestamp = date -> Timestamp.valueOf(LocalDateTime.of(date, LocalTime.of(0, 0)));

        WireProtocolTypeTest testCases = createTypeTest()
                .addInput(dateDataType(), LocalDate.of(1952, 4, 3), toJdbcTimestamp) // before epoch
                .addInput(dateDataType(), LocalDate.of(1970, 1, 1), toJdbcTimestamp)
                .addInput(dateDataType(), LocalDate.of(1970, 2, 3), toJdbcTimestamp)
                .addInput(dateDataType(), LocalDate.of(2017, 7, 1), toJdbcTimestamp) // summer on northern hemisphere (possible DST)
                .addInput(dateDataType(), LocalDate.of(2017, 1, 1), toJdbcTimestamp) // winter on northern hemisphere (possible DST on southern hemisphere)
                .addInput(dateDataType(), dateOfLocalTimeChangeForwardAtMidnightInJvmZone, toJdbcTimestamp)
                .addInput(dateDataType(), dateOfLocalTimeChangeForwardAtMidnightInSomeZone, toJdbcTimestamp)
                .addInput(dateDataType(), dateOfLocalTimeChangeBackwardAtMidnightInSomeZone, toJdbcTimestamp);
        testCases.executeSuite();
    }

    @Test
    public void testTimestamp()
    {
        createTypeTest()
                .addInput(timestampDataType(3), beforeEpoch, Timestamp::valueOf)
                .addInput(timestampDataType(3), afterEpoch, Timestamp::valueOf)
                .addInput(timestampDataType(3), timeDoubledInJvmZone, Timestamp::valueOf)
                .addInput(timestampDataType(3), timeDoubledInVilnius, Timestamp::valueOf)
                .addInput(timestampDataType(3), epoch, Timestamp::valueOf)
                .addInput(timestampDataType(3), timeGapInJvmZone1, Timestamp::valueOf)
                .addInput(timestampDataType(3), timeGapInJvmZone2, Timestamp::valueOf)
                .addInput(timestampDataType(3), timeGapInVilnius, Timestamp::valueOf)
                .addInput(timestampDataType(3), timeGapInKathmandu, Timestamp::valueOf)
                .addInput(timestampDataType(6), LocalDateTime.of(2023, 4, 24, 17, 43, 3, 123_456_000), Timestamp::valueOf)
                .executeSuite();
    }

    // TODO: Interval oid in DuckDB is 27 but postgres is 1186. Need to do other type mapping.
    @Test(enabled = false)
    public void testInterval()
    {
        createTypeTest()
                .addInput(intervalType(), "'1' year", value -> toPGInterval("1 year"))
                .addInput(intervalType(), "'-5' DAY", value -> toPGInterval("-5 days"))
                .addInput(intervalType(), "'-1' SECOND", value -> toPGInterval("-00:00:01"))
                .addInput(intervalType(), "'-25' MONTH", value -> toPGInterval("-2 years -1 mons"))
                .addInput(intervalType(), "'10:20:30.52' HOUR TO SECOND", value -> toPGInterval("10:20:30.520"))
                .addInput(intervalType(), "'1-2' YEAR TO MONTH", value -> toPGInterval("1 year 2 mons"))
                .addInput(intervalType(), "'1 5:30' DAY TO MINUTE", value -> toPGInterval("1 day 05:30:00"))
                .executeSuite();
    }

    private static PGobject toPGInterval(String value)
    {
        try {
            PGInterval pgInterval = new PGInterval();
            pgInterval.setType("interval");
            pgInterval.setValue(value);
            return pgInterval;
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void checkIsGap(ZoneId zone, LocalDateTime dateTime)
    {
        verify(isGap(zone, dateTime), "Expected %s to be a gap in %s", dateTime, zone);
    }

    private static boolean isGap(ZoneId zone, LocalDateTime dateTime)
    {
        return zone.getRules().getValidOffsets(dateTime).isEmpty();
    }

    private static void checkIsDoubled(ZoneId zone, LocalDateTime dateTime)
    {
        verify(zone.getRules().getValidOffsets(dateTime).size() == 2, "Expected %s to be doubled in %s", dateTime, zone);
    }

    protected static <E> DataType<List<E>> arrayDataType(DataType<E> elementType, PGArray insertType)
    {
        return dataType(
                format("ARRAY(%s)", elementType.getInsertType()),
                insertType,
                valuesList -> "array" + valuesList.stream().map(elementType::toLiteral).collect(toList()));
    }

    protected WireProtocolTypeTest createTypeTest()
    {
        return new WireProtocolTypeTest();
    }

    public class WireProtocolTypeTest
    {
        private final List<Input<?>> inputs = new ArrayList<>();

        private WireProtocolTypeTest() {}

        public <T> WireProtocolTypeTest addInput(DataType<T> type, T value)
        {
            inputs.add(new WireProtocolTypeTest.Input<>(type, value));
            return this;
        }

        public <T> WireProtocolTypeTest addInput(DataType<T> inputType, T inputValue, Function<T, ?> toJdbcQueryResult)
        {
            inputs.add(new WireProtocolTypeTest.Input<>(inputType, inputValue, toJdbcQueryResult));
            return this;
        }

        public void executeSuite()
        {
            try {
                execute(1);
                // just want to test multirows, it is ok that the data are the same
                execute(10);
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        private void execute(int rowCopies)
                throws SQLException
        {
            List<Object> expectedResults = inputs.stream().map(WireProtocolTypeTest.Input::toJdbcQueryResult).collect(toList());
            List<String> expectedTypeName = inputs.stream().map(Input::getInsertType).collect(toList());

            try (Connection conn = createConnection()) {
                Statement stmt = conn.createStatement();
                String sql = prepareQueryForDataType(rowCopies);
                stmt.execute(sql);
                ResultSet result = stmt.getResultSet();
                long count = 0;
                while (result.next()) {
                    for (int i = 0; i < expectedResults.size(); i++) {
                        Object actual = result.getObject(i + 1);
                        if (actual instanceof Array) {
                            assertArrayEquals((Array) actual, (List<?>) expectedResults.get(i), expectedTypeName.get(i));
                        }
                        else if (actual instanceof PGInterval) {
                            PGInterval pgInterval = (PGInterval) actual;
                            assertThat(pgInterval.getValue()).isEqualTo(((PGInterval) expectedResults.get(i)).getValue());
                        }
                        else {
                            assertThat(actual).isEqualTo(expectedResults.get(i));
                        }
                    }
                    count++;
                }
                assertThat(count).isEqualTo(rowCopies);
            }
        }

        /**
         * Jdbc will get array result by Java array. Transform it to List to match the type of the expected answer.
         */
        private Object arrayToList(Object value, String expectedType)
        {
            if (value instanceof Object[]) {
                // We don't use toImmutableList here because it requires non-null elements but there are null values in test cases.
                return Arrays.stream((Object[]) value).map(inner -> arrayToList(inner, expectedType)).collect(toList());
            }
            if (value instanceof byte[]) {
                return "\\x" + encodeHexString((byte[]) value);
            }
            if (value instanceof PGobject) {
                String type = ((PGobject) value).getType();
                if (type.equals("record")) {
                    String pValue = ((PGobject) value).getValue();
                    return ImmutableList.copyOf(pValue.substring(1, pValue.length() - 1).split(","));
                }
            }
            return value;
        }

        private void assertArrayEquals(Array jdbcArray, List<?> expected, String expectedType)
                throws SQLException
        {
            Object[] actualArray = (Object[]) jdbcArray.getArray();
            Object actual = arrayToList(actualArray, expectedType);
            assertThat(actual).isEqualTo(expected);
        }

        private String prepareQueryForDataType(int rowCopies)
        {
            Stream<String> columnValuesWithNames = range(0, inputs.size())
                    .mapToObj(i -> format("%s col_%d", literalInExplicitCast(inputs.get(i)), i));
            String selectBody = Joiner.on(",\n").join(columnValuesWithNames.iterator());
            return Joiner.on("\nUNION ALL\n").join(nCopies(rowCopies, format("SELECT %s", selectBody)));
        }

        private String literalInExplicitCast(WireProtocolTypeTest.Input<?> input)
        {
            if (input.getInsertType().startsWith("decimal")) {
                return format("CAST('%s' AS %s)", input.toLiteral(), input.getInsertType());
            }
            return format("CAST(%s AS %s)", input.toLiteral(), input.getInsertType());
        }

        public class Input<T>
        {
            private final DataType<T> dataType;
            private final T value;
            private final Function<T, ?> toJdbcQueryResult;

            public Input(DataType<T> dataType, T value)
            {
                this(dataType, value, null);
            }

            public Input(DataType<T> dataType, T value, Function<T, ?> toJdbcQueryResult)
            {
                this.dataType = dataType;
                this.value = value;
                this.toJdbcQueryResult = toJdbcQueryResult;
            }

            public String getInsertType()
            {
                return dataType.getInsertType();
            }

            public Object toJdbcQueryResult()
            {
                if (toJdbcQueryResult != null) {
                    return toJdbcQueryResult.apply(value);
                }
                return value;
            }

            public String toLiteral()
            {
                return dataType.toLiteral(value);
            }
        }
    }
}
