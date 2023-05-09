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

package io.graphmdl.testing.bigquery;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.graphmdl.base.type.PGTypes;
import io.graphmdl.testing.AbstractWireProtocolTest;
import io.graphmdl.testing.DataType;
import org.postgresql.util.PGobject;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
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
import static io.graphmdl.base.type.SmallIntType.SMALLINT;
import static io.graphmdl.base.type.TinyIntType.TINYINT;
import static io.graphmdl.testing.DataType.bigintDataType;
import static io.graphmdl.testing.DataType.booleanDataType;
import static io.graphmdl.testing.DataType.byteaDataType;
import static io.graphmdl.testing.DataType.charDataType;
import static io.graphmdl.testing.DataType.dataType;
import static io.graphmdl.testing.DataType.dateDataType;
import static io.graphmdl.testing.DataType.decimalDataType;
import static io.graphmdl.testing.DataType.doubleDataType;
import static io.graphmdl.testing.DataType.integerDataType;
import static io.graphmdl.testing.DataType.jsonDataType;
import static io.graphmdl.testing.DataType.nameDataType;
import static io.graphmdl.testing.DataType.realDataType;
import static io.graphmdl.testing.DataType.textDataType;
import static io.graphmdl.testing.DataType.timestampDataType;
import static io.graphmdl.testing.DataType.varcharDataType;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.apache.commons.codec.binary.Hex.encodeHexString;
import static org.assertj.core.api.Assertions.assertThat;

public class TestWireProtocolType
        extends AbstractWireProtocolTest
{
    // BigQuery has only INT64 type. We should cast other int to int32 after got them.
    private static final List<String> TYPE_FORCED_TO_LONG = ImmutableList.of("integer", "smallint", "tinyint", "array(integer)", "array(smallint)", "array(tinyint)");
    private static final List<String> TYPE_FORCED_TO_DOUBLE = ImmutableList.of("real", "array(real)");

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
    public void testBasicTypes()
    {
        createTypeTest()
                .addInput(booleanDataType(), true)
                .addInput(booleanDataType(), false)
                .addInput(bigintDataType(), 123_456_789_012L)
                .addInput(integerDataType(), 1_234_567_890)
                .addInput(jdbcSmallintDataType(), 32_456)
                .addInput(jdbcTinyintDataType(), 125)
                .addInput(doubleDataType(), 123.45d)
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
                .addInput(textDataType(), "graphmdl_text")
                .addInput(nameDataType(), "graphmdl_name");
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
    public void testCreatedDecimal()
    {
        decimalTests()
                .executeSuite();
    }

    private WireProtocolTypeTest decimalTests()
    {
        // BigQuery will remove trailing zeros from values.
        Function<BigDecimal, BigDecimal> removeTrailingZeros = value -> new BigDecimal(value.stripTrailingZeros().toPlainString());

        return createTypeTest()
                .addInput(decimalDataType(), new BigDecimal("0.123"))
                .addInput(decimalDataType(3, 0), new BigDecimal("0"))
                .addInput(decimalDataType(3, 0), new BigDecimal("193"))
                .addInput(decimalDataType(3, 0), new BigDecimal("19"))
                .addInput(decimalDataType(3, 0), new BigDecimal("-193"))
                .addInput(decimalDataType(3, 1), new BigDecimal("10.0"), removeTrailingZeros)
                .addInput(decimalDataType(3, 1), new BigDecimal("10.1"))
                .addInput(decimalDataType(3, 1), new BigDecimal("-10.1"))
                .addInput(decimalDataType(4, 2), new BigDecimal("2.00"), removeTrailingZeros)
                .addInput(decimalDataType(4, 2), new BigDecimal("2.30"), removeTrailingZeros)
                .addInput(decimalDataType(24, 2), new BigDecimal("2.00"), removeTrailingZeros)
                .addInput(decimalDataType(24, 2), new BigDecimal("2.30"), removeTrailingZeros)
                .addInput(decimalDataType(24, 2), new BigDecimal("123456789.30"), removeTrailingZeros)
                .addInput(decimalDataType(24, 4), new BigDecimal("12345678901234567890.3100"), removeTrailingZeros)
                .addInput(decimalDataType(30, 5), new BigDecimal("3141592653589793238462643.38327"))
                .addInput(decimalDataType(30, 5), new BigDecimal("-3141592653589793238462643.38327"))
                .addInput(decimalDataType(30, 0), new BigDecimal("9223372036854775807"))
                .addInput(decimalDataType(38, 0), new BigDecimal("27182818284590452353602874713526624977"))
                .addInput(decimalDataType(38, 9), new BigDecimal("27182818284590452353602874713.526624977"))
                .addInput(decimalDataType(39, 9), new BigDecimal("271828182845904523536028747130.526624977"))
                .addInput(decimalDataType(38, 10), new BigDecimal("2718281828459045235360287471.3526624977"));
    }

    @Test
    public void testArray()
    {
        // basic types
        createTypeTest()
                .addInput(arrayDataType(booleanDataType()), asList(true, false))
                .addInput(arrayDataType(bigintDataType()), asList(123_456_789_012L))
                .addInput(arrayDataType(integerDataType()), asList(1, 2, 1_234_567_890))
                // TODO: handle calcite array syntax
                //  https://github.com/Canner/canner-metric-layer/issues/69
                // .addInput(arrayDataType(smallintDataType()), asList((short) 32_456))
                // .addInput(arrayDataType(doubleDataType()), asList(123.45d))
                // .addInput(arrayDataType(realDataType()), asList(123.45f))
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

    private static <E> DataType<List<E>> arrayDataType(DataType<E> elementType)
    {
        return arrayDataType(elementType, format("array(%s)", elementType.getInsertType()));
    }

    private static <E> DataType<List<E>> arrayDataType(DataType<E> elementType, String insertType)
    {
        return dataType(
                insertType,
                PGTypes.getArrayType(elementType.getPgResultType().oid()),
                valuesList -> "array" + valuesList.stream().map(elementType::toLiteral).collect(toList()));
    }

    private WireProtocolTypeTest createTypeTest()
    {
        return new WireProtocolTypeTest();
    }

    /**
     * JDBC get pg smallint by Integer
     */
    private static DataType<Integer> jdbcSmallintDataType()
    {
        return dataType("smallint", SMALLINT, Object::toString);
    }

    /**
     * JDBC get pg tinyint by Integer.
     */
    private static DataType<Integer> jdbcTinyintDataType()
    {
        return dataType("tinyint", TINYINT, Object::toString);
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
                        else if (expectedResults.get(i) instanceof PGobject) {
                            PGobject expected = (PGobject) expectedResults.get(i);
                            if ("inet".equals(expected.getType())) {
                                assertThat(actual).isEqualTo(expected.getValue());
                            }
                        }
                        else if (TYPE_FORCED_TO_LONG.contains(expectedTypeName.get(i))) {
                            assertThat(Long.valueOf((long) actual).intValue()).isEqualTo(expectedResults.get(i));
                        }
                        else if (TYPE_FORCED_TO_DOUBLE.contains(expectedTypeName.get(i))) {
                            assertThat(Double.valueOf((double) actual).floatValue()).isEqualTo(expectedResults.get(i));
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
            if (TYPE_FORCED_TO_LONG.contains(expectedType) && value instanceof Long) {
                return Long.valueOf((long) value).intValue();
            }
            if (TYPE_FORCED_TO_DOUBLE.contains(expectedType) && value instanceof Double) {
                return Double.valueOf((double) value).floatValue();
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
