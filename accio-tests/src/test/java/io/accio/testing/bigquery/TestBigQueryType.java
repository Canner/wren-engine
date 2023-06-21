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

package io.accio.testing.bigquery;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.inject.Key;
import io.accio.base.metadata.SchemaTableName;
import io.accio.connector.bigquery.BigQueryClient;
import io.airlift.log.Logger;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBigQueryType
        extends AbstractWireProtocolTestWithBigQuery
{
    private static final Logger LOG = Logger.get(TestBigQueryType.class);
    private SchemaTableName testSchemaTableName;
    private BigQueryClient bigQueryClient;
    private Multimap<DataType, TypeCase> testCases;

    enum DataType
    {
        BOOL,
        BYTES,
        STRING,
        INT64,
        FLOAT64,
        NUMERIC,
        BIGNUMERIC,
        DATE,
        TIMESTAMP,
        TIME,
        DATETIME,
        INTERVAL,
        JSON,
        GEOGRAPHY,
        ARRAY,
        STRUCT
    }

    @BeforeClass
    public void init()
            throws SQLException
    {
        testSchemaTableName = new SchemaTableName("cml_temp", "test_bigquery_type_" + currentTimeMillis());
        bigQueryClient = getInstance(Key.get(BigQueryClient.class));
        testCases = initTestcases();
        createBigQueryTable();
    }

    @AfterClass(alwaysRun = true)
    public void close()
    {
        bigQueryClient.dropTable(testSchemaTableName);
    }

    private Multimap<DataType, TypeCase> initTestcases()
            throws SQLException
    {
        Multimap<DataType, TypeCase> typeCaseMap = HashMultimap.create();
        typeCaseMap.put(DataType.BOOL, new TypeCase("bool", "true", true));
        typeCaseMap.put(DataType.BYTES, new TypeCase("bytes", "B\"hello\"", "hello".getBytes(UTF_8)));
        typeCaseMap.put(DataType.STRING, new TypeCase("string", "\"hello\"", "hello"));
        typeCaseMap.put(DataType.INT64, new TypeCase("int64", "12345", 12345L));
        typeCaseMap.put(DataType.FLOAT64, new TypeCase("float64", "1.2345", 1.2345));
        typeCaseMap.put(DataType.NUMERIC, new TypeCase("numeric", "1.2345", new BigDecimal("1.2345")));
        typeCaseMap.put(DataType.BIGNUMERIC, new TypeCase("bignumeric", "1.2345", new BigDecimal("1.2345")));
        typeCaseMap.put(DataType.DATE, new TypeCase("date", "\"2020-01-01\"", Date.valueOf("2020-01-01")));
        typeCaseMap.put(DataType.TIME, new TypeCase("time", "\"15:10:55\"", Time.valueOf("15:10:55")));
        typeCaseMap.put(DataType.TIMESTAMP, new TypeCase("timestamp", "\"2020-01-01 15:10:55\"", Timestamp.valueOf("2020-01-01 15:10:55")));
        typeCaseMap.put(DataType.DATETIME, new TypeCase("datetime", "\"2020-01-01 15:10:55.123456\"", Timestamp.valueOf("2020-01-01 15:10:55.123456")));
        typeCaseMap.put(DataType.JSON, new TypeCase("json", "PARSE_JSON(\"{\\\"a\\\": 1}\")", "{\"a\":1}"));
        typeCaseMap.put(DataType.INTERVAL, new TypeCase("interval", "INTERVAL '1' day", new PGInterval(0, 0, 1, 0, 0, 0)));
        typeCaseMap.put(DataType.GEOGRAPHY, new TypeCase("geography", "ST_GEOGPOINT(30, 50)", "POINT(30 50)"));

        typeCaseMap.put(DataType.ARRAY, new TypeCase("c_bool_in_array", "array<bool>", "[true, false]", new Boolean[] {true, false}));
        typeCaseMap.put(DataType.ARRAY, new TypeCase("c_bytes_in_array", "array<bytes>", "[B\"hello\", B\"world\"]", new byte[][] {"hello".getBytes(UTF_8),
                "world".getBytes(UTF_8)}));
        typeCaseMap.put(DataType.ARRAY, new TypeCase("c_string_in_array", "array<string>", "[\"hello\", \"world\"]", new String[] {"hello", "world"}));
        typeCaseMap.put(DataType.ARRAY, new TypeCase("c_int64_in_array", "array<int64>", "[1, 2, 3]", new Long[] {1L, 2L, 3L}));
        typeCaseMap.put(DataType.ARRAY, new TypeCase("c_float64_in_array", "array<float64>", "[1.1, 2.2, 3.3]", new Double[] {1.1, 2.2, 3.3}));
        typeCaseMap.put(DataType.ARRAY, new TypeCase("c_numeric_in_array", "array<numeric>", "[numeric '1.1', numeric '2.2', numeric '3.3']", new BigDecimal[] {
                new BigDecimal("1.1"),
                new BigDecimal("2.2"), new BigDecimal("3.3")}));
        typeCaseMap.put(DataType.ARRAY, new TypeCase("c_bignumeric_in_array", "array<bignumeric>", "[bignumeric '1.1', bignumeric '2.2', bignumeric '3.3']", new BigDecimal[] {
                new BigDecimal("1.1"), new BigDecimal("2.2"), new BigDecimal("3.3")}));
        typeCaseMap.put(DataType.ARRAY, new TypeCase("c_date_in_array", "array<date>", "[date \"2020-01-01\", date \"2020-01-02\"]", new Date[] {Date.valueOf("2020-01-01"),
                Date.valueOf("2020-01-02")}));
//        typeCaseMap.put(DataType.ARRAY, new TypeCase("c_time_in_array", "array<time>", "[time \"15:10:55\", time \"15:10:56\"]", new Time[]{Time.valueOf("15:10:55"), Time.valueOf("15:10:56")}));
        typeCaseMap.put(DataType.ARRAY, new TypeCase("c_timestamp_in_array", "array<timestamp>", "[timestamp \"2020-01-01 15:10:55\", timestamp \"2020-01-01 15:10:56\"]", new Timestamp[] {
                Timestamp.valueOf("2020-01-01 15:10:55"), Timestamp.valueOf("2020-01-01 15:10:56")}));
        typeCaseMap.put(DataType.ARRAY, new TypeCase("c_datetime_in_array", "array<datetime>", "[datetime \"2020-01-01 15:10:55.123456\", datetime \"2020-01-01 15:10:56.123456\"]", new Timestamp[] {
                Timestamp.valueOf("2020-01-01 15:10:55.123456"), Timestamp.valueOf("2020-01-01 15:10:56.123456")}));
        typeCaseMap.put(DataType.ARRAY, new TypeCase("c_json_in_array", "array<json>", "[PARSE_JSON(\"{\\\"a\\\": 1}\"), PARSE_JSON(\"{\\\"a\\\": 2}\")]", new String[] {"{a:1}",
                "{a:2}"}));
        PGobject s1 = new PGobject();
        s1.setType("record");
        s1.setValue("(1,hello)");
        PGobject s2 = new PGobject();
        s2.setType("record");
        s2.setValue("(2,world)");
        typeCaseMap.put(DataType.ARRAY, new TypeCase("c_struct_in_array", "array<struct<s1 int64, s2 string>>", "[(1, 'hello'), (2, 'world')]", new PGobject[] {s1, s2}));
//        typeCaseMap.put(DataType.ARRAY, new TypeCase("c_interval_in_array", "array<interval>", "[INTERVAL '1' day, INTERVAL '2' day]", new PGInterval[] {
//                new PGInterval(0, 0, 1, 0, 0, 0), new PGInterval(0, 0, 2, 0, 0, 0)}));
//        typeCaseMap.put(DataType.ARRAY, new TypeCase("c_geography_in_array", "array<geography>", "[ST_GEOGPOINT(30, 50), ST_GEOGPOINT(40, 60)]", new String[]{"POINT(30 50)", "POINT(40 60)"}));

        PGobject structObject = new PGobject();
        structObject.setType("record");
        structObject.setValue("(1,hello)");
        typeCaseMap.put(DataType.STRUCT, new TypeCase("c_struct", "struct<s1 int64, s2 string>", "(1, \"hello\")", structObject));
        PGobject structObject2 = new PGobject();
        structObject2.setType("record");
        structObject2.setValue("(1,\"(2,hello)\")");
        typeCaseMap.put(DataType.STRUCT, new TypeCase("c_multi_struct", "struct<s1 int64, s2 struct<s2_1 int64, s2_2 string>>", "(1, struct(2, \"hello\"))", structObject2));
        PGobject structObject3 = new PGobject();
        structObject3.setType("record");
        structObject3.setValue("(1,\"{hello,world}\")");

        typeCaseMap.put(DataType.STRUCT, new TypeCase("c_array_struct", "struct<s1 int64, s2 array<string>>", "(1, [\"hello\", \"world\"])", structObject3));
        return typeCaseMap;
    }

    public void createBigQueryTable()
    {
        String columnType = Joiner.on(", ").join(testCases.values().stream().map(value -> value.getColumnName() + " " + value.getTypeStatement()).collect(toList()));
        String columnValue = Joiner.on(", ").join(testCases.values().stream().map(TypeCase::getValue).collect(toList()));

        // CREATE TABLE table_name (column_name1 data_type1, column_name2 data_type2, ...);
        String createTableQuery = format("CREATE TABLE %s (%s)", testSchemaTableName, columnType);
        LOG.info("[Input sql]: %s", createTableQuery);
        bigQueryClient.query(createTableQuery, ImmutableList.of());

        // INSERT INTO table_name VALUES (value1, value2, value3, ...);
        String insertTableQuery = format("INSERT INTO %s VALUES (%s)", testSchemaTableName, columnValue);
        LOG.info("[Input sql]: %s", insertTableQuery);
        bigQueryClient.query(insertTableQuery, ImmutableList.of());
    }

    @Test
    public void testBoolean()
            throws SQLException
    {
        for (TypeCase testCase : testCases.get(DataType.BOOL)) {
            assertType(testCase, testCase.getExpectedPgValue());
        }
    }

    @Test
    public void testBytes()
            throws SQLException
    {
        for (TypeCase testCase : testCases.get(DataType.BYTES)) {
            assertType(testCase, testCase.getExpectedPgValue());
        }
    }

    @Test
    public void testString()
            throws SQLException
    {
        for (TypeCase testCase : testCases.get(DataType.STRING)) {
            assertType(testCase, testCase.getExpectedPgValue());
        }
    }

    @Test
    public void testInt64()
            throws SQLException
    {
        for (TypeCase testCase : testCases.get(DataType.INT64)) {
            assertType(testCase, testCase.getExpectedPgValue());
        }
    }

    @Test
    public void testFloat64()
            throws SQLException
    {
        for (TypeCase testCase : testCases.get(DataType.FLOAT64)) {
            assertType(testCase, testCase.getExpectedPgValue());
        }
    }

    @Test
    public void testNumeric()
            throws SQLException
    {
        for (TypeCase testCase : testCases.get(DataType.NUMERIC)) {
            assertType(testCase, testCase.getExpectedPgValue());
        }
    }

    @Test
    public void testBigNumeric()
            throws SQLException
    {
        for (TypeCase testCase : testCases.get(DataType.BIGNUMERIC)) {
            assertType(testCase, testCase.getExpectedPgValue());
        }
    }

    @Test
    public void testDate()
            throws SQLException
    {
        for (TypeCase testCase : testCases.get(DataType.DATE)) {
            assertType(testCase, testCase.getExpectedPgValue());
        }
    }

    // TODO
    @Test(enabled = false)
    public void testTime()
            throws SQLException
    {
        for (TypeCase testCase : testCases.get(DataType.TIME)) {
            assertType(testCase, testCase.getExpectedPgValue());
        }
    }

    @Test
    public void testTimestamp()
            throws SQLException
    {
        for (TypeCase testCase : testCases.get(DataType.TIMESTAMP)) {
            assertType(testCase, testCase.getExpectedPgValue());
        }
    }

    @Test
    public void testDateTime()
            throws SQLException
    {
        for (TypeCase testCase : testCases.get(DataType.DATETIME)) {
            assertType(testCase, testCase.getExpectedPgValue());
        }
    }

    @Test
    public void testJson()
            throws SQLException
    {
        for (TypeCase testCase : testCases.get(DataType.JSON)) {
            assertType(testCase, testCase.getExpectedPgValue());
        }
    }

    @Test
    public void testInterval()
            throws SQLException
    {
        for (TypeCase testCase : testCases.get(DataType.INTERVAL)) {
            assertType(testCase, testCase.getExpectedPgValue());
        }
    }

    // TODO
    @Test(enabled = false)
    public void testGeography()
            throws SQLException
    {
        for (TypeCase testCase : testCases.get(DataType.GEOGRAPHY)) {
            assertType(testCase, testCase.getExpectedPgValue());
        }
    }

    @Test
    public void testArray()
            throws SQLException
    {
        for (TypeCase testCase : testCases.get(DataType.ARRAY)) {
            assertType(testCase, testCase.getExpectedPgValue());
        }
    }

    @Test
    public void testStruct()
            throws SQLException
    {
        for (TypeCase testCase : testCases.get(DataType.STRUCT)) {
            assertType(testCase, testCase.getExpectedPgValue());
        }
    }

    private void assertType(TypeCase typeCase, Object expected)
            throws SQLException
    {
        try (Connection conn = createConnection()) {
            Statement stmt = conn.createStatement();
            String sql = format("SELECT %s FROM %s", typeCase.columnName, testSchemaTableName);
            LOG.info("[Input sql]: %s", sql);
            stmt.execute(sql);
            ResultSet result = stmt.getResultSet();
            int count = 0;
            while (result.next()) {
                Object actual = result.getObject(1);
                if (actual instanceof Array) {
                    assertArrayEquals((Array) actual, expected);
                }
                else {
                    assertThat(actual).isEqualTo(expected);
                }
                count++;
            }
            assertThat(count).isEqualTo(1);
        }
    }

    private void assertArrayEquals(Array jdbcArray, Object expected)
            throws SQLException
    {
        Object[] actualArray = (Object[]) jdbcArray.getArray();
        assertThat(actualArray).isEqualTo(expected);
    }

    private static class TypeCase
    {
        private final String columnName;
        private final String typeStatement;
        private final String value;
        private final Object expectedPgValue;

        TypeCase(String typeStatement, String value, Object expectedPgValue)
        {
            this(format("c_%s", typeStatement), typeStatement, value, expectedPgValue);
        }

        TypeCase(String columnName, String typeStatement, String value, Object expectedPgValue)
        {
            this.columnName = requireNonNull(columnName, "columnName is null");
            this.typeStatement = requireNonNull(typeStatement, "typeStatement is null");
            this.value = requireNonNull(value, "value is null");
            this.expectedPgValue = requireNonNull(expectedPgValue, "expectedPgValue is null");
        }

        public String getColumnName()
        {
            return columnName;
        }

        public String getTypeStatement()
        {
            return typeStatement;
        }

        public String getValue()
        {
            return value;
        }

        public Object getExpectedPgValue()
        {
            return expectedPgValue;
        }
    }
}
