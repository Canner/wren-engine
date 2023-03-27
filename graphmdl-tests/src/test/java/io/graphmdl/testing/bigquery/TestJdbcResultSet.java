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

import com.google.common.collect.ImmutableMap;
import io.graphmdl.testing.AbstractWireProtocolTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestJdbcResultSet
        extends AbstractWireProtocolTest
{
    // TODO Force mapping type before we fix https://github.com/Canner/canner-metric-layer/issues/196
    private final Map<Integer, Integer> TYPE_FORCE_MAPPING = ImmutableMap.<Integer, Integer>builder()
            .put(Types.SMALLINT, Types.BIGINT)
            .put(Types.INTEGER, Types.BIGINT)
            .put(Types.REAL, Types.DOUBLE)
            .put(Types.NUMERIC, Types.DOUBLE)
            .put(Types.DECIMAL, Types.DOUBLE)
            .put(Types.CHAR, Types.VARCHAR)
            .build();
    private Connection connection;
    private Statement statement;

    @SuppressWarnings("JDBCResourceOpenedButNotSafelyClosed")
    @BeforeMethod
    public void setup()
            throws Exception
    {
        connection = createConnection();
        statement = connection.createStatement();
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(statement);
        closeQuietly(connection);
    }

    @Test
    public void testDuplicateColumnLabels()
            throws Exception
    {
        try (ResultSet rs = statement.executeQuery("SELECT 123 x, 456 x")) {
            ResultSetMetaData metadata = rs.getMetaData();
            assertEquals(metadata.getColumnCount(), 2);
            assertEquals(metadata.getColumnName(1), "x");
            // TODO: the second column name should be `x`
            assertEquals(metadata.getColumnName(2), "x0");

            assertTrue(rs.next());
            assertEquals(rs.getLong(1), 123L);
            assertEquals(rs.getLong(2), 456L);
            assertEquals(rs.getLong("x"), 123L);
        }
    }

    @Test
    public void testObjectTypes()
            throws Exception
    {
        checkRepresentation("123", Types.INTEGER, (long) 123);
        checkRepresentation("12300000000", Types.BIGINT, 12300000000L);
        checkRepresentation("REAL '123.45'", Types.REAL, 123.45);
        checkRepresentation("1e-1", Types.DOUBLE, 0.1);
        // TODO bigquery can't do division by zero
//        checkRepresentation("1.0E0 / 0.0E0", Types.DOUBLE, Double.POSITIVE_INFINITY);
//        checkRepresentation("0.0E0 / 0.0E0", Types.DOUBLE, Double.NaN);
        checkRepresentation("0.1", Types.NUMERIC, 0.1);
        // In PostgreSQL JDBC, BooleanType will be represent to JDBC Bit Type
        // https://github.com/pgjdbc/pgjdbc/blob/master/pgjdbc/src/main/java/org/postgresql/jdbc/TypeInfoCache.java#L95
        checkRepresentation("true", Types.BIT, true);
        checkRepresentation("'hello'", Types.VARCHAR, "hello");
        checkRepresentation("cast('foo' as char(5))", Types.CHAR, "foo  ");
        checkRepresentation("ARRAY[1, 2]", Types.ARRAY, (rs, column) -> assertEquals(rs.getArray(column).getArray(), new long[] {1, 2}));
        checkRepresentation("DECIMAL '0.1'", Types.DECIMAL, 0.1);
        // TODO:
        // checkRepresentation("IPADDRESS '1.2.3.4'", Types.JAVA_OBJECT, "1.2.3.4");
        // TODO should be Types.OTHER：https://github.com/Canner/canner-metric-layer/issues/196
        checkRepresentation("UUID '0397e63b-2b78-4b7b-9c87-e085fa225dd8'", Types.VARCHAR, "0397e63b-2b78-4b7b-9c87-e085fa225dd8");

        checkRepresentation("DATE '2018-02-13'", Types.DATE, (rs, column) -> {
            assertEquals(rs.getObject(column), Date.valueOf(LocalDate.of(2018, 2, 13)));
            assertEquals(rs.getDate(column), Date.valueOf(LocalDate.of(2018, 2, 13)));
        });

        checkRepresentation("TIMESTAMP '2018-02-13 13:14:15.123'", Types.TIMESTAMP, (rs, column) -> {
            assertEquals(rs.getObject(column), Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 13, 14, 15, 123_000_000)));
            assertEquals(rs.getTimestamp(column), Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 13, 14, 15, 123_000_000)));
        });

        // TODO: need to support variable precision timestamp, now set precision as 3
        checkRepresentation("TIMESTAMP '2018-02-13 13:14:15.111111111111'", Types.TIMESTAMP, (rs, column) -> {
            assertEquals(rs.getObject(column), Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 13, 14, 15, 111_000_000)));
            assertEquals(rs.getTimestamp(column), Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 13, 14, 15, 111_000_000)));
        });

        checkRepresentation("TIMESTAMP '2018-02-13 13:14:15.555555555555'", Types.TIMESTAMP, (rs, column) -> {
            assertEquals(rs.getObject(column), Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 13, 14, 15, 555_000_000)));
            assertEquals(rs.getTimestamp(column), Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 13, 14, 15, 555_000_000)));
        });

        // TODO support timestamp with timezone
//        checkRepresentation("TIMESTAMP '2018-02-13 13:14:15.227 Europe/Warsaw'", Types.TIMESTAMP, (rs, column) -> {
//            assertEquals(rs.getObject(column), Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 6, 14, 15, 227_000_000)));
//            assertEquals(rs.getTimestamp(column), Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 6, 14, 15, 227_000_000)));
//        });
//
//        checkRepresentation("TIMESTAMP '1970-01-01 09:14:15.227 Europe/Warsaw'", Types.TIMESTAMP, (rs, column) -> {
//            assertEquals(rs.getObject(column), Timestamp.valueOf(LocalDateTime.of(1970, 1, 1, 1, 14, 15, 227_000_000)));
//            assertEquals(rs.getTimestamp(column), Timestamp.valueOf(LocalDateTime.of(1970, 1, 1, 1, 14, 15, 227_000_000)));
//        });
//
//        checkRepresentation("TIMESTAMP '1970-01-01 00:14:15.227 Europe/Warsaw'", Types.TIMESTAMP, (rs, column) -> {
//            assertEquals(rs.getObject(column), Timestamp.valueOf(LocalDateTime.of(1969, 12, 31, 15, 14, 15, 227_000_000)));
//            assertEquals(rs.getTimestamp(column), Timestamp.valueOf(LocalDateTime.of(1969, 12, 31, 15, 14, 15, 227_000_000)));
//        });
    }

    private void checkRepresentation(String expression, int expectedSqlType, Object expectedRepresentation)
            throws Exception
    {
        int expectedForceMappingType = TYPE_FORCE_MAPPING.getOrDefault(expectedSqlType, expectedSqlType);
        checkRepresentation(expression, expectedForceMappingType, (rs, column) -> assertEquals(rs.getObject(column), expectedRepresentation));
    }

    private void checkRepresentation(String expression, int expectedSqlType, ResultAssertion assertion)
            throws Exception
    {
        try (ResultSet rs = statement.executeQuery("SELECT " + expression)) {
            ResultSetMetaData metadata = rs.getMetaData();
            assertEquals(metadata.getColumnCount(), 1);
            assertEquals(metadata.getColumnType(1), expectedSqlType);
            assertTrue(rs.next());
            assertion.accept(rs, 1);
            assertFalse(rs.next());
        }
    }

    @Test
    public void testMaxRowsUnset()
            throws SQLException
    {
        assertMaxRowsLimit(0);
        assertMaxRowsResult(7);
    }

    @Test
    public void testMaxRowsUnlimited()
            throws SQLException
    {
        assertMaxRowsLimit(0);
        statement.setMaxRows(0);
        assertMaxRowsLimit(0);
        assertMaxRowsResult(7);
    }

    @Test
    public void testMaxRowsLimited()
            throws SQLException
    {
        assertMaxRowsLimit(0);
        statement.setMaxRows(4);
        assertMaxRowsLimit(4);
        assertMaxRowsResult(4);
    }

    @Test
    public void testMaxRowsLimitLargerThanResult()
            throws SQLException
    {
        assertMaxRowsLimit(0);
        statement.setMaxRows(10);
        assertMaxRowsLimit(10);
        assertMaxRowsResult(7);
    }

    private void assertMaxRowsLimit(int expectedLimit)
            throws SQLException
    {
        assertEquals(statement.getMaxRows(), expectedLimit);
    }

    private void assertMaxRowsResult(long expectedCount)
            throws SQLException
    {
        try (ResultSet rs = statement.executeQuery("SELECT * FROM (VALUES (1), (2), (3), (4), (5), (6), (7)) AS x (a)")) {
            assertEquals(countRows(rs), expectedCount);
        }
    }

    private static long countRows(ResultSet rs)
            throws SQLException
    {
        long count = 0;
        while (rs.next()) {
            count++;
        }
        return count;
    }

    @FunctionalInterface
    private interface ResultAssertion
    {
        void accept(ResultSet rs, int column)
                throws Exception;
    }

    static void closeQuietly(AutoCloseable closeable)
    {
        try {
            closeable.close();
        }
        catch (Exception ignored) {
        }
    }
}
