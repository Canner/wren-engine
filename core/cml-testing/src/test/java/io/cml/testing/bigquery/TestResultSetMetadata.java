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

package io.cml.testing.bigquery;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cml.testing.AbstractWireProtocolTest;
import io.cml.testing.TestingWireProtocolServer;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.System.getenv;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestResultSetMetadata
        extends AbstractWireProtocolTest
{
    @Override
    protected TestingWireProtocolServer createWireProtocolServer()
    {
        return TestingWireProtocolServer.builder()
                .setRequiredConfigs(
                        ImmutableMap.<String, String>builder()
                                .put("bigquery.project-id", getenv("TEST_BIG_QUERY_PROJECT_ID"))
                                .put("bigquery.location", "US")
                                .put("bigquery.credentials-key", getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"))
                                .build())
                .build();
    }

    @Test
    public void testGetClientInfoProperties()
            throws SQLException
    {
        try (Connection connection = createConnection()) {
            assertResultSet(connection.getMetaData().getClientInfoProperties())
                    .hasColumnCount(4)
                    .hasColumn(1, "NAME", Types.VARCHAR)
                    .hasColumn(2, "MAX_LEN", Types.INTEGER)
                    .hasColumn(3, "DEFAULT_VALUE", Types.VARCHAR)
                    .hasColumn(4, "DESCRIPTION", Types.VARCHAR)
                    .hasRows((list(list("ApplicationName", 63, "", "The name of the application currently utilizing the connection."))));
        }
    }

    @Test(enabled = false)
    public void testGetTypeInfo()
            throws Exception
    {
        Map<Object, List<Object>> map = ImmutableMap.<Object, List<Object>>builder()
                .put("bool", list("bool", -7, 0, "'", "'", null, 1, false, 3, true, false, false, null, 0, 0, null, null, 10))
                .put("int2", list("int2", 5, 0, null, null, null, 1, false, 3, false, false, false, null, 0, 0, null, null, 10))
                .put("int4", list("int4", 4, 0, null, null, null, 1, false, 3, false, false, false, null, 0, 0, null, null, 10))
                .put("int8", list("int8", -5, 0, null, null, null, 1, false, 3, false, false, false, null, 0, 0, null, null, 10))
                .put("float4", list("float4", 7, 0, null, null, null, 1, false, 3, false, false, false, null, 0, 0, null, null, 10))
                .put("float8", list("float8", 8, 0, null, null, null, 1, false, 3, false, false, false, null, 0, 0, null, null, 10))
                .put("numeric", list("numeric", 2, 1000, null, null, null, 1, false, 3, false, false, false, null, 0, 1000, null, null, 10))
                .put("varchar", list("varchar", 12, 10485760, "'", "'", null, 1, true, 3, true, false, false, null, 0, 0, null, null, 10))
                .put("char", list("char", 1, 0, "'", "'", null, 1, true, 3, true, false, false, null, 0, 0, null, null, 10))
                .put("timestamp", list("timestamp", 93, 6, "'", "'", null, 1, false, 3, true, false, false, null, 0, 0, null, null, 10))
                .put("timestamptz", list("timestamptz", 93, 6, "'", "'", null, 1, false, 3, true, false, false, null, 0, 0, null, null, 10))
                .build();

        try (Connection connection = createConnection()) {
            ResultSet resultSet = connection.getMetaData().getTypeInfo();
            while (resultSet.next()) {
                String type = resultSet.getString("TYPE_NAME");
                if (map.containsKey(type)) {
                    assertColumnSpec(resultSet, map.get(type));
                }
            }
        }
    }

    private static void assertColumnSpec(ResultSet rs, List<Object> expects)
            throws SQLException
    {
        String message = " of " + expects.get(0) + ": ";
        assertEquals(rs.getObject("TYPE_NAME"), expects.get(0), "TYPE_NAME" + message);
        assertEquals(rs.getObject("DATA_TYPE"), expects.get(1), "DATA_TYPE" + message);
        assertEquals(rs.getObject("PRECISION"), expects.get(2), "PRECISION" + message);
        assertEquals(rs.getObject("LITERAL_PREFIX"), expects.get(3), "LITERAL_PREFIX" + message);
        assertEquals(rs.getObject("LITERAL_SUFFIX"), expects.get(4), "LITERAL_SUFFIX" + message);
        assertEquals(rs.getObject("CREATE_PARAMS"), expects.get(5), "CREATE_PARAMS" + message);
        assertEquals(rs.getObject("NULLABLE"), expects.get(6), "NULLABLE" + message);
        assertEquals(rs.getObject("CASE_SENSITIVE"), expects.get(7), "CASE_SENSITIVE" + message);
        assertEquals(rs.getObject("SEARCHABLE"), expects.get(8), "SEARCHABLE" + message);
        assertEquals(rs.getObject("UNSIGNED_ATTRIBUTE"), expects.get(9), "UNSIGNED_ATTRIBUTE" + message);
        assertEquals(rs.getObject("FIXED_PREC_SCALE"), expects.get(10), "FIXED_PREC_SCALE" + message);
        assertEquals(rs.getObject("AUTO_INCREMENT"), expects.get(11), "AUTO_INCREMENT" + message);
        assertEquals(rs.getObject("LOCAL_TYPE_NAME"), expects.get(12), "LOCAL_TYPE_NAME" + message);
        assertEquals(rs.getObject("MINIMUM_SCALE"), expects.get(13), "MINIMUM_SCALE" + message);
        assertEquals(rs.getObject("MAXIMUM_SCALE"), expects.get(14), "MAXIMUM_SCALE" + message);
        assertEquals(rs.getObject("SQL_DATA_TYPE"), expects.get(15), "SQL_DATA_TYPE" + message);
        assertEquals(rs.getObject("SQL_DATETIME_SUB"), expects.get(16), "SQL_DATETIME_SUB" + message);
        assertEquals(rs.getObject("NUM_PREC_RADIX"), expects.get(17), "NUM_PREC_RADIX" + message);
    }

    @SafeVarargs
    public static <T> List<T> list(T... elements)
    {
        return asList(elements);
    }

    public static ResultSetAssert assertResultSet(ResultSet resultSet)
    {
        return new ResultSetAssert(resultSet);
    }

    public static class ResultSetAssert
    {
        private final ResultSet resultSet;

        public ResultSetAssert(ResultSet resultSet)
        {
            this.resultSet = requireNonNull(resultSet, "resultSet is null");
        }

        public ResultSetAssert hasColumnCount(int expectedColumnCount)
                throws SQLException
        {
            assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(expectedColumnCount);
            return this;
        }

        public ResultSetAssert hasColumn(int columnIndex, String name, int sqlType)
                throws SQLException
        {
            assertThat(resultSet.getMetaData().getColumnName(columnIndex)).isEqualTo(name);
            assertThat(resultSet.getMetaData().getColumnType(columnIndex)).isEqualTo(sqlType);
            return this;
        }

        public ResultSetAssert hasRows(List<List<?>> expected)
                throws SQLException
        {
            assertThat(readRows(resultSet)).isEqualTo(expected);
            return this;
        }
    }

    public static List<List<Object>> readRows(ResultSet rs)
            throws SQLException
    {
        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
        int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                row.add(rs.getObject(i));
            }
            rows.add(row);
        }
        return rows.build();
    }
}
