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

import static java.lang.System.getenv;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

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
                    .hasRows((ImmutableList.of(ImmutableList.of("ApplicationName", 63, "", "The name of the application currently utilizing the connection."))));
        }
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
