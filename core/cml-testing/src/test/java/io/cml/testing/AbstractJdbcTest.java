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

import org.assertj.core.api.AssertionsForClassTypes;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public abstract class AbstractJdbcTest
        extends RequireCmlServer
        implements JdbcTesting
{
    @Test
    public void testJdbcConnection()
            throws Exception
    {
        try (Connection conn = createConnection(server())) {
            Statement stmt = conn.createStatement();
            stmt.execute("select * from (values ('rows1', 10), ('rows2', 10), ('rows3', 10)) as t(col1, col2) where col2 = 10");
            ResultSet result = stmt.getResultSet();
            long count = 0;
            while (result.next()) {
                count++;
            }

            AssertionsForClassTypes.assertThat(count).isEqualTo(3);
        }
    }

    @Test
    public void testJdbcPreparedStatement()
            throws SQLException
    {
        try (Connection conn = createConnection(server())) {
            PreparedStatement stmt = conn.prepareStatement("select * from (values ('rows1', 10), ('rows2', 10)) as t(col1, col2) where col2 = ?");
            stmt.setInt(1, 10);
            ResultSet result = stmt.executeQuery();
            result.next();
            AssertionsForClassTypes.assertThat(result.getString(1)).isEqualTo("rows1");
            AssertionsForClassTypes.assertThat(result.getInt(2)).isEqualTo(10);
            long count = 1;
            while (result.next()) {
                count++;
            }
            AssertionsForClassTypes.assertThat(count).isEqualTo(2);
        }

        try (Connection conn = createConnection(server())) {
            PreparedStatement stmt = conn.prepareStatement("select * from (values ('rows1', 10), ('rows2', 10)) as t(col1, col2)");
            ResultSet result = stmt.executeQuery();
            long count = 0;
            while (result.next()) {
                count++;
            }
            AssertionsForClassTypes.assertThat(count).isEqualTo(2);
        }

        try (Connection conn = createConnection(server())) {
            Statement stmt = conn.createStatement();
            assertThatThrownBy(() -> stmt.executeQuery("")).hasMessageFindingMatch(".*No results were returned by the query.*");

            assertThatThrownBy(() -> stmt.executeQuery("BEGIN")).hasMessageFindingMatch(".*No results were returned by the query.*");

            ResultSet result = stmt.executeQuery("select count(*) from (values ('rows1', 10), ('rows2', 10)) as t(col1, col2) ");
            AssertionsForClassTypes.assertThat(result.next()).isTrue();
            AssertionsForClassTypes.assertThat(result.getLong(1)).isEqualTo(2);

            assertThatThrownBy(() -> stmt.executeQuery("COMMIT")).hasMessageFindingMatch(".*No results were returned by the query.*");
        }
    }

    @Test
    public void testJdbcGetParamMetadata()
            throws SQLException
    {
        try (Connection conn = createConnection(server())) {
            PreparedStatement stmt = conn.prepareStatement("select * from (values ('rows1', 10), ('rows2', 10)) as t(col1, col2) where col1 = ? and col2 = ?");
            stmt.setString(1, "rows1");
            stmt.setInt(2, 10);
            ParameterMetaData metaData = stmt.getParameterMetaData();
            AssertionsForClassTypes.assertThat(metaData.getParameterCount()).isEqualTo(2);
            AssertionsForClassTypes.assertThat(metaData.getParameterType(1)).isEqualTo(Types.VARCHAR);
            AssertionsForClassTypes.assertThat(metaData.getParameterType(2)).isEqualTo(Types.INTEGER);
        }
    }

    @Test
    public void testJdbcReusePreparedStatement()
            throws SQLException
    {
        try (Connection conn = createConnection(server())) {
            PreparedStatement stmt = conn.prepareStatement("select * from (values ('rows1', 10), ('rows2', 10)) as t(col1, col2) where col1 = ? and col2 = ?");
            stmt.setString(1, "rows1");
            stmt.setInt(2, 10);
            ResultSet result = stmt.executeQuery();
            AssertionsForClassTypes.assertThat(result.next()).isTrue();
            AssertionsForClassTypes.assertThat(result.getString(1)).isEqualTo("rows1");
            AssertionsForClassTypes.assertThat(result.getInt(2)).isEqualTo(10);
            AssertionsForClassTypes.assertThat(result.next()).isFalse();

            stmt.setString(1, "rows2");
            stmt.setInt(2, 10);
            ResultSet result2 = stmt.executeQuery();
            AssertionsForClassTypes.assertThat(result2.next()).isTrue();
            AssertionsForClassTypes.assertThat(result2.getString(1)).isEqualTo("rows2");
            AssertionsForClassTypes.assertThat(result2.getInt(2)).isEqualTo(10);
            AssertionsForClassTypes.assertThat(result2.next()).isFalse();
        }
    }

    @Test
    public void testJdbcMultiPreparedStatement()
            throws SQLException
    {
        try (Connection conn = createConnection(server())) {
            PreparedStatement stmt = conn.prepareStatement("select * from (values ('rows1', 10), ('rows2', 10)) as t(col1, col2) where col1 = ? and col2 = ?");
            stmt.setString(1, "rows1");
            stmt.setInt(2, 10);
            ResultSet result = stmt.executeQuery();
            AssertionsForClassTypes.assertThat(result.next()).isTrue();
            AssertionsForClassTypes.assertThat(result.getString(1)).isEqualTo("rows1");
            AssertionsForClassTypes.assertThat(result.getInt(2)).isEqualTo(10);
            AssertionsForClassTypes.assertThat(result.next()).isFalse();

            PreparedStatement stmt2 = conn.prepareStatement("select * from (values ('rows1', 10), ('rows2', 10)) as t(col1, col2) where col1 = ? and col2 = ?");
            stmt2.setString(1, "rows2");
            stmt2.setInt(2, 10);
            ResultSet result2 = stmt2.executeQuery();
            AssertionsForClassTypes.assertThat(result2.next()).isTrue();
            AssertionsForClassTypes.assertThat(result2.getString(1)).isEqualTo("rows2");
            AssertionsForClassTypes.assertThat(result2.getInt(2)).isEqualTo(10);
            AssertionsForClassTypes.assertThat(result2.next()).isFalse();
        }
    }

    @Test
    public void testJdbcCrossExecuteDifferentStatementAndPortals()
            throws SQLException
    {
        try (Connection conn = createConnection(server())) {
            // prepare two parameters statement
            PreparedStatement stateWithTwoParams = conn.prepareStatement("select * from (values ('rows1', 10), ('rows1', 10), ('rows2', 20), ('rows2', 20)) as t(col1, col2) where col1 = ? and col2 = ?");

            // create portal1
            stateWithTwoParams.setString(1, "rows1");
            stateWithTwoParams.setInt(2, 10);
            ResultSet result = stateWithTwoParams.executeQuery();
            AssertionsForClassTypes.assertThat(result.next()).isTrue();
            AssertionsForClassTypes.assertThat(result.getString(1)).isEqualTo("rows1");
            AssertionsForClassTypes.assertThat(result.getInt(2)).isEqualTo(10);

            // prepare one parameter statement
            PreparedStatement stateWtihOneParam = conn.prepareStatement("select * from (values ('rows1', 10), ('rows2', 20)) as t(col1, col2) where col2 = ?");

            // create portal2
            stateWtihOneParam.setInt(1, 10);
            ResultSet result2 = stateWtihOneParam.executeQuery();
            AssertionsForClassTypes.assertThat(result2.next()).isTrue();
            AssertionsForClassTypes.assertThat(result2.getString(1)).isEqualTo("rows1");
            AssertionsForClassTypes.assertThat(result2.getInt(2)).isEqualTo(10);
            AssertionsForClassTypes.assertThat(result2.next()).isFalse();

            // create portal3
            stateWtihOneParam.setInt(1, 20);
            ResultSet result3 = stateWtihOneParam.executeQuery();
            AssertionsForClassTypes.assertThat(result3.next()).isTrue();
            AssertionsForClassTypes.assertThat(result3.getString(1)).isEqualTo("rows2");
            // assert it used statement 2
            AssertionsForClassTypes.assertThat(result3.getInt(2)).isEqualTo(20);
            AssertionsForClassTypes.assertThat(result3.next()).isFalse();

            // assert portal1 is available.
            AssertionsForClassTypes.assertThat(result.next()).isTrue();
            AssertionsForClassTypes.assertThat(result.getString(1)).isEqualTo("rows1");
            AssertionsForClassTypes.assertThat(result.getInt(2)).isEqualTo(10);
            AssertionsForClassTypes.assertThat(result.next()).isFalse();

            // assert statement1 available.
            // create portal4
            stateWithTwoParams.setString(1, "rows2");
            stateWithTwoParams.setInt(2, 20);
            ResultSet result4 = stateWithTwoParams.executeQuery();
            AssertionsForClassTypes.assertThat(result4.next()).isTrue();
            AssertionsForClassTypes.assertThat(result4.getString(1)).isEqualTo("rows2");
            AssertionsForClassTypes.assertThat(result4.getInt(2)).isEqualTo(20);
            AssertionsForClassTypes.assertThat(result4.next()).isTrue();
            AssertionsForClassTypes.assertThat(result4.getString(1)).isEqualTo("rows2");
            AssertionsForClassTypes.assertThat(result4.getInt(2)).isEqualTo(20);
            AssertionsForClassTypes.assertThat(result4.next()).isFalse();
        }
    }

    @Test
    public void testJdbcExecuteWithMaxRow()
            throws SQLException
    {
        try (Connection conn = createConnection(server())) {
            // prepare statement1
            PreparedStatement stmt = conn.prepareStatement("select * from (values ('rows1', 10), ('rows2', 10), ('rows3', 10)) as t(col1, col2) where col2 = ?");

            // create portal1
            stmt.setInt(1, 10);
            stmt.setMaxRows(1);
            ResultSet result = stmt.executeQuery();
            AssertionsForClassTypes.assertThat(result.next()).isTrue();
            AssertionsForClassTypes.assertThat(result.getString(1)).isEqualTo("rows1");
            AssertionsForClassTypes.assertThat(result.next()).isFalse();
        }
    }

    @Test
    public void testJdbcMultiExecuteWithMaxRow()
            throws SQLException
    {
        try (Connection conn = createConnection(server())) {
            // prepare statement1
            PreparedStatement stmt = conn.prepareStatement("select * from (values ('rows1', 10), ('rows2', 10), ('rows3', 10)) as t(col1, col2) where col2 = ?");
            // create portal1
            stmt.setInt(1, 10);
            stmt.setMaxRows(1);
            ResultSet result = stmt.executeQuery();
            AssertionsForClassTypes.assertThat(result.next()).isTrue();
            AssertionsForClassTypes.assertThat(result.getString(1)).isEqualTo("rows1");
            AssertionsForClassTypes.assertThat(result.next()).isFalse();
            PreparedStatement stmt2 = conn.prepareStatement("select * from (values ('rows1', 10), ('rows2', 10), ('rows3', 10)) as t(col1, col2) where col2 = ?");
            stmt2.setInt(1, 10);
            ResultSet result2 = stmt2.executeQuery();
            AssertionsForClassTypes.assertThat(result2.next()).isTrue();
            AssertionsForClassTypes.assertThat(result2.getString(1)).isEqualTo("rows1");
            AssertionsForClassTypes.assertThat(result2.next()).isTrue();
            AssertionsForClassTypes.assertThat(result2.getString(1)).isEqualTo("rows2");
            AssertionsForClassTypes.assertThat(result2.next()).isTrue();
            AssertionsForClassTypes.assertThat(result2.getString(1)).isEqualTo("rows3");
            AssertionsForClassTypes.assertThat(result.next()).isFalse();
        }
    }
}
