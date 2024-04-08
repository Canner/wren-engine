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

package io.wren.testing.postgres;

import com.google.common.collect.ImmutableList;
import io.wren.base.type.PGType;
import io.wren.base.type.PGTypes;
import io.wren.main.wireprotocol.PostgresWireProtocol;
import io.wren.testing.TestingWireProtocolClient;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.wren.base.type.IntegerType.INTEGER;
import static io.wren.base.type.VarcharType.VARCHAR;
import static io.wren.testing.TestingWireProtocolClient.DescribeType.PORTAL;
import static io.wren.testing.TestingWireProtocolClient.DescribeType.STATEMENT;
import static io.wren.testing.TestingWireProtocolClient.Parameter.textParameter;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestWireProtocolWithPostgres
        extends AbstractWireProtocolTestWithPostgres
{
    @Test
    public void testSimpleQuery()
            throws IOException
    {
        try (TestingWireProtocolClient protocolClient = wireProtocolClient()) {
            protocolClient.sendStartUpMessage(196608, MOCK_PASSWORD, "test", "canner");

            protocolClient.assertAuthOk();
            assertDefaultPgConfigResponse(protocolClient);
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery("select * from (values ('rows1', 10), ('rows2', 10), ('rows3', 10)) as t(col1, col2) where col2 = 10");

            List<TestingWireProtocolClient.Field> fields = protocolClient.assertAndGetRowDescriptionFields();
            List<PGType<?>> types = fields.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(Collectors.toList());
            assertThat(types).isEqualTo(ImmutableList.of(VARCHAR, INTEGER));

            protocolClient.assertDataRow("rows1,10");
            protocolClient.assertDataRow("rows2,10");
            protocolClient.assertDataRow("rows3,10");
            protocolClient.assertCommandComplete("SELECT 3");
            protocolClient.assertReadyForQuery('I');
        }
    }

    @Test
    public void testExtendedQuery()
            throws IOException
    {
        try (TestingWireProtocolClient protocolClient = wireProtocolClient()) {
            protocolClient.sendStartUpMessage(196608, MOCK_PASSWORD, "test", "canner");
            protocolClient.assertAuthOk();
            assertDefaultPgConfigResponse(protocolClient);
            protocolClient.assertReadyForQuery('I');

            List<PGType> paramTypes = ImmutableList.of(INTEGER);
            protocolClient.sendParse("teststmt", "select * from (values ('rows1', 10), ('rows2', 10)) as t(col1, col2) where col2 = ?",
                    paramTypes.stream().map(PGType::oid).collect(toImmutableList()));
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.STATEMENT, "teststmt");
            protocolClient.sendBind("exec1", "teststmt", ImmutableList.of(textParameter(10, INTEGER)));
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.PORTAL, "exec1");
            protocolClient.sendExecute("exec1", 0);
            protocolClient.sendSync();

            protocolClient.assertParseComplete();

            List<PGType<?>> actualParamTypes = protocolClient.assertAndGetParameterDescription();
            assertThat(actualParamTypes).isEqualTo(paramTypes);

            List<TestingWireProtocolClient.Field> fields = protocolClient.assertAndGetRowDescriptionFields();
            List<PGType> actualTypes = fields.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
            assertThat(actualTypes).isEqualTo(ImmutableList.of(VARCHAR, INTEGER));

            protocolClient.assertBindComplete();

            List<TestingWireProtocolClient.Field> fields2 = protocolClient.assertAndGetRowDescriptionFields();
            List<PGType> actualTypes2 = fields2.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
            assertThat(actualTypes2).isEqualTo(ImmutableList.of(VARCHAR, INTEGER));

            protocolClient.assertDataRow("rows1,10");
            protocolClient.assertDataRow("rows2,10");
            protocolClient.assertCommandComplete("SELECT 2");
            protocolClient.assertReadyForQuery('I');
        }
    }

    @Test
    public void testNullExtendedQuery()
            throws IOException
    {
        try (TestingWireProtocolClient protocolClient = wireProtocolClient()) {
            protocolClient.sendStartUpMessage(196608, MOCK_PASSWORD, "test", "canner");
            protocolClient.assertAuthOk();
            assertDefaultPgConfigResponse(protocolClient);
            protocolClient.assertReadyForQuery('I');
            protocolClient.sendNullParse("");
            protocolClient.sendSync();
            protocolClient.assertErrorMessage("query can't be null");
            protocolClient.assertReadyForQuery('I');
        }
    }

    @Test
    public void testNotExistOid()
            throws IOException
    {
        try (TestingWireProtocolClient protocolClient = wireProtocolClient()) {
            protocolClient.sendStartUpMessage(196608, MOCK_PASSWORD, "test", "canner");
            protocolClient.assertAuthOk();
            assertDefaultPgConfigResponse(protocolClient);
            protocolClient.assertReadyForQuery('I');
            protocolClient.sendParse("teststmt", "select * from (values ('rows1', 10), ('rows2', 20)) as t(col1, col2) where col2 = ?",
                    ImmutableList.of(999));
            protocolClient.sendSync();
            protocolClient.assertErrorMessage("No oid mapping from '999' to pg_type");
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendBind("exec1", "teststmt", ImmutableList.of(textParameter("10", INTEGER)));
            protocolClient.sendSync();
            protocolClient.assertErrorMessage("prepared statement teststmt not found");
            protocolClient.assertReadyForQuery('I');
        }
    }

    @Test
    public void testDescribeEmptyStatement()
            throws IOException
    {
        try (TestingWireProtocolClient protocolClient = wireProtocolClient()) {
            protocolClient.sendStartUpMessage(196608, MOCK_PASSWORD, "test", "canner");
            protocolClient.assertAuthOk();
            assertDefaultPgConfigResponse(protocolClient);
            protocolClient.assertReadyForQuery('I');
            protocolClient.sendParse("teststmt", "", ImmutableList.of());
            protocolClient.sendDescribe(STATEMENT, "teststmt");
            protocolClient.sendBind("exec1", "teststmt", ImmutableList.of());
            protocolClient.sendDescribe(PORTAL, "exec1");
            protocolClient.sendSync();

            protocolClient.assertParseComplete();
            List<PGType<?>> fields = protocolClient.assertAndGetParameterDescription();
            assertThat(fields.size()).isZero();
            protocolClient.assertNoData();

            protocolClient.assertBindComplete();
            protocolClient.assertNoData();

            protocolClient.assertReadyForQuery('I');
        }
    }

    @Test
    public void testExtendedQueryWithMaxRow()
            throws IOException
    {
        try (TestingWireProtocolClient protocolClient = wireProtocolClient()) {
            protocolClient.sendStartUpMessage(196608, MOCK_PASSWORD, "test", "canner");

            protocolClient.assertAuthOk();
            assertDefaultPgConfigResponse(protocolClient);
            protocolClient.assertReadyForQuery('I');
            List<PGType<?>> paramTypes = ImmutableList.of(INTEGER);
            protocolClient.sendParse("teststmt", "select * from (values ('rows1', 10), ('rows2', 10)) as t(col1, col2) where col2 = ?",
                    paramTypes.stream().map(PGType::oid).collect(toImmutableList()));
            protocolClient.sendBind("exec1", "teststmt", ImmutableList.of(textParameter(10, INTEGER)));
            protocolClient.sendDescribe(PORTAL, "exec1");
            protocolClient.sendExecute("exec1", 1);
            protocolClient.sendSync();

            protocolClient.assertParseComplete();
            protocolClient.assertBindComplete();

            List<TestingWireProtocolClient.Field> fields = protocolClient.assertAndGetRowDescriptionFields();
            List<PGType<?>> actualTypes = fields.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
            assertThat(actualTypes).isEqualTo(ImmutableList.of(VARCHAR, INTEGER));

            protocolClient.assertDataRow("rows1,10");
            protocolClient.assertPortalPortalSuspended();
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendExecute("exec1", 1);
            protocolClient.sendSync();

            protocolClient.assertDataRow("rows2,10");
            protocolClient.assertCommandComplete("SELECT 2");
            protocolClient.assertReadyForQuery('I');
        }
    }

    @Test
    public void testCloseExtendedQuery()
            throws IOException
    {
        try (TestingWireProtocolClient protocolClient = wireProtocolClient()) {
            protocolClient.sendStartUpMessage(196608, MOCK_PASSWORD, "test", "canner");
            protocolClient.assertAuthOk();
            assertDefaultPgConfigResponse(protocolClient);
            protocolClient.assertReadyForQuery('I');
            List<PGType<?>> paramTypes = ImmutableList.of(INTEGER);
            protocolClient.sendParse("teststmt", "select * from (values ('rows1', 10), ('rows2', 10)) as t(col1, col2) where col2 = ?",
                    paramTypes.stream().map(PGType::oid).collect(toImmutableList()));
            protocolClient.sendBind("exec1", "teststmt", ImmutableList.of(textParameter(10, INTEGER)));
            protocolClient.sendDescribe(PORTAL, "exec1");
            protocolClient.sendExecute("exec1", 1);
            protocolClient.sendSync();

            protocolClient.assertParseComplete();
            protocolClient.assertBindComplete();

            List<TestingWireProtocolClient.Field> fields = protocolClient.assertAndGetRowDescriptionFields();
            List<PGType<?>> actualTypes = fields.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
            assertThat(actualTypes).isEqualTo(ImmutableList.of(VARCHAR, INTEGER));

            protocolClient.assertDataRow("rows1,10");
            protocolClient.assertPortalPortalSuspended();
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendClose(TestingWireProtocolClient.DescribeType.PORTAL, "exec1");
            protocolClient.sendExecute("exec1", 1);
            protocolClient.sendClose('S', "teststmt");
            protocolClient.sendBind("exec1", "teststmt", ImmutableList.of(textParameter(10, INTEGER)));
            protocolClient.sendSync();

            protocolClient.assertCloseComplete();
            protocolClient.assertErrorMessage(".*portal exec1 not found.*");
            protocolClient.assertCloseComplete();
            protocolClient.assertErrorMessage(".*prepared statement teststmt not found.*");
            protocolClient.assertReadyForQuery('I');
        }
    }

    @Test
    public void testMultiQueryInOneConnection()
            throws IOException
    {
        try (TestingWireProtocolClient protocolClient = wireProtocolClient()) {
            protocolClient.sendStartUpMessage(196608, MOCK_PASSWORD, "test", "canner");
            protocolClient.assertAuthOk();
            assertDefaultPgConfigResponse(protocolClient);
            protocolClient.assertReadyForQuery('I');
            List<PGType<?>> paramTypes = ImmutableList.of(INTEGER);
            protocolClient.sendParse("", "select col1 from (values ('rows1', 1), ('rows2', 2), ('rows3', 3)) as t(col1, col2) where col2 = ?",
                    paramTypes.stream().map(PGType::oid).collect(toImmutableList()));
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.STATEMENT, "");
            protocolClient.sendBind("", "", ImmutableList.of(textParameter(1, INTEGER)));
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.PORTAL, "");
            protocolClient.sendExecute("", 0);

            protocolClient.sendParse("", "select col1 from (values ('rows1', 1), ('rows2', 2), ('rows3', 3)) as t(col1, col2) where col2 = ?",
                    paramTypes.stream().map(PGType::oid).collect(toImmutableList()));
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.STATEMENT, "");
            protocolClient.sendBind("", "", ImmutableList.of(textParameter(2, INTEGER)));
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.PORTAL, "");
            protocolClient.sendExecute("", 0);

            protocolClient.sendParse("", "select col1 from (values ('rows1', 1), ('rows2', 2), ('rows3', 3)) as t(col1, col2) where col2 = ?",
                    paramTypes.stream().map(PGType::oid).collect(toImmutableList()));
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.STATEMENT, "");
            protocolClient.sendBind("", "", ImmutableList.of(textParameter(3, INTEGER)));
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.PORTAL, "");
            protocolClient.sendExecute("", 0);
            protocolClient.sendSync();
            assertResponse(protocolClient, "rows1");
            assertResponse(protocolClient, "rows2");
            assertResponse(protocolClient, "rows3");
            protocolClient.assertReadyForQuery('I');
        }
    }

    private void assertResponse(TestingWireProtocolClient protocolClient, String expected)
            throws IOException
    {
        List<PGType<?>> paramTypes = ImmutableList.of(INTEGER);
        protocolClient.assertParseComplete();
        List<PGType<?>> actualParamTypes = protocolClient.assertAndGetParameterDescription();
        assertThat(actualParamTypes).isEqualTo(paramTypes);
        List<TestingWireProtocolClient.Field> fields = protocolClient.assertAndGetRowDescriptionFields();
        List<PGType<?>> actualTypes = fields.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
        assertThat(actualTypes).isEqualTo(ImmutableList.of(VARCHAR));
        protocolClient.assertBindComplete();
        List<TestingWireProtocolClient.Field> fields2 = protocolClient.assertAndGetRowDescriptionFields();
        List<PGType<?>> actualTypes2 = fields2.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
        assertThat(actualTypes2).isEqualTo(ImmutableList.of(VARCHAR));
        protocolClient.assertDataRow(expected);
        protocolClient.assertCommandComplete("SELECT 1");
    }

    @Test
    public void testPreparedNameIsPreservedWord()
            throws IOException
    {
        try (TestingWireProtocolClient protocolClient = wireProtocolClient()) {
            protocolClient.sendStartUpMessage(196608, MOCK_PASSWORD, "test", "canner");
            protocolClient.assertAuthOk();
            assertDefaultPgConfigResponse(protocolClient);
            protocolClient.assertReadyForQuery('I');

            String preparedName = "all";
            List<PGType<?>> paramTypes = ImmutableList.of(INTEGER);
            protocolClient.sendParse(preparedName, "select * from (values ('rows1', 10), ('rows2', 10)) as t(col1, col2) where col2 = ?",
                    paramTypes.stream().map(PGType::oid).collect(toImmutableList()));
            protocolClient.sendSync();

            protocolClient.assertErrorMessage(".*all is a preserved word. Can't be the name of prepared statement.*");
            protocolClient.assertReadyForQuery('I');

            preparedName = "ALL";
            protocolClient.sendParse(preparedName, "select * from (values ('rows1', 10), ('rows2', 10)) as t(col1, col2) where col2 = ?",
                    paramTypes.stream().map(PGType::oid).collect(toImmutableList()));
            protocolClient.sendSync();

            protocolClient.assertErrorMessage(".*ALL is a preserved word. Can't be the name of prepared statement.*");
            protocolClient.assertReadyForQuery('I');
        }
    }

    @DataProvider
    public Object[][] statementNameWithSpecialCharacters()
    {
        return new Object[][] {
                {"test-stmt"},
                {"test*stmt"},
                {"test~stmt"},
                {"test+stmt"},
                {"test%stmt"},
                {"test^stmt"},
                {"\"teststmt\""},
                {"'teststmt'"},
        };
    }

    @Test(dataProvider = "statementNameWithSpecialCharacters")
    public void testStatementNameWithHyphens(String statementName)
            throws IOException
    {
        try (TestingWireProtocolClient protocolClient = wireProtocolClient()) {
            protocolClient.sendStartUpMessage(196608, MOCK_PASSWORD, "test", "canner");
            protocolClient.assertAuthOk();

            assertDefaultPgConfigResponse(protocolClient);
            protocolClient.assertReadyForQuery('I');

            List<PGType<?>> paramTypes = ImmutableList.of(INTEGER);
            protocolClient.sendParse(statementName, "select col1 from (values ('rows1', 1), ('rows2', 2), ('rows3', 3)) as t(col1, col2) where col2 = ?",
                    paramTypes.stream().map(PGType::oid).collect(toImmutableList()));
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.STATEMENT, statementName);
            protocolClient.sendBind("exec1", statementName, ImmutableList.of(textParameter(1, INTEGER)));
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.PORTAL, "exec1");
            protocolClient.sendExecute("exec1", 0);
            protocolClient.sendSync();

            protocolClient.assertParseComplete();
            List<PGType<?>> actualParamTypes = protocolClient.assertAndGetParameterDescription();
            assertThat(actualParamTypes).isEqualTo(paramTypes);
            assertFields(protocolClient, ImmutableList.of(VARCHAR));
            protocolClient.assertBindComplete();
            assertFields(protocolClient, ImmutableList.of(VARCHAR));
            protocolClient.assertDataRow("rows1");
            protocolClient.assertCommandComplete("SELECT 1");
            protocolClient.assertReadyForQuery('I');
        }
    }

    private static void assertFields(TestingWireProtocolClient client, List<PGType<?>> types)
            throws IOException
    {
        List<TestingWireProtocolClient.Field> fields = client.assertAndGetRowDescriptionFields();
        List<PGType<?>> actualTypes = fields.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
        assertThat(actualTypes).isEqualTo(types);
    }

    @Test
    public void testEmptyStatement()
            throws IOException
    {
        try (TestingWireProtocolClient protocolClient = wireProtocolClient()) {
            protocolClient.sendStartUpMessage(196608, MOCK_PASSWORD, "test", "canner");

            protocolClient.assertAuthOk();
            assertDefaultPgConfigResponse(protocolClient);
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery("");
            protocolClient.assertEmptyResponse();
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery(" ");
            protocolClient.assertEmptyResponse();
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery(";");
            protocolClient.assertEmptyResponse();
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery(" ;");
            protocolClient.assertEmptyResponse();
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendParse("teststmt", " ;", ImmutableList.of());
            protocolClient.sendBind("exec", "teststmt", ImmutableList.of());
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.PORTAL, "exec");
            protocolClient.sendExecute("exec", 0);
            protocolClient.sendSync();

            protocolClient.assertParseComplete();
            protocolClient.assertBindComplete();
            protocolClient.assertNoData();
            protocolClient.assertEmptyResponse();
            protocolClient.assertReadyForQuery('I');
        }
    }

    @Test
    public void testIgnoreCommands()
            throws IOException
    {
        try (TestingWireProtocolClient protocolClient = wireProtocolClient()) {
            protocolClient.sendStartUpMessage(196608, MOCK_PASSWORD, "test", "canner");
            protocolClient.assertAuthOk();
            assertDefaultPgConfigResponse(protocolClient);
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery("BEGIN;");
            protocolClient.assertCommandComplete("BEGIN");
            // we does not support transactions now, so BEGIN state will be 'I' if idle (not in a transaction block)
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery(" BEGIN;");
            protocolClient.assertCommandComplete("BEGIN");
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery("BEGIN  ;");
            protocolClient.assertCommandComplete("BEGIN");
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery("begin;");
            protocolClient.assertCommandComplete("BEGIN");
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery("BEGIN WORK;");
            protocolClient.assertCommandComplete("BEGIN");
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery("COMMIT;");
            protocolClient.assertCommandComplete("COMMIT");
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery("commit;");
            protocolClient.assertCommandComplete("COMMIT");
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery("COMMIT TRANSACTION;");
            protocolClient.assertCommandComplete("COMMIT");
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery("BEGIN; select * from (values (1, 2, 3), (2, 4, 6)) t1(c1, c2, c3);");
            protocolClient.assertCommandComplete("BEGIN");
            List<TestingWireProtocolClient.Field> fields = protocolClient.assertAndGetRowDescriptionFields();
            List<PGType<?>> types = fields.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(Collectors.toList());
            assertThat(types).isEqualTo(ImmutableList.of(INTEGER, INTEGER, INTEGER));
            protocolClient.assertDataRow("1,2,3");
            protocolClient.assertDataRow("2,4,6");
            protocolClient.assertCommandComplete("SELECT 2");
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery("DISCARD ALL;DISCARD PLANS;DISCARD SEQUENCES;DISCARD TEMPORARY;DISCARD TEMP;");
            protocolClient.assertCommandComplete("DISCARD");
            protocolClient.assertCommandComplete("DISCARD");
            protocolClient.assertCommandComplete("DISCARD");
            protocolClient.assertCommandComplete("DISCARD");
            protocolClient.assertCommandComplete("DISCARD");
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery("SET SESSION AUTHORIZATION DEFAULT;SET SESSION AUTHORIZATION canner");
            protocolClient.assertCommandComplete("SET");
            protocolClient.assertCommandComplete("SET");
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery("UNLISTEN *");
            protocolClient.assertCommandComplete("UNLISTEN");
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery("RESET ALL");
            protocolClient.assertCommandComplete("RESET");
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery("CLOSE ALL");
            protocolClient.assertCommandComplete("CLOSE");
            protocolClient.assertReadyForQuery('I');

            sendSimpleQueryInExtendQueryMode(protocolClient, "BEGIN;");
            protocolClient.assertParseComplete();
            protocolClient.assertBindComplete();
            protocolClient.assertNoData();
            protocolClient.assertCommandComplete("BEGIN");
            protocolClient.assertReadyForQuery('I');

            sendSimpleQueryInExtendQueryMode(protocolClient, "COMMIT;");
            protocolClient.assertParseComplete();
            protocolClient.assertBindComplete();
            protocolClient.assertNoData();
            protocolClient.assertCommandComplete("COMMIT");
            protocolClient.assertReadyForQuery('I');

            sendSimpleQueryInExtendQueryMode(protocolClient, " COMMIT  ;");
            protocolClient.assertParseComplete();
            protocolClient.assertBindComplete();
            protocolClient.assertNoData();
            protocolClient.assertCommandComplete("COMMIT");
            protocolClient.assertReadyForQuery('I');

            sendSimpleQueryInExtendQueryMode(protocolClient, "DISCARD ALL");
            protocolClient.assertParseComplete();
            protocolClient.assertBindComplete();
            protocolClient.assertNoData();
            protocolClient.assertCommandComplete("DISCARD");
            protocolClient.assertReadyForQuery('I');

            sendSimpleQueryInExtendQueryMode(protocolClient, "CLOSE ALL");
            protocolClient.assertParseComplete();
            protocolClient.assertBindComplete();
            protocolClient.assertNoData();
            protocolClient.assertCommandComplete("CLOSE");
            protocolClient.assertReadyForQuery('I');

            sendSimpleQueryInExtendQueryMode(protocolClient, "RESET ALL");
            protocolClient.assertParseComplete();
            protocolClient.assertBindComplete();
            protocolClient.assertNoData();
            protocolClient.assertCommandComplete("RESET");
            protocolClient.assertReadyForQuery('I');

            sendSimpleQueryInExtendQueryMode(protocolClient, "UNLISTEN *");
            protocolClient.assertParseComplete();
            protocolClient.assertBindComplete();
            protocolClient.assertNoData();
            protocolClient.assertCommandComplete("UNLISTEN");
            protocolClient.assertReadyForQuery('I');

            sendSimpleQueryInExtendQueryMode(protocolClient, "SET SESSION AUTHORIZATION DEFAULT");
            protocolClient.assertParseComplete();
            protocolClient.assertBindComplete();
            protocolClient.assertNoData();
            protocolClient.assertCommandComplete("SET");
            protocolClient.assertReadyForQuery('I');
        }
    }

    private void sendSimpleQueryInExtendQueryMode(TestingWireProtocolClient protocolClient, @Language("SQL") String sql)
            throws IOException
    {
        protocolClient.sendParse("", sql, ImmutableList.of());
        protocolClient.sendBind("", "", ImmutableList.of());
        protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.PORTAL, "");
        protocolClient.sendExecute("", 0);
        protocolClient.sendSync();
    }

    @Test
    public void testSetSession()
            throws IOException
    {
        try (TestingWireProtocolClient protocolClient = wireProtocolClient()) {
            protocolClient.sendStartUpMessage(196608, MOCK_PASSWORD, "test", "canner");
            protocolClient.assertAuthOk();
            assertDefaultPgConfigResponse(protocolClient);
            protocolClient.assertReadyForQuery('I');

            // ignore property
            protocolClient.sendSimpleQuery("set data_directory = '/tmp'");
            protocolClient.assertCommandComplete("SET");
            protocolClient.assertReadyForQuery('I');

            // system property
            protocolClient.sendSimpleQuery("set extra_float_digits = 2");
            protocolClient.assertCommandComplete("SET");
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery("set DateStyle = 'ISO'");
            protocolClient.assertParameterStatus("DateStyle", "ISO");
            protocolClient.assertCommandComplete("SET");
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery("set DateStyle = 'ISO', 'DMY'");
            protocolClient.assertParameterStatus("DateStyle", "ISO, DMY");
            protocolClient.assertCommandComplete("SET");
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery("set DateStyle = ISO, DMY");
            protocolClient.assertParameterStatus("DateStyle", "ISO, DMY");
            protocolClient.assertCommandComplete("SET");
            protocolClient.assertReadyForQuery('I');

            // hard-weird property
            protocolClient.sendSimpleQuery("SET application_name = 'PostgreSQL JDBC Driver'");
            protocolClient.assertParameterStatus("application_name", "PostgreSQL JDBC Driver");
            protocolClient.assertCommandComplete("SET");
            protocolClient.assertReadyForQuery('I');

            // ignore property
            protocolClient.sendParse("teststmt", "set data_directory = '/tmp'", ImmutableList.of());
            protocolClient.sendBind("exec", "teststmt", ImmutableList.of());
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.PORTAL, "exec");
            protocolClient.sendExecute("exec", 0);
            protocolClient.sendSync();

            protocolClient.assertParseComplete();
            protocolClient.assertBindComplete();
            protocolClient.assertNoData();
            protocolClient.assertCommandComplete("SET");
            protocolClient.assertReadyForQuery('I');

            // system property
            protocolClient.sendParse("teststmt2", "set extra_float_digits = 2", ImmutableList.of());
            protocolClient.sendBind("exec2", "teststmt2", ImmutableList.of());
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.PORTAL, "exec2");
            protocolClient.sendExecute("exec2", 0);
            protocolClient.sendSync();

            protocolClient.assertParseComplete();
            protocolClient.assertBindComplete();
            protocolClient.assertNoData();
            protocolClient.assertCommandComplete("SET");
            protocolClient.assertReadyForQuery('I');

            // hard-weird property
            protocolClient.sendParse("teststmt4", "SET application_name = 'PostgreSQL JDBC Driver'", ImmutableList.of());
            protocolClient.sendBind("exec4", "teststmt4", ImmutableList.of());
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.PORTAL, "exec4");
            protocolClient.sendExecute("exec4", 0);
            protocolClient.sendSync();

            protocolClient.assertParseComplete();
            protocolClient.assertBindComplete();
            protocolClient.assertNoData();
            protocolClient.assertParameterStatus("application_name", "PostgreSQL JDBC Driver");
            protocolClient.assertCommandComplete("SET");
            protocolClient.assertReadyForQuery('I');

            // hard-weird property
            protocolClient.sendSimpleQuery("SET TimeZone = 'UTC'");
            protocolClient.assertParameterStatus("TimeZone", "UTC");
            protocolClient.assertCommandComplete("SET");
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery("SET timezone = 'UTC'");
            protocolClient.assertParameterStatus("timezone", "UTC");
            protocolClient.assertCommandComplete("SET");
            protocolClient.assertReadyForQuery('I');
        }
    }

    @Test
    public void testJdbcConnection()
            throws Exception
    {
        try (Connection conn = createConnection()) {
            Statement stmt = conn.createStatement();
            stmt.execute("select * from (values ('rows1', 10), ('rows2', 10), ('rows3', 10)) as t(col1, col2) where col2 = 10");
            ResultSet result = stmt.getResultSet();
            long count = 0;
            while (result.next()) {
                count++;
            }

            assertThat(count).isEqualTo(3);
        }
    }

    @Test
    public void testJdbcPreparedStatement()
            throws SQLException
    {
        try (Connection conn = createConnection()) {
            PreparedStatement stmt = conn.prepareStatement("select * from (values ('rows1', 10), ('rows2', 10)) as t(col1, col2) where col2 = ?");
            stmt.setInt(1, 10);
            ResultSet result = stmt.executeQuery();
            result.next();
            assertThat(result.getString(1)).isEqualTo("rows1");
            assertThat(result.getInt(2)).isEqualTo(10);
            long count = 1;
            while (result.next()) {
                count++;
            }
            assertThat(count).isEqualTo(2);
        }

        try (Connection conn = createConnection()) {
            PreparedStatement stmt = conn.prepareStatement("select * from (values ('rows1', 10), ('rows2', 10)) as t(col1, col2)");
            ResultSet result = stmt.executeQuery();
            long count = 0;
            while (result.next()) {
                count++;
            }
            assertThat(count).isEqualTo(2);
        }

        try (Connection conn = createConnection()) {
            Statement stmt = conn.createStatement();
            assertThatThrownBy(() -> stmt.executeQuery("")).hasMessageFindingMatch(".*No results were returned by the query.*");

            assertThatThrownBy(() -> stmt.executeQuery("BEGIN")).hasMessageFindingMatch(".*No results were returned by the query.*");

            ResultSet result = stmt.executeQuery("select count(*) from (values ('rows1', 10), ('rows2', 10)) as t(col1, col2) ");
            assertThat(result.next()).isTrue();
            assertThat(result.getLong(1)).isEqualTo(2);

            assertThatThrownBy(() -> stmt.executeQuery("COMMIT")).hasMessageFindingMatch(".*No results were returned by the query.*");
        }
    }

    @Test
    public void testJdbcGetParamMetadata()
            throws SQLException
    {
        try (Connection conn = createConnection()) {
            PreparedStatement stmt = conn.prepareStatement("select * from (values ('rows1', 10), ('rows2', 10)) as t(col1, col2) where col1 = ? and col2 = ?");
            stmt.setString(1, "rows1");
            stmt.setInt(2, 10);
            ParameterMetaData metaData = stmt.getParameterMetaData();
            assertThat(metaData.getParameterCount()).isEqualTo(2);
            assertThat(metaData.getParameterType(1)).isEqualTo(Types.VARCHAR);
            assertThat(metaData.getParameterType(2)).isEqualTo(Types.INTEGER);
        }
    }

    @Test
    public void testJdbcReusePreparedStatement()
            throws SQLException
    {
        try (Connection conn = createConnection()) {
            PreparedStatement stmt = conn.prepareStatement("select * from (values ('rows1', 10), ('rows2', 10)) as t(col1, col2) where col1 = ? and col2 = ?");
            stmt.setString(1, "rows1");
            stmt.setInt(2, 10);
            ResultSet result = stmt.executeQuery();
            assertThat(result.next()).isTrue();
            assertThat(result.getString(1)).isEqualTo("rows1");
            assertThat(result.getInt(2)).isEqualTo(10);
            assertThat(result.next()).isFalse();

            stmt.setString(1, "rows2");
            stmt.setInt(2, 10);
            ResultSet result2 = stmt.executeQuery();
            assertThat(result2.next()).isTrue();
            assertThat(result2.getString(1)).isEqualTo("rows2");
            assertThat(result2.getInt(2)).isEqualTo(10);
            assertThat(result2.next()).isFalse();
        }
    }

    @Test
    public void testJdbcMultiPreparedStatement()
            throws SQLException
    {
        try (Connection conn = createConnection()) {
            PreparedStatement stmt = conn.prepareStatement("select * from (values ('rows1', 10), ('rows2', 10)) as t(col1, col2) where col1 = ? and col2 = ?");
            stmt.setString(1, "rows1");
            stmt.setInt(2, 10);
            ResultSet result = stmt.executeQuery();
            assertThat(result.next()).isTrue();
            assertThat(result.getString(1)).isEqualTo("rows1");
            assertThat(result.getInt(2)).isEqualTo(10);
            assertThat(result.next()).isFalse();

            PreparedStatement stmt2 = conn.prepareStatement("select * from (values ('rows1', 10), ('rows2', 10)) as t(col1, col2) where col1 = ? and col2 = ?");
            stmt2.setString(1, "rows2");
            stmt2.setInt(2, 10);
            ResultSet result2 = stmt2.executeQuery();
            assertThat(result2.next()).isTrue();
            assertThat(result2.getString(1)).isEqualTo("rows2");
            assertThat(result2.getInt(2)).isEqualTo(10);
            assertThat(result2.next()).isFalse();
        }
    }

    @Test
    public void testJdbcCrossExecuteDifferentStatementAndPortals()
            throws SQLException
    {
        try (Connection conn = createConnection()) {
            // prepare two parameters statement
            PreparedStatement stateWithTwoParams = conn.prepareStatement("select * from (values ('rows1', 10), ('rows1', 10), ('rows2', 20), ('rows2', 20)) as t(col1, col2) where col1 = ? and col2 = ?");

            // create portal1
            stateWithTwoParams.setString(1, "rows1");
            stateWithTwoParams.setInt(2, 10);
            ResultSet result = stateWithTwoParams.executeQuery();
            assertThat(result.next()).isTrue();
            assertThat(result.getString(1)).isEqualTo("rows1");
            assertThat(result.getInt(2)).isEqualTo(10);

            // prepare one parameter statement
            PreparedStatement stateWtihOneParam = conn.prepareStatement("select * from (values ('rows1', 10), ('rows2', 20)) as t(col1, col2) where col2 = ?");

            // create portal2
            stateWtihOneParam.setInt(1, 10);
            ResultSet result2 = stateWtihOneParam.executeQuery();
            assertThat(result2.next()).isTrue();
            assertThat(result2.getString(1)).isEqualTo("rows1");
            assertThat(result2.getInt(2)).isEqualTo(10);
            assertThat(result2.next()).isFalse();

            // create portal3
            stateWtihOneParam.setInt(1, 20);
            ResultSet result3 = stateWtihOneParam.executeQuery();
            assertThat(result3.next()).isTrue();
            assertThat(result3.getString(1)).isEqualTo("rows2");
            // assert it used statement 2
            assertThat(result3.getInt(2)).isEqualTo(20);
            assertThat(result3.next()).isFalse();

            // assert portal1 is available.
            assertThat(result.next()).isTrue();
            assertThat(result.getString(1)).isEqualTo("rows1");
            assertThat(result.getInt(2)).isEqualTo(10);
            assertThat(result.next()).isFalse();

            // assert statement1 available.
            // create portal4
            stateWithTwoParams.setString(1, "rows2");
            stateWithTwoParams.setInt(2, 20);
            ResultSet result4 = stateWithTwoParams.executeQuery();
            assertThat(result4.next()).isTrue();
            assertThat(result4.getString(1)).isEqualTo("rows2");
            assertThat(result4.getInt(2)).isEqualTo(20);
            assertThat(result4.next()).isTrue();
            assertThat(result4.getString(1)).isEqualTo("rows2");
            assertThat(result4.getInt(2)).isEqualTo(20);
            assertThat(result4.next()).isFalse();
        }
    }

    @Test
    public void testJdbcExecuteWithMaxRow()
            throws SQLException
    {
        try (Connection conn = createConnection()) {
            // prepare statement1
            PreparedStatement stmt = conn.prepareStatement("select * from (values ('rows1', 10), ('rows2', 10), ('rows3', 10)) as t(col1, col2) where col2 = ?");

            // create portal1
            stmt.setInt(1, 10);
            stmt.setMaxRows(1);
            ResultSet result = stmt.executeQuery();
            assertThat(result.next()).isTrue();
            assertThat(result.getString(1)).isEqualTo("rows1");
            assertThat(result.next()).isFalse();
        }
    }

    @Test
    public void testJdbcMultiExecuteWithMaxRow()
            throws SQLException
    {
        try (Connection conn = createConnection()) {
            // prepare statement1
            PreparedStatement stmt = conn.prepareStatement("select * from (values ('rows1', 10), ('rows2', 10), ('rows3', 10)) as t(col1, col2) where col2 = ?");
            // create portal1
            stmt.setInt(1, 10);
            stmt.setMaxRows(1);
            ResultSet result = stmt.executeQuery();
            assertThat(result.next()).isTrue();
            assertThat(result.getString(1)).isEqualTo("rows1");
            assertThat(result.next()).isFalse();
            PreparedStatement stmt2 = conn.prepareStatement("select * from (values ('rows1', 10), ('rows2', 10), ('rows3', 10)) as t(col1, col2) where col2 = ?");
            stmt2.setInt(1, 10);
            ResultSet result2 = stmt2.executeQuery();
            assertThat(result2.next()).isTrue();
            assertThat(result2.getString(1)).isEqualTo("rows1");
            assertThat(result2.next()).isTrue();
            assertThat(result2.getString(1)).isEqualTo("rows2");
            assertThat(result2.next()).isTrue();
            assertThat(result2.getString(1)).isEqualTo("rows3");
            assertThat(result.next()).isFalse();
        }
    }

    @DataProvider
    public Object[][] jdbcQuery()
    {
        return new Object[][] {
                {"SHOW TRANSACTION ISOLATION LEVEL"},
                {"SELECT t.typlen FROM pg_catalog.pg_type t, pg_catalog.pg_namespace n WHERE t.typnamespace=n.oid AND t.typname='name' AND n.nspname='pg_catalog'"},
                {"SELECT\n" +
                        "  t.typname\n" +
                        ", t.oid\n" +
                        "FROM\n" +
                        "  (pg_catalog.pg_type t\n" +
                        "INNER JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid))\n" +
                        "WHERE ((n.nspname <> 'pg_toast') AND ((t.typrelid = 0) OR (SELECT (c.relkind = 'c') \"?column?\"\n" +
                        "FROM\n" +
                        "  pg_catalog.pg_class c\n" +
                        "WHERE (c.oid = t.typrelid)\n" +
                        ")))"},
                {"SELECT 1, 2, 3"},
                {"SELECT array[1,2,3][1]"},
                {"select current_schemas(false)[1]"},
                {"select typinput = 1, typoutput = 1, typreceive = 1 from pg_catalog.pg_type"},
                {"select * from unnest(generate_array(1, 10)) t(col_1)"},
                {"select * from unnest(array[1,2,3]) t(col_1)"},
                {"SELECT\n" +
                        "s.r\n" +
                        ", current_schemas(false)[s.r] nspname\n" +
                        "FROM\n" +
                        "UNNEST(generate_array(1, array_upper(current_schemas(false), 1))) s (r)"},
        };
    }

    /**
     * In this test, we only check the query used by jdbc can be parsed and executed.
     * We don't care whether the result is correct.
     */
    @Test(dataProvider = "jdbcQuery")
    public void testJdbcQuery(String sql)
            throws SQLException
    {
        try (Connection connection = createConnection()) {
            PreparedStatement stmt = connection.prepareStatement(sql);
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
        }
    }

    @DataProvider
    public Object[][] paramTypes()
    {
        return new Object[][] {
                {"bool", true},
                {"bool", false},
                {"int8", Long.MIN_VALUE},
                {"int8", Long.MAX_VALUE},
                {"float8", Double.MIN_VALUE},
                {"float8", Double.MAX_VALUE},
                {"char", "c"},
                {"varchar", "Bag full of 💰"},
                {"text", "Bag full of 💰"},
                {"name", "Piękna łąka w 東京都"},
                {"int4", Integer.MIN_VALUE},
                {"int4", Integer.MAX_VALUE},
                {"int2", Short.MIN_VALUE},
                {"int2", Short.MAX_VALUE},
                {"float4", Float.MIN_VALUE},
                {"float4", Float.MAX_VALUE},
                {"oid", 1L},
                {"numeric", new BigDecimal("30.123")},
                {"date", LocalDate.of(1900, 1, 3)},
                // TODO support time
                // {"time", LocalTime.of(12, 10, 16)},
                {"timestamp", LocalDateTime.of(1900, 1, 3, 12, 10, 16, 123000000)},
                // TODO support timestamptz
                // {"timestamptz", ZonedDateTime.of(LocalDateTime.of(1900, 1, 3, 12, 10, 16, 123000000), ZoneId.of("America/Los_Angeles"))},
                {"json", "{\"test\":3, \"test2\":4}"},
                {"bytea", "test1".getBytes(UTF_8)},
                // TODO support interval for pg connector
                // {"interval", new PGInterval(1, 5, -3, 7, 55, 20)},
                {"array", new Boolean[] {true, false}},
                {"array", new Double[] {1.0, 2.0, 3.0}},
                {"array", new String[] {"hello", "world"}}

                // TODO: type support
                // {"any", new Object[] {1, "test", new BigDecimal(10)}}
        };
    }

    @Test(dataProvider = "paramTypes")
    public void testJdbcParamTypes(String name, Object obj)
            throws SQLException
    {
        try (Connection conn = createConnection()) {
            PreparedStatement stmt = conn.prepareStatement("SELECT ? as col;");
            stmt.setObject(1, obj);
            ResultSet result = stmt.executeQuery();
            result.next();
            Object expected = obj;

            if (name.equals("int2")) {
                expected = ((Short) obj).intValue();
            }

            if (name.equals("array")) {
                assertThat(result.getArray(1).getArray()).isEqualTo(expected);
            }
            else if (name.equals("date")) {
                // pg jdbc handle date type as String if using `getObject`
                assertThat(result.getDate(1).toLocalDate()).isEqualTo(expected);
            }
            else if (name.equals("timestamp")) {
                assertThat(result.getTimestamp(1).toLocalDateTime()).isEqualTo(expected);
            }
            else {
                assertThat(result.getObject(1)).isEqualTo(expected);
            }
        }
    }

    protected static void assertDefaultPgConfigResponse(TestingWireProtocolClient protocolClient)
            throws IOException
    {
        for (Map.Entry<String, String> config : PostgresWireProtocol.DEFAULT_PG_CONFIGS.entrySet()) {
            protocolClient.assertParameterStatus(config.getKey(), config.getValue());
        }
    }
}
