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

import com.google.common.collect.ImmutableList;
import io.accio.base.type.PGArray;
import io.accio.base.type.PGType;
import io.accio.base.type.PGTypes;
import io.accio.main.wireprotocol.PostgresWireProtocol;
import io.accio.testing.TestingWireProtocolClient;
import org.assertj.core.api.AssertionsForClassTypes;
import org.intellij.lang.annotations.Language;
import org.postgresql.util.PGInterval;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.accio.base.type.BigIntType.BIGINT;
import static io.accio.base.type.BooleanType.BOOLEAN;
import static io.accio.base.type.ByteaType.BYTEA;
import static io.accio.base.type.CharType.CHAR;
import static io.accio.base.type.DateType.DATE;
import static io.accio.base.type.DoubleType.DOUBLE;
import static io.accio.base.type.IntegerType.INTEGER;
import static io.accio.base.type.IntervalType.INTERVAL;
import static io.accio.base.type.JsonType.JSON;
import static io.accio.base.type.NumericType.NUMERIC;
import static io.accio.base.type.OidType.OID_INSTANCE;
import static io.accio.base.type.RealType.REAL;
import static io.accio.base.type.SmallIntType.SMALLINT;
import static io.accio.base.type.TimestampType.TIMESTAMP;
import static io.accio.base.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIMEZONE;
import static io.accio.base.type.VarcharType.NameType.NAME;
import static io.accio.base.type.VarcharType.TextType.TEXT;
import static io.accio.base.type.VarcharType.VARCHAR;
import static io.accio.testing.TestingWireProtocolClient.DescribeType.PORTAL;
import static io.accio.testing.TestingWireProtocolClient.DescribeType.STATEMENT;
import static io.accio.testing.TestingWireProtocolClient.Parameter.binaryParameter;
import static io.accio.testing.TestingWireProtocolClient.Parameter.textParameter;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class TestWireProtocolWithBigquery
        extends AbstractWireProtocolTestWithBigQuery
{
    @Override
    protected Optional<String> getAccioMDLPath()
    {
        return Optional.of(requireNonNull(getClass().getClassLoader().getResource("bigquery/TestWireProtocolWithBigquery.json")).getPath());
    }

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
            assertThat(types).isEqualTo(ImmutableList.of(VARCHAR, BIGINT));

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
            AssertionsForClassTypes.assertThat(actualParamTypes).isEqualTo(paramTypes);

            List<TestingWireProtocolClient.Field> fields = protocolClient.assertAndGetRowDescriptionFields();
            List<PGType> actualTypes = fields.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
            AssertionsForClassTypes.assertThat(actualTypes).isEqualTo(ImmutableList.of(VARCHAR, BIGINT));

            protocolClient.assertBindComplete();

            List<TestingWireProtocolClient.Field> fields2 = protocolClient.assertAndGetRowDescriptionFields();
            List<PGType> actualTypes2 = fields2.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
            AssertionsForClassTypes.assertThat(actualTypes2).isEqualTo(ImmutableList.of(VARCHAR, BIGINT));

            protocolClient.assertDataRow("rows1,10");
            protocolClient.assertDataRow("rows2,10");
            protocolClient.assertCommandComplete("SELECT 2");
            protocolClient.assertReadyForQuery('I');
        }
    }

    @Test
    public void testPreparedStatementTextNameType()
            throws Exception
    {
        try (TestingWireProtocolClient protocolClient = wireProtocolClient()) {
            protocolClient.sendStartUpMessage(196608, MOCK_PASSWORD, "test", "canner");
            protocolClient.assertAuthOk();
            assertDefaultPgConfigResponse(protocolClient);
            protocolClient.assertReadyForQuery('I');

            List<PGType> paramTypes = ImmutableList.of(TEXT, NAME);
            protocolClient.sendParse("teststmt", "select * from (values ('rows1', 'rows11'), ('rows2', 'rows22')) as t(col1, col2) where col1 = ? and col2 = ?",
                    paramTypes.stream().map(PGType::oid).collect(toImmutableList()));
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.STATEMENT, "teststmt");
            protocolClient.sendBind("exec1", "teststmt", ImmutableList.of(textParameter("rows1", TEXT), textParameter("rows11", NAME)));
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.PORTAL, "exec1");
            protocolClient.sendExecute("exec1", 0);
            protocolClient.sendSync();

            protocolClient.assertParseComplete();

            List<PGType<?>> actualParamTypes = protocolClient.assertAndGetParameterDescription();
            AssertionsForClassTypes.assertThat(actualParamTypes).isEqualTo(paramTypes);

            List<TestingWireProtocolClient.Field> fields = protocolClient.assertAndGetRowDescriptionFields();
            List<PGType> actualTypes = fields.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
            AssertionsForClassTypes.assertThat(actualTypes).isEqualTo(ImmutableList.of(VARCHAR, VARCHAR));

            protocolClient.assertBindComplete();

            List<TestingWireProtocolClient.Field> fields2 = protocolClient.assertAndGetRowDescriptionFields();
            List<PGType> actualTypes2 = fields2.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
            AssertionsForClassTypes.assertThat(actualTypes2).isEqualTo(ImmutableList.of(VARCHAR, VARCHAR));

            protocolClient.assertDataRow("rows1,rows11");
            protocolClient.assertCommandComplete("SELECT 1");
            protocolClient.assertReadyForQuery('I');
        }
    }

    @Test(dataProvider = "paramTypes")
    public void testPreparedStatementParameterType(String ignored, PGType pgType, Object value, String expected)
            throws Exception
    {
        try (TestingWireProtocolClient protocolClient = wireProtocolClient()) {
            protocolClient.sendStartUpMessage(196608, MOCK_PASSWORD, "test", "canner");
            protocolClient.assertAuthOk();
            assertDefaultPgConfigResponse(protocolClient);
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendParse("teststmt", "select ?", ImmutableList.of(pgType.oid()));
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.STATEMENT, "teststmt");
            if (pgType.equals(BYTEA)) {
                protocolClient.sendBind("exec1", "teststmt", ImmutableList.of(binaryParameter(value, pgType)));
            }
            else {
                protocolClient.sendBind("exec1", "teststmt", ImmutableList.of(textParameter(value, pgType)));
            }
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.PORTAL, "exec1");
            protocolClient.sendExecute("exec1", 0);
            protocolClient.sendSync();

            protocolClient.assertParseComplete();

            PGType expectedType = getDescriptionExpectedType(pgType);

            List<PGType<?>> actualParamTypes = protocolClient.assertAndGetParameterDescription();
            AssertionsForClassTypes.assertThat(actualParamTypes).isEqualTo(ImmutableList.of(pgType));

            List<TestingWireProtocolClient.Field> fields = protocolClient.assertAndGetRowDescriptionFields();
            List<PGType> actualTypes = fields.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
            AssertionsForClassTypes.assertThat(actualTypes).isEqualTo(ImmutableList.of(expectedType));

            protocolClient.assertBindComplete();

            List<TestingWireProtocolClient.Field> fields2 = protocolClient.assertAndGetRowDescriptionFields();
            List<PGType> actualTypes2 = fields2.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
            AssertionsForClassTypes.assertThat(actualTypes2).isEqualTo(ImmutableList.of(expectedType));

            protocolClient.assertDataRow(expected);
            protocolClient.assertCommandComplete("SELECT 1");
            protocolClient.assertReadyForQuery('I');
        }
    }

    private PGType getDescriptionExpectedType(PGType pgType)
    {
        if (pgType.equals(CHAR) ||
                pgType.equals(TEXT) ||
                pgType.equals(NAME) ||
                pgType.equals(JSON)) {
            return VARCHAR;
        }
        if (pgType.equals(INTEGER) || pgType.equals(SMALLINT) ||
                pgType.equals(OID_INSTANCE)) {
            return BIGINT;
        }
        if (pgType.equals(REAL)) {
            return DOUBLE;
        }
        if (pgType.equals(TIMESTAMP_WITH_TIMEZONE)) {
            return TIMESTAMP;
        }
        return pgType;
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
            protocolClient.assertErrorMessage("query can't be null");
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
            protocolClient.assertErrorMessage("No oid mapping from '999' to pg_type");

            protocolClient.sendBind("exec1", "teststmt", ImmutableList.of(textParameter("10", INTEGER)));
            protocolClient.assertErrorMessage("prepared statement teststmt not found");
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
            AssertionsForClassTypes.assertThat(fields.size()).isZero();
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
            AssertionsForClassTypes.assertThat(actualTypes).isEqualTo(ImmutableList.of(VARCHAR, BIGINT));

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
            AssertionsForClassTypes.assertThat(actualTypes).isEqualTo(ImmutableList.of(VARCHAR, BIGINT));

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
        AssertionsForClassTypes.assertThat(actualParamTypes).isEqualTo(paramTypes);
        List<TestingWireProtocolClient.Field> fields = protocolClient.assertAndGetRowDescriptionFields();
        List<PGType<?>> actualTypes = fields.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
        AssertionsForClassTypes.assertThat(actualTypes).isEqualTo(ImmutableList.of(VARCHAR));
        protocolClient.assertBindComplete();
        List<TestingWireProtocolClient.Field> fields2 = protocolClient.assertAndGetRowDescriptionFields();
        List<PGType<?>> actualTypes2 = fields2.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
        AssertionsForClassTypes.assertThat(actualTypes2).isEqualTo(ImmutableList.of(VARCHAR));
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
            AssertionsForClassTypes.assertThat(actualParamTypes).isEqualTo(paramTypes);
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
        AssertionsForClassTypes.assertThat(actualTypes).isEqualTo(types);
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
            AssertionsForClassTypes.assertThat(types).isEqualTo(ImmutableList.of(BIGINT, BIGINT, BIGINT));
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
    public void testAuthMessageFlow()
            throws Exception
    {
        try (TestingWireProtocolClient protocolClient = wireProtocolClient()) {
            protocolClient.sendStartUpMessage(196608, "wah", "test", "ina");
            protocolClient.assertAuthOk();
            assertDefaultPgConfigResponse(protocolClient);
            protocolClient.assertReadyForQuery('I');
        }

        try (TestingWireProtocolClient protocolClient = wireProtocolClient()) {
            protocolClient.sendStartUpMessage(196608, null, "test", "ina");
            protocolClient.assertAuthenticationCleartextPassword();
            protocolClient.sendPasswordMessage("wah");
            protocolClient.assertAuthOk();
            assertDefaultPgConfigResponse(protocolClient);
            protocolClient.assertReadyForQuery('I');
        }

        try (TestingWireProtocolClient protocolClient = wireProtocolClient()) {
            protocolClient.sendStartUpMessage(196608, null, "test", "fakeuser");
            protocolClient.assertAuthenticationCleartextPassword();
            protocolClient.sendPasswordMessage(MOCK_PASSWORD);
            protocolClient.assertErrorMessage(".*not found or permission denied");
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

            AssertionsForClassTypes.assertThat(count).isEqualTo(3);
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
            AssertionsForClassTypes.assertThat(result.getString(1)).isEqualTo("rows1");
            AssertionsForClassTypes.assertThat(result.getInt(2)).isEqualTo(10);
            long count = 1;
            while (result.next()) {
                count++;
            }
            AssertionsForClassTypes.assertThat(count).isEqualTo(2);
        }

        try (Connection conn = createConnection()) {
            PreparedStatement stmt = conn.prepareStatement("select * from (values ('rows1', 10), ('rows2', 10)) as t(col1, col2)");
            ResultSet result = stmt.executeQuery();
            long count = 0;
            while (result.next()) {
                count++;
            }
            AssertionsForClassTypes.assertThat(count).isEqualTo(2);
        }

        try (Connection conn = createConnection()) {
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
        try (Connection conn = createConnection()) {
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
        try (Connection conn = createConnection()) {
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
        try (Connection conn = createConnection()) {
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
        try (Connection conn = createConnection()) {
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
        try (Connection conn = createConnection()) {
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
        try (Connection conn = createConnection()) {
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

    @DataProvider
    public Object[][] jdbcQuery()
    {
        return new Object[][] {
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
                {"SELECT FLOOR((\"tpch_tiny\".\"Lineitem\".\"orderkey\" / 7500.0)) * 7500.0 AS \"l_orderkey\", COUNT(*) AS \"count\" " +
                        "FROM \"tpch_tiny\".\"Lineitem\" " +
                        "GROUP BY FLOOR((\"tpch_tiny\".\"Lineitem\".\"orderkey\" / 7500.0)) * 7500.0 " +
                        "ORDER BY FLOOR((\"tpch_tiny\".\"Lineitem\".\"orderkey\" / 7500.0)) * 7500.0 ASC"},
                {"SELECT (CAST(extract(dow from \"tpch_tiny\".\"Lineitem\".\"shipdate\") AS integer) + 1) AS \"l_shipdate\", " +
                        "count(distinct \"tpch_tiny\".\"Lineitem\".\"orderkey\") AS \"count\" " +
                        "FROM \"tpch_tiny\".\"Lineitem\" " +
                        "GROUP BY (CAST(extract(dow from \"tpch_tiny\".\"Lineitem\".\"shipdate\") AS integer) + 1) " +
                        "ORDER BY (CAST(extract(dow from \"tpch_tiny\".\"Lineitem\".\"shipdate\") AS integer) + 1) ASC LIMIT 10\n"},
                {"SELECT DATE_TRUNC('year', (NOW() + INTERVAL '-30 year'))"},
                {"SELECT 'array_in'::regproc"}
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
                {"bool", BOOLEAN, true, "t"},
                {"bool", BOOLEAN, false, "f"},
                {"int8", BIGINT, Long.MIN_VALUE, Long.toString(Long.MIN_VALUE)},
                {"int8", BIGINT, Long.MAX_VALUE, Long.toString(Long.MAX_VALUE)},
                {"float8", DOUBLE, Double.MIN_VALUE, Double.toString(Double.MIN_VALUE)},
                {"float8", DOUBLE, Double.MAX_VALUE, Double.toString(Double.MAX_VALUE)},
                {"char", CHAR, "c", "c"},
                {"varchar", VARCHAR, "Bag full of 💰", "Bag full of 💰"},
                {"text", TEXT, "Bag full of 💰", "Bag full of 💰"},
                {"name", NAME, "Piękna łąka w 東京都", "Piękna łąka w 東京都"},
                {"int4", INTEGER, Integer.MIN_VALUE, Integer.toString(Integer.MIN_VALUE)},
                {"int4", INTEGER, Integer.MAX_VALUE, Integer.toString(Integer.MAX_VALUE)},
                {"int2", SMALLINT, Short.MIN_VALUE, Short.toString(Short.MIN_VALUE)},
                {"int2", SMALLINT, Short.MAX_VALUE, Short.toString(Short.MAX_VALUE)},
                {"float4", REAL, Float.MIN_VALUE, Float.toString(Float.MIN_VALUE)},
                {"float4", REAL, Float.MAX_VALUE, Float.toString(Float.MAX_VALUE)},
                {"oid", OID_INSTANCE, 1L, "1"},
                {"numeric", NUMERIC, new BigDecimal("30.123"), "30.123"},
                {"date", DATE, LocalDate.of(1900, 1, 3), "1900-01-03"},
                // TODO support time
                // {"time", LocalTime.of(12, 10, 16)},
                {"timestamp", TIMESTAMP, Timestamp.valueOf(LocalDateTime.of(1900, 1, 3, 12, 10, 16, 123000000)), "1900-01-03 12:10:16.123000"},
                {"timestamptz", TIMESTAMP_WITH_TIMEZONE,
                        ZonedDateTime.of(LocalDateTime.of(1900, 1, 3, 12, 10, 16, 123000000), ZoneId.of("America/Los_Angeles")),
                        // BigQuery will transform the timestamp to UTC time
                        "1900-01-03 20:10:16.123000"},
                {"json", JSON, "{\"test\":3, \"test2\":4}", "{\"test\":3,\"test2\":4}"},
                {"bytea", BYTEA, "test1".getBytes(UTF_8), "\\x7465737431"},
                {"interval", INTERVAL, new PGInterval(1, 5, -3, 7, 55, 20), "1 year 5 mons -3 days 07:55:20"},
                {"array", PGArray.BOOL_ARRAY, new Boolean[] {true, false}, "{t,f}"},
                {"array", PGArray.FLOAT8_ARRAY, new Double[] {1.0, 2.0, 3.0}, "{1.0,2.0,3.0}"},
                {"array", PGArray.VARCHAR_ARRAY, new String[] {"hello", "world"}, "{hello,world}"},

                // TODO: type support
                // {"any", new Object[] {1, "test", new BigDecimal(10)}}
        };
    }

    @Test(dataProvider = "paramTypes")
    public void testJdbcParamTypes(String name, PGType ignored, Object obj, String ignored2)
            throws SQLException
    {
        try (Connection conn = createConnection()) {
            PreparedStatement stmt = conn.prepareStatement("SELECT ? as col;");
            if (name.equals("timestamptz")) {
                ZonedDateTime zdt = (ZonedDateTime) obj;
                stmt.setObject(1, zdt.toOffsetDateTime());
            }
            else if (name.equals("timestamp")) {
                Timestamp timestamp = (Timestamp) obj;
                stmt.setObject(1, timestamp.toLocalDateTime());
            }
            else {
                stmt.setObject(1, obj);
            }
            ResultSet result = stmt.executeQuery();
            result.next();
            Object expected = obj;
            // TODO https://github.com/Canner/canner-metric-layer/issues/196
            if (name.equals("int2")) {
                expected = ((Short) obj).longValue();
            }
            else if (name.equals("int4")) {
                expected = ((Integer) obj).longValue();
            }
            else if (name.equals("float4")) {
                expected = Double.valueOf(obj.toString());
            }
            else if (name.equals("date")) {
                expected = Date.valueOf((LocalDate) obj);
            }
            // bigquery always return utc time
            else if (name.equals("timestamptz")) {
                ZonedDateTime zdt = (ZonedDateTime) obj;
                Timestamp timestamp = (Timestamp) result.getObject(1);
                LocalDateTime utcTime = timestamp.toLocalDateTime();
                AssertionsForClassTypes.assertThat(utcTime.atZone(UTC)).isEqualTo(zdt);
                return;
            }

            if (name.equals("array")) {
                assertThat(result.getArray(1).getArray()).isEqualTo(expected);
            }
            else {
                assertThat(result.getObject(1)).isEqualTo(expected);
            }
        }
    }

    @Test
    public void testColumns()
            throws SQLException
    {
        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            ResultSet result = stmt.executeQuery("SELECT\n" +
                    "n.nspname AS TABLE_SCHEM,\n" +
                    "ct.relname AS TABLE_NAME,\n" +
                    "a.attname AS COLUMN_NAME,\n" +
                    "a.attnum AS A_ATTNUM\n" +
                    "FROM pg_catalog.pg_class ct\n" +
                    "JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)\n" +
                    "JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid) WHERE true AND n.nspname = E'tpch_tiny' AND ct.relname = E'Orders'");
            List<String> columnNames = new ArrayList<>();
            while (result.next()) {
                columnNames.add(result.getString("COLUMN_NAME"));
            }
            assertThat(columnNames).containsExactlyInAnyOrder("orderkey", "custkey", "totalprice", "orderdate", "orderstatus");
        }
    }

    @Test
    public void testCountif()
            throws Exception
    {
        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            ResultSet result = stmt.executeQuery("SELECT count_if(orderkey > 100) FROM Lineitem");
            result.next();
            assertThat(result.getLong(1)).isEqualTo(60065L);
        }
    }

    @Test
    public void testLimitOffset()
            throws Exception
    {
        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            ResultSet result = stmt.executeQuery("SELECT orderkey FROM Lineitem LIMIT 100");
            int count = 0;
            while (result.next()) {
                count++;
            }
            assertThat(count).isEqualTo(100);
        }

        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            ResultSet result = stmt.executeQuery("SELECT orderkey FROM Lineitem LIMIT 100 OFFSET 50");
            int count = 0;
            while (result.next()) {
                count++;
            }
            assertThat(count).isEqualTo(100);
        }
    }

    @Test
    public void testArrayAgg()
            throws Exception
    {
        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            ResultSet result = stmt.executeQuery("SELECT array_agg(distinct if(orderkey < 2, orderkey, null)) FROM Lineitem");
            result.next();
            assertThat(result.getArray(1).getArray()).isEqualTo(new Long[] {1L});
        }

        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            ResultSet result = stmt.executeQuery("SELECT array_agg(if(orderkey < 3, orderkey, null) order by orderkey asc) FROM Lineitem");
            result.next();
            assertThat(result.getArray(1).getArray()).isEqualTo(new Long[] {1L, 1L, 1L, 1L, 1L, 1L, 2L});
        }
    }

    @Test
    public void testDateDiff()
            throws Exception
    {
        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            ResultSet result = stmt.executeQuery("SELECT date_diff('second', TIMESTAMP '2020-03-01 00:00:00', TIMESTAMP '2020-03-02 00:00:00')");
            result.next();
            assertThat(result.getLong(1)).isEqualTo(86400L);
        }

        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            ResultSet result = stmt.executeQuery("SELECT date_diff('hour', TIMESTAMP '2020-03-01 00:00:00 UTC', TIMESTAMP '2020-03-02 00:00:00 UTC')");
            result.next();
            assertThat(result.getLong(1)).isEqualTo(24L);
        }

        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            ResultSet result = stmt.executeQuery("SELECT date_diff('millisecond', TIMESTAMP '2020-06-01 12:30:45.000000', TIMESTAMP '2020-06-02 12:30:45.123456')");
            result.next();
            assertThat(result.getLong(1)).isEqualTo(86400123L);
        }

        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            ResultSet result = stmt.executeQuery("SELECT date_diff('microsecond', TIMESTAMP '2020-06-01 12:30:45.000000', TIMESTAMP '2020-06-02 12:30:45.123456')");
            result.next();
            assertThat(result.getLong(1)).isEqualTo(86400123456L);
        }

        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            ResultSet result = stmt.executeQuery("SELECT date_diff('day', DATE '2019-01-01', DATE '2019-01-02')");
            result.next();
            assertThat(result.getLong(1)).isEqualTo(1L);
        }

        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            ResultSet result = stmt.executeQuery("SELECT date_diff('day', '2019-01-01', '2019-01-02')");
            result.next();
            assertThat(result.getLong(1)).isEqualTo(1L);
        }
    }

    @Test
    public void testAuth()
    {
        Properties props = new Properties();
        props.setProperty("password", "wah");
        props.setProperty("user", "ina");
        props.setProperty("ssl", "false");
        props.setProperty("currentSchema", getDefaultSchema());
        assertThatNoException().isThrownBy(() -> createConnection(props).close());
        props.setProperty("user", "gura");
        props.setProperty("password", "a");
        assertThatNoException().isThrownBy(() -> createConnection(props).close());
        props.setProperty("user", "pekochan");
        props.setProperty("password", "pekopeko");
        props.setProperty("user", "notfound");
        assertThatThrownBy(() -> createConnection(props)).hasMessageContaining("not found or permission denied");
        props.setProperty("user", "ina");
        props.setProperty("password", "wrong");
        assertThatThrownBy(() -> createConnection(props)).hasMessageContaining("not found or permission denied");
        props.setProperty("user", "");
        props.setProperty("password", "emptypass");
        assertThatThrownBy(() -> createConnection(props)).hasMessageContaining("user is empty");
    }

    @Test
    public void testIntervalCompareWithTimestamp()
            throws Exception
    {
        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            assertThatNoException().isThrownBy(() -> {
                ResultSet resultSet = stmt.executeQuery("SELECT shipdate FROM Lineitem WHERE shipdate > current_date + interval '1' day");
                resultSet.next();
            });
        }
        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            assertThatNoException().isThrownBy(() -> {
                ResultSet resultSet = stmt.executeQuery("SELECT shipdate FROM Lineitem WHERE shipdate > current_timestamp + interval '1' day");
                resultSet.next();
            });
        }

        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            assertThatNoException().isThrownBy(() -> {
                ResultSet resultSet = stmt.executeQuery("SELECT shipdate FROM Lineitem WHERE shipdate > current_date + interval '1 week'");
                resultSet.next();
            });
        }

        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            assertThatNoException().isThrownBy(() -> {
                ResultSet resultSet = stmt.executeQuery("SELECT shipdate FROM Lineitem WHERE shipdate > current_date + interval '1' day AND shipdate > current_date + interval '1' day");
                resultSet.next();
            });
        }

        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            assertThatNoException().isThrownBy(() -> {
                ResultSet resultSet = stmt.executeQuery("SELECT shipdate > current_date + interval '1' day FROM Lineitem");
                resultSet.next();
            });
        }

        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            assertThatNoException().isThrownBy(() -> {
                ResultSet resultSet = stmt.executeQuery("SELECT shipdate > current_date + interval '1' day AND shipdate < now() FROM Lineitem");
                resultSet.next();
            });
        }

        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            assertThatNoException().isThrownBy(() -> {
                ResultSet resultSet = stmt.executeQuery("SELECT cast(shipdate as timestamp) = date_trunc('month', shipdate) FROM Lineitem");
                resultSet.next();
            });
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
