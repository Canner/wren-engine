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
import io.cml.spi.type.PGType;
import io.cml.spi.type.PGTypes;
import io.cml.testing.AbstractWireProtocolTest;
import io.cml.testing.TestingWireProtocolClient;
import io.cml.testing.TestingWireProtocolServer;
import io.cml.wireprotocol.FormatCodes;
import io.cml.wireprotocol.PostgresWireProtocol;
import org.assertj.core.api.AssertionsForClassTypes;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.cml.spi.type.IntegerType.INTEGER;
import static io.cml.spi.type.VarcharType.VARCHAR;
import static io.cml.testing.TestingWireProtocolClient.Parameter.textParameter;
import static java.lang.System.getenv;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestWireProtocolWithBigquery
        extends AbstractWireProtocolTest
{
    public static final String MOCK_PASSWORD = "ignored";

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
    public void testSimpleQuery()
            throws IOException
    {
        try (TestingWireProtocolClient protocolClient = wireProtocolClient()) {
            protocolClient.sendStartUpMessage(196608, MOCK_PASSWORD, "test", "canner");

            protocolClient.assertAuthOk();
            assertDefaultPgConfigResponse(protocolClient);
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery("SELECT o_custkey, COUNT(*) as cnt FROM \"cannerflow-286003\".\"tpch_tiny\".\"orders\" GROUP BY o_custkey");

            List<TestingWireProtocolClient.Field> fields = protocolClient.assertAndGetRowDescriptionFields();
            List<PGType<?>> types = fields.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(Collectors.toList());
            assertThat(types).isEqualTo(ImmutableList.of(INTEGER, INTEGER));

            protocolClient.printResult(ImmutableList.of(), new FormatCodes.FormatCode[0]);
            protocolClient.assertReadyForQuery('I');
        }
    }

    // TODO: enable this test
    @Test(enabled = false)
    public void testSimpleQueryTpchQ1()
            throws IOException, URISyntaxException
    {
        Path path = Paths.get(requireNonNull(getClass().getClassLoader().getResource("tpch/q1.sql")).toURI());
        String sql = Files.readString(path);

        try (TestingWireProtocolClient protocolClient = wireProtocolClient()) {
            protocolClient.sendStartUpMessage(196608, MOCK_PASSWORD, "test", "canner");

            protocolClient.assertAuthOk();
            assertDefaultPgConfigResponse(protocolClient);
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery(sql);

            // List<TestingWireProtocolClient.Field> fields = protocolClient.assertAndGetRowDescriptionFields();
            // List<PGType<?>> types = fields.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(Collectors.toList());
            // assertThat(types).isEqualTo(ImmutableList.of(INTEGER, INTEGER, INTEGER));

            protocolClient.printResult(ImmutableList.of(), new FormatCodes.FormatCode[0]);
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

            List<PGType> paramTypes = ImmutableList.of(VARCHAR);
            protocolClient.sendParse("teststmt", "select o_orderstatus, o_orderkey from \"cannerflow-286003\".\"tpch_tiny\".\"orders\" where o_orderstatus = ? limit 2",
                    paramTypes.stream().map(PGType::oid).collect(toImmutableList()));
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.STATEMENT, "teststmt");
            protocolClient.sendBind("exec1", "teststmt", ImmutableList.of(textParameter("F", VARCHAR)));
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.PORTAL, "exec1");
            protocolClient.sendExecute("exec1", 0);
            protocolClient.sendSync();

            protocolClient.assertParseComplete();

            List<PGType<?>> actualParamTypes = protocolClient.assertAndGetParameterDescription();
            AssertionsForClassTypes.assertThat(actualParamTypes).isEqualTo(paramTypes);

            List<TestingWireProtocolClient.Field> fields = protocolClient.assertAndGetRowDescriptionFields();
            List<PGType> actualTypes = fields.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
            AssertionsForClassTypes.assertThat(actualTypes).isEqualTo(ImmutableList.of(VARCHAR, INTEGER));

            protocolClient.assertBindComplete();

            List<TestingWireProtocolClient.Field> fields2 = protocolClient.assertAndGetRowDescriptionFields();
            List<PGType> actualTypes2 = fields2.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
            AssertionsForClassTypes.assertThat(actualTypes2).isEqualTo(ImmutableList.of(VARCHAR, INTEGER));

            protocolClient.assertDataRow("F,36485");
            protocolClient.assertDataRow("F,37315");
            protocolClient.assertCommandComplete("SELECT 2");
            protocolClient.assertReadyForQuery('I');
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
