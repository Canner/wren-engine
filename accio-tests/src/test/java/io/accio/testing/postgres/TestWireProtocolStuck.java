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

package io.accio.testing.postgres;

import com.google.cloud.bigquery.DatasetId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.Key;
import io.accio.base.type.PGType;
import io.accio.base.type.PGTypes;
import io.accio.connector.bigquery.BigQueryClient;
import io.accio.main.metadata.Metadata;
import io.accio.main.wireprotocol.PostgresWireProtocol;
import io.accio.testing.AbstractWireProtocolTest;
import io.accio.testing.TestingAccioServer;
import io.accio.testing.TestingPostgreSqlServer;
import io.accio.testing.TestingWireProtocolClient;
import io.airlift.log.Logger;
import org.assertj.core.api.AssertionsForClassTypes;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.accio.base.Utils.randomIntString;
import static io.accio.base.type.IntegerType.INTEGER;
import static io.accio.base.type.VarcharType.VARCHAR;
import static io.accio.testing.TestingWireProtocolClient.Parameter.textParameter;
import static java.lang.String.format;
import static java.lang.System.getenv;
import static java.util.Objects.requireNonNull;

public class TestWireProtocolStuck
        extends AbstractWireProtocolTest
{
    private static final Logger log = Logger.get(TestWireProtocolStuck.class.getName());
    private TestingPostgreSqlServer testingPostgreSqlServer;
    private TestingAccioServer testingAccioServerForPG;
    private TestingAccioServer testingAccioServerForBigQuery;

    @Override
    protected String getDefaultCatalog()
    {
        return "tpch";
    }

    @Override
    protected String getDefaultSchema()
    {
        return "tpch";
    }

    @Override
    protected TestingAccioServer createAccioServer()
    {
        log.info("TestWireProtocolStuck.createAccioServer");

        testingPostgreSqlServer = closer.register(new TestingPostgreSqlServer());
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("postgres.jdbc.url", testingPostgreSqlServer.getJdbcUrl())
                .put("postgres.user", testingPostgreSqlServer.getUser())
                .put("postgres.password", testingPostgreSqlServer.getPassword())
                .put("accio.datasource.type", "POSTGRES");
        return TestingAccioServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
    }

    @Test
    public void testStatementNameWithHyphens()
    {
        testBQ();
        testPG();
    }

    private void testBQ()
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("bigquery.project-id", getenv("TEST_BIG_QUERY_PROJECT_ID"))
                .put("bigquery.location", "asia-east1")
                .put("bigquery.credentials-key", getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"))
                .put("bigquery.metadata.schema.prefix", format("test_%s_", randomIntString()))
                .put("pg-wire-protocol.auth.file", requireNonNull(getClass().getClassLoader().getResource("accounts")).getPath())
                .put("accio.datasource.type", "bigquery");
        try {
            Path dir = Files.createTempDirectory(getAccioDirectory());
            Files.copy(Path.of(Optional.of(requireNonNull(getClass().getClassLoader().getResource("bigquery/TestWireProtocolWithBigquery.json")).getPath()).get()), dir.resolve("mdl.json"));
            properties.put("accio.directory", dir.toString());
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        testingAccioServerForBigQuery = TestingAccioServer.builder()
                .setRequiredConfigs(properties.build())
                .build();

        String statementName = "'teststmt'";
        log.info(format("TestWireProtocolStuck.testStatementNameWithHyphensForBQ: case: %s try start", statementName));
        try (TestingWireProtocolClient protocolClient = wireProtocolClientForBQ()) {
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
            log.info(format("TestWireProtocolStuck.testStatementNameWithHyphensForBQ: case: %s try end", statementName));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        log.info(format("TestWireProtocolStuck.testStatementNameWithHyphensForBQ: case: %s Done", statementName));
        cleanBigQuery();
    }

    private void testPG()
    {
        testingPostgreSqlServer = closer.register(new TestingPostgreSqlServer());
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("postgres.jdbc.url", testingPostgreSqlServer.getJdbcUrl())
                .put("postgres.user", testingPostgreSqlServer.getUser())
                .put("postgres.password", testingPostgreSqlServer.getPassword())
                .put("accio.datasource.type", "POSTGRES");
        testingAccioServerForPG = TestingAccioServer.builder()
                .setRequiredConfigs(properties.build())
                .build();

        String statementName = "'teststmt'";
        log.info(format("TestWireProtocolStuck.testStatementNameWithHyphensForPG: case: %s try start", statementName));
        try (TestingWireProtocolClient protocolClient = wireProtocolClientForPG()) {
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

            Thread.sleep(500);

            protocolClient.assertParseComplete();
            List<PGType<?>> actualParamTypes = protocolClient.assertAndGetParameterDescription();
            AssertionsForClassTypes.assertThat(actualParamTypes).isEqualTo(paramTypes);
            assertFields(protocolClient, ImmutableList.of(VARCHAR));
            protocolClient.assertBindComplete();
            assertFields(protocolClient, ImmutableList.of(VARCHAR));
            protocolClient.assertDataRow("rows1");
            protocolClient.assertCommandComplete("SELECT 1");
            protocolClient.assertReadyForQuery('I');
            log.info(format("TestWireProtocolStuck.testStatementNameWithHyphensForPG: case: %s try end", statementName));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            log.info(format("TestWireProtocolStuck.testStatementNameWithHyphensForPG: case: %s Done", statementName));
        }
        cleanPG();
    }

    private void cleanPG()
    {
        log.info("TestWireProtocolStuck.cleanPG");
        try {
            testingAccioServerForPG.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void cleanBigQuery()
    {
        log.info("TestWireProtocolStuck.cleanBigQuery");
        try {
            Metadata metadata = testingAccioServerForBigQuery.getInstance(Key.get(Metadata.class));
            BigQueryClient bigQueryClient = testingAccioServerForBigQuery.getInstance(Key.get(BigQueryClient.class));
            log.info("drop dataset pg catalog: " + metadata.getPgCatalogName());
            bigQueryClient.dropDatasetWithAllContent(DatasetId.of(getDefaultCatalog(), metadata.getPgCatalogName()));
            log.info("drop dataset metadata schema: " + metadata.getMetadataSchemaName());
            bigQueryClient.dropDatasetWithAllContent(DatasetId.of(getDefaultCatalog(), metadata.getMetadataSchemaName()));
            log.info("Server close");
            testingAccioServerForBigQuery.close();
        }
        catch (Exception ex) {
            log.error(ex, "cleanup bigquery schema failed");
            try {
                testingAccioServerForBigQuery.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void assertFields(TestingWireProtocolClient client, List<PGType<?>> types)
            throws IOException
    {
        List<TestingWireProtocolClient.Field> fields = client.assertAndGetRowDescriptionFields();
        List<PGType<?>> actualTypes = fields.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
        AssertionsForClassTypes.assertThat(actualTypes).isEqualTo(types);
    }

    protected static void assertDefaultPgConfigResponse(TestingWireProtocolClient protocolClient)
            throws IOException
    {
        for (Map.Entry<String, String> config : PostgresWireProtocol.DEFAULT_PG_CONFIGS.entrySet()) {
            protocolClient.assertParameterStatus(config.getKey(), config.getValue());
        }
    }

    protected TestingWireProtocolClient wireProtocolClientForPG()
            throws IOException
    {
        HostAndPort hostAndPort = testingAccioServerForPG.getPgHostAndPort();
        return new TestingWireProtocolClient(
                new InetSocketAddress(hostAndPort.getHost(), hostAndPort.getPort()));
    }

    protected TestingWireProtocolClient wireProtocolClientForBQ()
            throws IOException
    {
        HostAndPort hostAndPort = testingAccioServerForBigQuery.getPgHostAndPort();
        return new TestingWireProtocolClient(
                new InetSocketAddress(hostAndPort.getHost(), hostAndPort.getPort()));
    }
}
