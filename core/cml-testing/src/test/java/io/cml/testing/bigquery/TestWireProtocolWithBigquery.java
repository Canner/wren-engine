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
import io.cml.wireprotocol.PostgresWireProtocol;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.cml.spi.type.IntegerType.INTEGER;
import static java.lang.System.getenv;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestWireProtocolWithBigquery
        extends AbstractWireProtocolTest
{
    public static final String MOCK_CLIENT_ID = "canner-client-id";
    public static final String MOCK_CLIENT_SECRET = "VCNWojqhHDhRhzjMm7IYWZj627z97iaU";
    public static final String MOCK_PAT = Base64.getEncoder().encodeToString((MOCK_CLIENT_ID + ":" + MOCK_CLIENT_SECRET).getBytes(UTF_8));

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
            protocolClient.sendStartUpMessage(196608, MOCK_PAT, "test", "canner");

            protocolClient.assertAuthOk();
            assertDefaultPgConfigResponse(protocolClient);
            protocolClient.assertReadyForQuery('I');

            protocolClient.sendSimpleQuery("select * from (values (1, 2, 3), (2, 4, 6)) t1(c1, c2, c3);");

            List<TestingWireProtocolClient.Field> fields = protocolClient.assertAndGetRowDescriptionFields();
            List<PGType<?>> types = fields.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(Collectors.toList());
            assertThat(types).isEqualTo(ImmutableList.of(INTEGER, INTEGER, INTEGER));

            protocolClient.assertDataRow("1,2,3");
            protocolClient.assertDataRow("2,4,6");

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
