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

package io.accio.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.accio.base.AccioMDL;
import io.accio.base.dto.Manifest;
import io.accio.base.type.PGType;
import io.accio.base.type.PGTypes;
import org.assertj.core.api.AssertionsForClassTypes;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.accio.base.type.IntegerType.INTEGER;
import static io.accio.base.type.VarcharType.VARCHAR;
import static io.accio.testing.TestingWireProtocolClient.Parameter.textParameter;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMetadataQuery
        extends AbstractWireProtocolTest
{
    @Override
    protected String getDefaultCatalog()
    {
        return "canner-cml";
    }

    @Override
    protected String getDefaultSchema()
    {
        return "tiny_tpch";
    }

    @Override
    protected Optional<String> getAccioMDLPath()
    {
        return Optional.of(requireNonNull(getClass().getClassLoader().getResource("bigquery/TestWireProtocolWithBigquery.json")).getPath());
    }

    @Override
    protected TestingAccioServer createAccioServer()
    {
        Path dir;
        try {
            dir = Files.createTempDirectory(getAccioDirectory());
            if (getAccioMDLPath().isPresent()) {
                Files.copy(Path.of(getAccioMDLPath().get()), dir.resolve("mdl.json"));
            }
            else {
                Files.write(dir.resolve("manifest.json"), Manifest.MANIFEST_JSON_CODEC.toJsonBytes(AccioMDL.EMPTY.getManifest()));
            }
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("accio.datasource.type", "DUCKDB")
                .put("accio.directory", dir.toString());

        return TestingAccioServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
    }

    @Test
    public void testBasic()
            throws Exception
    {
        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            ResultSet result = stmt.executeQuery("SELECT typname FROM pg_type WHERE oid = 14");
            result.next();
            assertThat(result.getString(1)).isEqualTo("bigint");
        }

        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            ResultSet result = stmt.executeQuery("SELECT 'Orders'::regclass");
            result.next();
            assertThat(result.getString(1)).isEqualTo("Orders");
        }
    }

    @Test
    public void testPreparedStatement()
            throws Exception
    {
        try (Connection conn = createConnection()) {
            PreparedStatement stmt = conn.prepareStatement("SELECT typname FROM pg_type WHERE oid = ?");
            stmt.setInt(1, 14);
            ResultSet result = stmt.executeQuery();
            result.next();
            assertThat(result.getString(1)).isEqualTo("bigint");
        }
    }

    @Test
    public void testAuthWithoutPassword()
            throws Exception
    {
        try (TestingWireProtocolClient protocolClient = wireProtocolClient()) {
            protocolClient.sendStartUpMessage(196608, null, "test", "canner");
            protocolClient.assertAuthOk();
            assertDefaultPgConfigResponse(protocolClient);
            protocolClient.assertReadyForQuery('I');
        }
    }

    @Test
    public void testExecuteAndDescribeLevel1Query()
            throws Exception
    {
        try (TestingWireProtocolClient protocolClient = wireProtocolClient()) {
            protocolClient.sendStartUpMessage(196608, MOCK_PASSWORD, "test", "canner");
            protocolClient.assertAuthOk();
            assertDefaultPgConfigResponse(protocolClient);
            protocolClient.assertReadyForQuery('I');

            List<PGType> paramTypes = ImmutableList.of(INTEGER);
            protocolClient.sendParse("", "select typname from pg_type where pg_type.oid = ?",
                    paramTypes.stream().map(PGType::oid).collect(toImmutableList()));
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.STATEMENT, "");
            protocolClient.sendBind("", "", ImmutableList.of(textParameter(14, INTEGER)));
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.PORTAL, "");
            protocolClient.sendExecute("", 0);

            protocolClient.assertParseComplete();

            List<PGType<?>> actualParamTypes = protocolClient.assertAndGetParameterDescription();
            AssertionsForClassTypes.assertThat(actualParamTypes).isEqualTo(paramTypes);

            List<TestingWireProtocolClient.Field> fields = protocolClient.assertAndGetRowDescriptionFields();
            List<PGType> actualTypes = fields.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
            AssertionsForClassTypes.assertThat(actualTypes).isEqualTo(ImmutableList.of(VARCHAR));

            protocolClient.assertBindComplete();

            List<TestingWireProtocolClient.Field> fields2 = protocolClient.assertAndGetRowDescriptionFields();
            List<PGType> actualTypes2 = fields2.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
            AssertionsForClassTypes.assertThat(actualTypes2).isEqualTo(ImmutableList.of(VARCHAR));

            protocolClient.assertDataRow("bigint");
            protocolClient.assertCommandComplete("SELECT 1");
        }
    }

    @Test
    public void testExecuteAndDescribeLevel2Query()
            throws Exception
    {
        try (TestingWireProtocolClient protocolClient = wireProtocolClient()) {
            protocolClient.sendStartUpMessage(196608, MOCK_PASSWORD, "test", "canner");
            protocolClient.assertAuthOk();
            assertDefaultPgConfigResponse(protocolClient);
            protocolClient.assertReadyForQuery('I');

            List<PGType> paramTypes = ImmutableList.of(INTEGER);
            protocolClient.sendParse("", "select 'pg_type'::regclass, ?",
                    paramTypes.stream().map(PGType::oid).collect(toImmutableList()));
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.STATEMENT, "");
            protocolClient.sendBind("", "", ImmutableList.of(textParameter(14, INTEGER)));
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.PORTAL, "");
            protocolClient.sendExecute("", 0);

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

            protocolClient.assertDataRow("pg_type,14");
            protocolClient.assertCommandComplete("SELECT 1");
        }
    }

    @Test
    public void testQueryLevelIsolationIssueInOneTransaction()
            throws IOException
    {
        try (TestingWireProtocolClient protocolClient = wireProtocolClient()) {
            protocolClient.sendStartUpMessage(196608, MOCK_PASSWORD, "test", "canner");
            protocolClient.assertAuthOk();
            assertDefaultPgConfigResponse(protocolClient);
            protocolClient.assertReadyForQuery('I');

            // Execute level 1

            List<PGType> paramTypes = ImmutableList.of(INTEGER);
            protocolClient.sendParse("", "select typname from pg_type where pg_type.oid = ?",
                    paramTypes.stream().map(PGType::oid).collect(toImmutableList()));
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.STATEMENT, "");
            protocolClient.sendBind("", "", ImmutableList.of(textParameter(14, INTEGER)));
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.PORTAL, "");
            protocolClient.sendExecute("", 0);

            protocolClient.assertParseComplete();

            List<PGType<?>> actualParamTypes = protocolClient.assertAndGetParameterDescription();
            AssertionsForClassTypes.assertThat(actualParamTypes).isEqualTo(paramTypes);

            List<TestingWireProtocolClient.Field> fields = protocolClient.assertAndGetRowDescriptionFields();
            List<PGType> actualTypes = fields.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
            AssertionsForClassTypes.assertThat(actualTypes).isEqualTo(ImmutableList.of(VARCHAR));

            protocolClient.assertBindComplete();

            List<TestingWireProtocolClient.Field> fields2 = protocolClient.assertAndGetRowDescriptionFields();
            List<PGType> actualTypes2 = fields2.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
            AssertionsForClassTypes.assertThat(actualTypes2).isEqualTo(ImmutableList.of(VARCHAR));

            protocolClient.assertDataRow("bigint");
            protocolClient.assertCommandComplete("SELECT 1");

            // Execute Level 3
            paramTypes = ImmutableList.of(INTEGER);
            protocolClient.sendParse("", "select * from (values ('rows1', 10), ('rows2', 10)) as t(col1, col2) where col2 = ?",
                    paramTypes.stream().map(PGType::oid).collect(toImmutableList()));
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.STATEMENT, "");
            protocolClient.sendBind("", "", ImmutableList.of(textParameter(10, INTEGER)));
            protocolClient.sendDescribe(TestingWireProtocolClient.DescribeType.PORTAL, "");
            protocolClient.sendExecute("", 0);
            protocolClient.sendSync();

            protocolClient.assertParseComplete();

            actualParamTypes = protocolClient.assertAndGetParameterDescription();
            AssertionsForClassTypes.assertThat(actualParamTypes).isEqualTo(paramTypes);

            fields = protocolClient.assertAndGetRowDescriptionFields();
            actualTypes = fields.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
            AssertionsForClassTypes.assertThat(actualTypes).isEqualTo(ImmutableList.of(VARCHAR, INTEGER));

            protocolClient.assertBindComplete();

            fields2 = protocolClient.assertAndGetRowDescriptionFields();
            actualTypes2 = fields2.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
            AssertionsForClassTypes.assertThat(actualTypes2).isEqualTo(ImmutableList.of(VARCHAR, INTEGER));

            protocolClient.assertDataRow("rows1,10");
            protocolClient.assertDataRow("rows2,10");
            protocolClient.assertCommandComplete("SELECT 2");
            protocolClient.assertReadyForQuery('I');
        }
    }
}
