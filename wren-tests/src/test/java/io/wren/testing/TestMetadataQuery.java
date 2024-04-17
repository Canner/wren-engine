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

package io.wren.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.wren.base.WrenMDL;
import io.wren.base.dto.Manifest;
import io.wren.base.type.PGType;
import io.wren.base.type.PGTypes;
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
import static io.wren.base.type.IntegerType.INTEGER;
import static io.wren.base.type.VarcharType.VARCHAR;
import static io.wren.testing.TestingWireProtocolClient.Parameter.textParameter;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class TestMetadataQuery
        extends AbstractWireProtocolTest
{
    @Override
    protected String getDefaultCatalog()
    {
        return "wrenai";
    }

    @Override
    protected String getDefaultSchema()
    {
        return "tiny_tpch";
    }

    @Override
    protected Optional<String> getWrenMDLPath()
    {
        return Optional.of(requireNonNull(getClass().getClassLoader().getResource("bigquery/TestWireProtocolWithBigquery.json")).getPath());
    }

    @Override
    protected TestingWrenServer createWrenServer()
    {
        Path dir;
        try {
            dir = Files.createTempDirectory(getWrenDirectory());
            if (getWrenMDLPath().isPresent()) {
                Files.copy(Path.of(getWrenMDLPath().get()), dir.resolve("mdl.json"));
            }
            else {
                Files.write(dir.resolve("manifest.json"), Manifest.MANIFEST_JSON_CODEC.toJsonBytes(WrenMDL.EMPTY.getManifest()));
            }
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("wren.datasource.type", "DUCKDB")
                .put("wren.directory", dir.toString());

        return TestingWrenServer.builder()
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

        assertThatNoException().isThrownBy(() -> {
            try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
                ResultSet result = stmt.executeQuery("with table_privileges as (\n" +
                        " select\n" +
                        "   NULL as role,\n" +
                        "   t.schemaname as schema,\n" +
                        "   t.objectname as table,\n" +
                        "   pg_catalog.has_table_privilege(current_user, '\"' || t.schemaname || '\"' || '.' || '\"' || t.objectname || '\"',  'UPDATE') as update,\n" +
                        "   pg_catalog.has_table_privilege(current_user, '\"' || t.schemaname || '\"' || '.' || '\"' || t.objectname || '\"',  'SELECT') as select,\n" +
                        "   pg_catalog.has_table_privilege(current_user, '\"' || t.schemaname || '\"' || '.' || '\"' || t.objectname || '\"',  'INSERT') as insert,\n" +
                        "   pg_catalog.has_table_privilege(current_user, '\"' || t.schemaname || '\"' || '.' || '\"' || t.objectname || '\"',  'DELETE') as delete\n" +
                        " from (\n" +
                        "   select schemaname, tablename as objectname from pg_catalog.pg_tables\n" +
                        "   union\n" +
                        "   select schemaname, viewname as objectname from pg_catalog.pg_views\n" +
                        "   union\n" +
                        "   select schemaname, matviewname as objectname from pg_catalog.pg_matviews\n" +
                        " ) t\n" +
                        " where t.schemaname !~ '^pg_'\n" +
                        "   and t.schemaname <> 'information_schema'\n" +
                        "   and pg_catalog.has_schema_privilege(current_user, t.schemaname, 'USAGE')\n" +
                        ")\n" +
                        "select t.*\n" +
                        "from table_privileges t");
                result.next();
            }
        });
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
            protocolClient.sendSync();

            protocolClient.assertParseComplete();

            List<PGType<?>> actualParamTypes = protocolClient.assertAndGetParameterDescription();
            assertThat(actualParamTypes).isEqualTo(paramTypes);

            List<TestingWireProtocolClient.Field> fields = protocolClient.assertAndGetRowDescriptionFields();
            List<PGType> actualTypes = fields.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
            assertThat(actualTypes).isEqualTo(ImmutableList.of(VARCHAR));

            protocolClient.assertBindComplete();

            List<TestingWireProtocolClient.Field> fields2 = protocolClient.assertAndGetRowDescriptionFields();
            List<PGType> actualTypes2 = fields2.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
            assertThat(actualTypes2).isEqualTo(ImmutableList.of(VARCHAR));

            protocolClient.assertDataRow("bigint");
            protocolClient.assertCommandComplete("SELECT 1");
            protocolClient.assertReadyForQuery('I');
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

            protocolClient.assertDataRow("pg_type,14");
            protocolClient.assertCommandComplete("SELECT 1");
            protocolClient.assertReadyForQuery('I');
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
            protocolClient.sendSync();

            protocolClient.assertParseComplete();

            List<PGType<?>> actualParamTypes = protocolClient.assertAndGetParameterDescription();
            assertThat(actualParamTypes).isEqualTo(paramTypes);

            List<TestingWireProtocolClient.Field> fields = protocolClient.assertAndGetRowDescriptionFields();
            List<PGType> actualTypes = fields.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
            assertThat(actualTypes).isEqualTo(ImmutableList.of(VARCHAR));

            protocolClient.assertBindComplete();

            List<TestingWireProtocolClient.Field> fields2 = protocolClient.assertAndGetRowDescriptionFields();
            List<PGType> actualTypes2 = fields2.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
            assertThat(actualTypes2).isEqualTo(ImmutableList.of(VARCHAR));

            protocolClient.assertDataRow("bigint");
            protocolClient.assertCommandComplete("SELECT 1");
            protocolClient.assertReadyForQuery('I');

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
            assertThat(actualParamTypes).isEqualTo(paramTypes);

            fields = protocolClient.assertAndGetRowDescriptionFields();
            actualTypes = fields.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
            assertThat(actualTypes).isEqualTo(ImmutableList.of(VARCHAR, INTEGER));

            protocolClient.assertBindComplete();

            fields2 = protocolClient.assertAndGetRowDescriptionFields();
            actualTypes2 = fields2.stream().map(TestingWireProtocolClient.Field::getTypeId).map(PGTypes::oidToPgType).collect(toImmutableList());
            assertThat(actualTypes2).isEqualTo(ImmutableList.of(VARCHAR, INTEGER));

            protocolClient.assertDataRow("rows1,10");
            protocolClient.assertDataRow("rows2,10");
            protocolClient.assertCommandComplete("SELECT 2");
            protocolClient.assertReadyForQuery('I');
        }
    }
}
