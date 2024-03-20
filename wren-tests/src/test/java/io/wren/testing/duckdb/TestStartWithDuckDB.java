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

package io.wren.testing.duckdb;

import com.google.common.collect.ImmutableMap;
import io.wren.base.dto.Manifest;
import io.wren.testing.TestingWrenServer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.wren.base.client.duckdb.DuckDBConnectorConfig.DUCKDB_CONNECTOR_INIT_SQL_PATH;
import static io.wren.base.client.duckdb.DuckDBConnectorConfig.DUCKDB_CONNECTOR_SESSION_SQL_PATH;
import static io.wren.base.config.WrenConfig.DataSourceType.DUCKDB;
import static io.wren.base.dto.Manifest.MANIFEST_JSON_CODEC;
import static org.assertj.core.api.Assertions.assertThatCode;

public class TestStartWithDuckDB
{
    private Path mdlDir;
    private ImmutableMap.Builder<String, String> baseProperties;

    @BeforeMethod
    public void setup()
    {
        try {
            mdlDir = Files.createTempDirectory("wren-mdl");
            Path wrenMDLFilePath = mdlDir.resolve("duckdb_mdl.json");
            Manifest initial = Manifest.builder()
                    .setCatalog("memory")
                    .setSchema("tpch")
                    .build();
            Files.write(wrenMDLFilePath, MANIFEST_JSON_CODEC.toJsonBytes(initial));
            baseProperties = ImmutableMap.<String, String>builder()
                    .put("wren.datasource.type", DUCKDB.name())
                    .put("wren.directory", mdlDir.toAbsolutePath().toString());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testStart()
            throws Exception
    {
        String initSqlPath = mdlDir.resolve("init.sql").toAbsolutePath().toString();
        Files.writeString(Path.of(initSqlPath), "CREATE SCHEMA tpch;");
        String sessionSqlPath = mdlDir.resolve("session.sql").toAbsolutePath().toString();
        Files.writeString(Path.of(sessionSqlPath), "SET s3_region = 'us-east-2';");

        baseProperties
                .put(DUCKDB_CONNECTOR_INIT_SQL_PATH, initSqlPath)
                .put(DUCKDB_CONNECTOR_SESSION_SQL_PATH, sessionSqlPath);

        assertServerStart(baseProperties.build());
    }

    @Test
    public void testStartWithIllegalInitSql()
            throws Exception
    {
        String initSqlPath = mdlDir.resolve("init.sql").toAbsolutePath().toString();
        Files.writeString(Path.of(initSqlPath), "Illegal SQL");
        String sessionSqlPath = mdlDir.resolve("session.sql").toAbsolutePath().toString();
        Files.writeString(Path.of(sessionSqlPath), "Illegal SQL");

        baseProperties
                .put(DUCKDB_CONNECTOR_INIT_SQL_PATH, initSqlPath)
                .put(DUCKDB_CONNECTOR_SESSION_SQL_PATH, sessionSqlPath);

        assertServerStart(baseProperties.build());
    }

    private void assertServerStart(Map<String, String> props)
    {
        assertThatCode(() -> TestingWrenServer.builder().setRequiredConfigs(props).build())
                .doesNotThrowAnyException();
    }
}
