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

package io.wren.testing.snowflake;

import com.google.common.collect.ImmutableMap;
import io.wren.base.config.SQLGlotConfig;
import io.wren.base.dto.Manifest;
import io.wren.testing.AbstractWireProtocolTest;
import io.wren.testing.TestingSQLGlotServer;
import io.wren.testing.TestingWrenServer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.wren.base.config.SQLGlotConfig.SQLGLOT_PORT;
import static io.wren.base.config.SQLGlotConfig.createConfigWithFreePort;
import static io.wren.base.config.SnowflakeConfig.SNOWFLAKE_DATABASE;
import static io.wren.base.config.SnowflakeConfig.SNOWFLAKE_JDBC_URL;
import static io.wren.base.config.SnowflakeConfig.SNOWFLAKE_PASSWORD;
import static io.wren.base.config.SnowflakeConfig.SNOWFLAKE_SCHEMA;
import static io.wren.base.config.SnowflakeConfig.SNOWFLAKE_USER;
import static io.wren.base.config.WrenConfig.WREN_DATASOURCE_TYPE;
import static io.wren.main.sqlglot.SQLGlot.Dialect.SNOWFLAKE;
import static java.lang.System.getenv;
import static java.util.Objects.requireNonNull;

public abstract class AbstractWireProtocolTestWithSnowflake
        extends AbstractWireProtocolTest
{
    protected TestingSQLGlotServer testingSQLGlotServer;

    protected static final Manifest DEFAULT_MANIFEST = Manifest.builder()
            .setCatalog("canner-cml")
            .setSchema("tpch_tiny")
            .build();

    @Override
    protected TestingWrenServer createWrenServer()
            throws Exception
    {
        ImmutableMap.Builder<String, String> propBuilder = ImmutableMap.<String, String>builder()
                .put(WREN_DATASOURCE_TYPE, SNOWFLAKE.name())
                .put(SNOWFLAKE_JDBC_URL, getenv("SNOWFLAKE_JDBC_URL"))
                .put(SNOWFLAKE_USER, getenv("SNOWFLAKE_USER"))
                .put(SNOWFLAKE_PASSWORD, getenv("SNOWFLAKE_PASSWORD"))
                .put(SNOWFLAKE_DATABASE, "SNOWFLAKE_SAMPLE_DATA")
                .put(SNOWFLAKE_SCHEMA, "TPCH_SF1")
                .put(SQLGLOT_PORT, String.valueOf(testingSQLGlotServer.getPort()));

        try {
            Path dir = Files.createTempDirectory("wren-snowflake-test");
            if (getWrenMDLPath().isPresent()) {
                Files.copy(Path.of(getWrenMDLPath().get()), dir.resolve("mdl.json"));
            }
            else {
                Files.write(dir.resolve("manifest.json"), Manifest.MANIFEST_JSON_CODEC.toJsonBytes(DEFAULT_MANIFEST));
            }
            propBuilder.put("wren.directory", dir.toString());
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        Map<String, String> properties = new HashMap<>(propBuilder.build());
        properties.putAll(properties());

        return TestingWrenServer.builder()
                .setRequiredConfigs(properties)
                .build();
    }

    protected Map<String, String> properties()
    {
        return ImmutableMap.of();
    }

    protected TestingSQLGlotServer prepareSQLGlot()
    {
        SQLGlotConfig config = createConfigWithFreePort();
        return new TestingSQLGlotServer(config);
    }

    protected Optional<String> getWrenMDLPath()
    {
        return Optional.of(requireNonNull(getClass().getClassLoader().getResource("snowflake/mdl.json")).getPath());
    }

    @Override
    protected String getDefaultCatalog()
    {
        return "canner-cml";
    }

    @Override
    protected String getDefaultSchema()
    {
        return "tpch_tiny";
    }
}
