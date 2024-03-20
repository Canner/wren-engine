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
import com.google.common.io.Resources;
import com.google.inject.Key;
import io.wren.main.connector.duckdb.DuckDBMetadata;
import io.wren.testing.AbstractWireProtocolTest;
import io.wren.testing.TestingWrenServer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.wren.base.config.WrenConfig.DataSourceType.DUCKDB;
import static io.wren.base.config.WrenConfig.WREN_DATASOURCE_TYPE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public abstract class AbstractWireProtocolTestWithDuckDB
        extends AbstractWireProtocolTest
{
    @Override
    protected TestingWrenServer createWrenServer()
            throws Exception
    {
        ImmutableMap.Builder<String, String> propBuilder = ImmutableMap.<String, String>builder()
                .put(WREN_DATASOURCE_TYPE, DUCKDB.name())
                .put("pg-wire-protocol.auth.file", requireNonNull(getClass().getClassLoader().getResource("accounts")).getPath());

        Path dir = Files.createTempDirectory(getWrenDirectory());
        if (getWrenMDLPath().isPresent()) {
            Files.copy(Path.of(getWrenMDLPath().get()), dir.resolve("mdl.json"));
        }
        propBuilder.put("wren.directory", dir.toString());

        Map<String, String> properties = new HashMap<>(propBuilder.build());
        properties.putAll(properties());

        TestingWrenServer wrenServer = TestingWrenServer.builder()
                .setRequiredConfigs(properties)
                .build();

        if (properties.get(WREN_DATASOURCE_TYPE).equals(DUCKDB.name())) {
            initDuckDB(wrenServer);
        }

        return wrenServer;
    }

    protected Map<String, String> properties()
    {
        return ImmutableMap.of();
    }

    protected void initDuckDB(TestingWrenServer wrenServer)
            throws Exception
    {
        ClassLoader classLoader = getClass().getClassLoader();
        String initSQL = Resources.toString(requireNonNull(classLoader.getResource("duckdb/init.sql")).toURI().toURL(), UTF_8);
        initSQL = initSQL.replaceAll("basePath", requireNonNull(classLoader.getResource("duckdb/data")).getPath());
        DuckDBMetadata metadata = wrenServer.getInstance(Key.get(DuckDBMetadata.class));
        metadata.setInitSQL(initSQL);
        metadata.reload();
    }

    protected Optional<String> getWrenMDLPath()
    {
        return Optional.of(requireNonNull(getClass().getClassLoader().getResource("duckdb/mdl.json")).getPath());
    }

    @Override
    protected String getDefaultCatalog()
    {
        return "memory";
    }

    @Override
    protected String getDefaultSchema()
    {
        return "tpch";
    }

    @Override
    protected void cleanup()
    {
        // Do nothing
    }
}
