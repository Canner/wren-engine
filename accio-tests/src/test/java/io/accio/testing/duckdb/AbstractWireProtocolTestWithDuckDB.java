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

package io.accio.testing.duckdb;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.inject.Key;
import io.accio.base.client.ForConnector;
import io.accio.base.client.duckdb.DuckdbClient;
import io.accio.testing.AbstractWireProtocolTest;
import io.accio.testing.TestingAccioServer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static io.accio.base.config.AccioConfig.DataSourceType.DUCKDB;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public abstract class AbstractWireProtocolTestWithDuckDB
        extends AbstractWireProtocolTest
{
    @Override
    protected TestingAccioServer createAccioServer()
            throws Exception
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("accio.datasource.type", DUCKDB.name())
                .put("pg-wire-protocol.auth.file", requireNonNull(getClass().getClassLoader().getResource("accounts")).getPath());

        Path dir = Files.createTempDirectory(getAccioDirectory());
        if (getAccioMDLPath().isPresent()) {
            Files.copy(Path.of(getAccioMDLPath().get()), dir.resolve("mdl.json"));
        }
        properties.put("accio.directory", dir.toString());

        TestingAccioServer accioServer = TestingAccioServer.builder()
                .setRequiredConfigs(properties.build())
                .build();

        initDuckDB(accioServer);

        return accioServer;
    }

    private void initDuckDB(TestingAccioServer accioServer)
            throws Exception
    {
        ClassLoader classLoader = getClass().getClassLoader();
        String initSQL = Resources.toString(requireNonNull(classLoader.getResource("duckdb/init.sql")).toURI().toURL(), UTF_8);
        initSQL = initSQL.replaceAll("basePath", requireNonNull(classLoader.getResource("duckdb/data")).getPath());
        accioServer.getInstance(Key.get(DuckdbClient.class, ForConnector.class)).executeDDL(initSQL);
    }

    protected Optional<String> getAccioMDLPath()
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
