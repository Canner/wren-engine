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
import io.wren.base.WrenMDL;
import io.wren.base.dto.Manifest;
import io.wren.testing.AbstractWireProtocolTest;
import io.wren.testing.TestingWrenServer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.wren.base.config.WrenConfig.DataSourceType.DUCKDB;
import static io.wren.base.config.WrenConfig.WREN_DATASOURCE_TYPE;
import static java.util.Objects.requireNonNull;

public abstract class AbstractWireProtocolTestWithDuckDB
        extends AbstractWireProtocolTest
{
    private Map<String, String> properties;

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
        else {
            Files.write(dir.resolve("manifest.json"),
                    Manifest.MANIFEST_JSON_CODEC.toJsonBytes(getManifest().orElse(WrenMDL.EMPTY.getManifest())));
        }
        propBuilder.put("wren.directory", dir.toString());
        propBuilder.put("pg-wire-protocol.enabled", "true");

        properties = new HashMap<>(propBuilder.build());
        properties.putAll(properties());

        return TestingWrenServer.builder()
                .setRequiredConfigs(properties)
                .build();
    }

    @Override
    protected void prepare()
    {
        if (properties.get(WREN_DATASOURCE_TYPE).equals(DUCKDB.name())) {
            initDuckDB();
        }
    }

    protected Map<String, String> properties()
    {
        return ImmutableMap.of();
    }

    protected Optional<Manifest> getManifest()
    {
        return Optional.empty();
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
