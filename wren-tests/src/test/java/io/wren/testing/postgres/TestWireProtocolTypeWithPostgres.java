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

package io.wren.testing.postgres;

import com.google.common.collect.ImmutableMap;
import io.wren.base.dto.Manifest;
import io.wren.testing.AbstractWireProtocolTypeTest;
import io.wren.testing.TestingPostgreSqlServer;
import io.wren.testing.TestingWrenServer;

import java.nio.file.Files;
import java.nio.file.Path;

public class TestWireProtocolTypeWithPostgres
        extends AbstractWireProtocolTypeTest
{
    protected static final Manifest DEFAULT_MANIFEST = Manifest.builder()
            .setCatalog("tpch")
            .setSchema("tpch")
            .build();

    @Override
    protected TestingWrenServer createWrenServer()
    {
        TestingPostgreSqlServer testingPostgreSqlServer = new TestingPostgreSqlServer();
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("postgres.jdbc.url", testingPostgreSqlServer.getJdbcUrl())
                .put("postgres.user", testingPostgreSqlServer.getUser())
                .put("postgres.password", testingPostgreSqlServer.getPassword())
                .put("wren.datasource.type", "POSTGRES");

        try {
            Path dir = Files.createTempDirectory(getWrenDirectory());
            if (getWrenMDLPath().isPresent()) {
                Files.copy(Path.of(getWrenMDLPath().get()), dir.resolve("mdl.json"));
            }
            else {
                Files.write(dir.resolve("manifest.json"), Manifest.MANIFEST_JSON_CODEC.toJsonBytes(DEFAULT_MANIFEST));
            }
            properties.put("wren.directory", dir.toString());
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        return TestingWrenServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
    }

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
}
