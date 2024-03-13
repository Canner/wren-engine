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

import com.google.common.collect.ImmutableMap;
import io.accio.base.dto.Manifest;
import io.accio.testing.AbstractWireProtocolTest;
import io.accio.testing.TestingAccioServer;
import io.accio.testing.TestingPostgreSqlServer;

import java.nio.file.Files;
import java.nio.file.Path;

public abstract class AbstractWireProtocolTestWithPostgres
        extends AbstractWireProtocolTest
{
    protected static final Manifest DEFAULT_MANIFEST = Manifest.builder()
            .setCatalog("tpch")
            .setSchema("tpch")
            .build();
    private TestingPostgreSqlServer testingPostgreSqlServer;

    @Override
    protected TestingAccioServer createAccioServer()
    {
        testingPostgreSqlServer = new TestingPostgreSqlServer();
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("postgres.jdbc.url", testingPostgreSqlServer.getJdbcUrl())
                .put("postgres.user", testingPostgreSqlServer.getUser())
                .put("postgres.password", testingPostgreSqlServer.getPassword())
                .put("accio.datasource.type", "POSTGRES");

        try {
            Path dir = Files.createTempDirectory(getAccioDirectory());
            if (getAccioMDLPath().isPresent()) {
                Files.copy(Path.of(getAccioMDLPath().get()), dir.resolve("mdl.json"));
            }
            else {
                Files.write(dir.resolve("manifest.json"), Manifest.MANIFEST_JSON_CODEC.toJsonBytes(DEFAULT_MANIFEST));
            }
            properties.put("accio.directory", dir.toString());
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return TestingAccioServer.builder()
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
