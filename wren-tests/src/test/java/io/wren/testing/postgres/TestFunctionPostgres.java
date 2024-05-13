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
import io.wren.testing.AbstractFunctionTest;
import io.wren.testing.TestingPostgreSqlServer;
import io.wren.testing.TestingWrenServer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.wren.base.config.PostgresConfig.POSTGRES_JDBC_URL;
import static io.wren.base.config.PostgresConfig.POSTGRES_PASSWORD;
import static io.wren.base.config.PostgresConfig.POSTGRES_USER;
import static io.wren.base.config.WrenConfig.DataSourceType.POSTGRES;
import static io.wren.base.config.WrenConfig.WREN_DATASOURCE_TYPE;
import static io.wren.base.config.WrenConfig.WREN_DIRECTORY;
import static io.wren.base.dto.Manifest.MANIFEST_JSON_CODEC;

public class TestFunctionPostgres
        extends AbstractFunctionTest
{
    TestingPostgreSqlServer testingPostgreSqlServer;

    @Override
    protected TestingWrenServer createWrenServer()
    {
        testingPostgreSqlServer = new TestingPostgreSqlServer();
        Path mdlDir;
        try {
            mdlDir = Files.createTempDirectory("wrenmdls");
            Path wrenMDLFilePath = mdlDir.resolve("wrenmdl.json");
            Files.write(wrenMDLFilePath, MANIFEST_JSON_CODEC.toJsonBytes(Manifest.builder().setCatalog("wren").setSchema("test").build()));
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put(WREN_DIRECTORY, mdlDir.toAbsolutePath().toString())
                .put(WREN_DATASOURCE_TYPE, POSTGRES.name())
                .put(POSTGRES_JDBC_URL, testingPostgreSqlServer.getJdbcUrl())
                .put(POSTGRES_USER, testingPostgreSqlServer.getUser())
                .put(POSTGRES_PASSWORD, testingPostgreSqlServer.getPassword());

        return TestingWrenServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
    }
}
