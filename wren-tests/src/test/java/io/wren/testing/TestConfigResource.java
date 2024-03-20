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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.wren.base.client.duckdb.DuckDBConfig;
import io.wren.base.config.ConfigManager;
import io.wren.base.config.PostgresWireProtocolConfig;
import io.wren.base.config.WrenConfig;
import io.wren.base.dto.Manifest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static io.wren.base.client.duckdb.DuckDBConfig.DUCKDB_CACHE_TASK_RETRY_DELAY;
import static io.wren.base.client.duckdb.DuckDBConfig.DUCKDB_MAX_CACHE_QUERY_TIMEOUT;
import static io.wren.base.client.duckdb.DuckDBConfig.DUCKDB_MAX_CONCURRENT_METADATA_QUERIES;
import static io.wren.base.client.duckdb.DuckDBConfig.DUCKDB_MAX_CONCURRENT_TASKS;
import static io.wren.base.client.duckdb.DuckDBConfig.DUCKDB_MEMORY_LIMIT;
import static io.wren.base.config.ConfigManager.ConfigEntry.configEntry;
import static io.wren.base.config.PostgresWireProtocolConfig.PG_WIRE_PROTOCOL_AUTH_FILE;
import static io.wren.base.config.PostgresWireProtocolConfig.PG_WIRE_PROTOCOL_NETTY_THREAD_COUNT;
import static io.wren.base.config.PostgresWireProtocolConfig.PG_WIRE_PROTOCOL_PORT;
import static io.wren.base.config.PostgresWireProtocolConfig.PG_WIRE_PROTOCOL_SSL_ENABLED;
import static io.wren.base.config.WrenConfig.DataSourceType.DUCKDB;
import static io.wren.base.config.WrenConfig.WREN_DATASOURCE_TYPE;
import static io.wren.base.config.WrenConfig.WREN_DIRECTORY;
import static io.wren.base.dto.Manifest.MANIFEST_JSON_CODEC;
import static io.wren.testing.AbstractTestFramework.withDefaultCatalogSchema;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class TestConfigResource
        extends RequireWrenServer
{
    private Path mdlDir;

    @Override
    protected TestingWrenServer createWrenServer()
    {
        Manifest manifest = withDefaultCatalogSchema()
                .build();

        try {
            mdlDir = Files.createTempDirectory("wrenmdls");
            Path wrenMDLFilePath = mdlDir.resolve("wrenmdl.json");
            Files.write(wrenMDLFilePath, MANIFEST_JSON_CODEC.toJsonBytes(manifest));
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put(WREN_DIRECTORY, mdlDir.toAbsolutePath().toString())
                .put(WREN_DATASOURCE_TYPE, DUCKDB.name());

        return TestingWrenServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
    }

    @Test
    public void testGetConfigs()
    {
        assertThat(getConfigs().size()).isGreaterThan(2);
        assertThat(getConfig(WREN_DIRECTORY)).isEqualTo(configEntry(WREN_DIRECTORY, mdlDir.toAbsolutePath().toString()));
        assertThat(getConfig(WREN_DATASOURCE_TYPE)).isEqualTo(configEntry(WREN_DATASOURCE_TYPE, DUCKDB.name()));

        assertThatThrownBy(() -> getConfig("notfound"))
                .hasMessageFindingMatch(".*404 Not Found.*");
        assertThatThrownBy(() -> getConfig(null))
                .hasMessageFindingMatch(".*404 Not Found.*");
    }

    @Test
    public void testuUpdateConfigs()
    {
        patchConfig(List.of(configEntry(DUCKDB_MEMORY_LIMIT, "2GB"), configEntry(DUCKDB_MAX_CONCURRENT_METADATA_QUERIES, "20")));
        assertThat(getConfig(WREN_DATASOURCE_TYPE)).isEqualTo(configEntry(WREN_DATASOURCE_TYPE, DUCKDB.name()));
        assertThat(getConfig(WREN_DIRECTORY)).isEqualTo(configEntry(WREN_DIRECTORY, mdlDir.toAbsolutePath().toString()));
        assertThat(getConfig(DUCKDB_MEMORY_LIMIT)).isEqualTo(configEntry(DUCKDB_MEMORY_LIMIT, "2GB"));

        resetConfig();
        DuckDBConfig duckDBConfig = new DuckDBConfig();
        WrenConfig wrenConfig = new WrenConfig();
        assertThat(getConfig(WREN_DATASOURCE_TYPE)).isEqualTo(configEntry(WREN_DATASOURCE_TYPE, wrenConfig.getDataSourceType().name()));
        assertThat(getConfig(WREN_DIRECTORY)).isEqualTo(configEntry(WREN_DIRECTORY, wrenConfig.getWrenMDLDirectory().getPath()));
        assertThat(getConfig(DUCKDB_MEMORY_LIMIT)).isEqualTo(configEntry(DUCKDB_MEMORY_LIMIT, duckDBConfig.getMemoryLimit().toString()));
    }

    @Test
    public void testDataSourceTypeCaseInsensitive()
    {
        patchConfig(List.of(configEntry(WREN_DATASOURCE_TYPE, "DuCkDb")));
        assertThat(getConfig(WREN_DATASOURCE_TYPE)).isEqualTo(configEntry(WREN_DATASOURCE_TYPE, "DuCkDb"));
        WrenConfig wrenConfig = server().getInstance(Key.get(ConfigManager.class)).getConfig(WrenConfig.class);
        assertThat(wrenConfig.getDataSourceType()).isEqualTo(DUCKDB);
        patchConfig(List.of(configEntry(WREN_DATASOURCE_TYPE, DUCKDB.name())));
        assertThat(getConfig(WREN_DATASOURCE_TYPE)).isEqualTo(configEntry(WREN_DATASOURCE_TYPE, DUCKDB.name()));
    }

    @Test
    public void testStaticConfigWontBeChanged()
    {
        patchConfig(List.of(
                configEntry(WREN_DIRECTORY, "fake"),
                configEntry(DUCKDB_MAX_CONCURRENT_TASKS, "100"),
                configEntry(DUCKDB_MAX_CONCURRENT_METADATA_QUERIES, "100"),
                configEntry(DUCKDB_MAX_CACHE_QUERY_TIMEOUT, "1000"),
                configEntry(DUCKDB_CACHE_TASK_RETRY_DELAY, "1000"),
                configEntry(PG_WIRE_PROTOCOL_PORT, "1234"),
                configEntry(PG_WIRE_PROTOCOL_SSL_ENABLED, "true"),
                configEntry(PG_WIRE_PROTOCOL_NETTY_THREAD_COUNT, "100"),
                configEntry(PG_WIRE_PROTOCOL_AUTH_FILE, "fake")));

        DuckDBConfig duckDBConfig = new DuckDBConfig();
        PostgresWireProtocolConfig postgresWireProtocolConfig = new PostgresWireProtocolConfig();
        assertThat(getConfig(WREN_DIRECTORY)).isEqualTo(configEntry(WREN_DIRECTORY, mdlDir.toAbsolutePath().toString()));
        assertThat(getConfig(DUCKDB_MAX_CONCURRENT_TASKS)).isEqualTo(configEntry(DUCKDB_MAX_CONCURRENT_TASKS, String.valueOf(duckDBConfig.getMaxConcurrentTasks())));
        assertThat(getConfig(DUCKDB_MAX_CONCURRENT_METADATA_QUERIES)).isEqualTo(configEntry(DUCKDB_MAX_CONCURRENT_METADATA_QUERIES, String.valueOf(duckDBConfig.getMaxConcurrentMetadataQueries())));
        assertThat(getConfig(DUCKDB_MAX_CACHE_QUERY_TIMEOUT)).isEqualTo(configEntry(DUCKDB_MAX_CACHE_QUERY_TIMEOUT, String.valueOf(duckDBConfig.getMaxCacheQueryTimeout())));
        assertThat(getConfig(DUCKDB_CACHE_TASK_RETRY_DELAY)).isEqualTo(configEntry(DUCKDB_CACHE_TASK_RETRY_DELAY, String.valueOf(duckDBConfig.getCacheTaskRetryDelay())));
        assertThat(getConfig(PG_WIRE_PROTOCOL_PORT)).isEqualTo(configEntry(PG_WIRE_PROTOCOL_PORT, String.valueOf(server().getPgHostAndPort().getPort())));
        assertThat(getConfig(PG_WIRE_PROTOCOL_SSL_ENABLED)).isEqualTo(configEntry(PG_WIRE_PROTOCOL_SSL_ENABLED, String.valueOf(postgresWireProtocolConfig.isSslEnable())));
        assertThat(getConfig(PG_WIRE_PROTOCOL_NETTY_THREAD_COUNT)).isEqualTo(configEntry(PG_WIRE_PROTOCOL_NETTY_THREAD_COUNT, String.valueOf(postgresWireProtocolConfig.getNettyThreadCount())));
        assertThat(getConfig(PG_WIRE_PROTOCOL_AUTH_FILE)).isEqualTo(configEntry(PG_WIRE_PROTOCOL_AUTH_FILE, postgresWireProtocolConfig.getAuthFile().getPath()));
    }
}
