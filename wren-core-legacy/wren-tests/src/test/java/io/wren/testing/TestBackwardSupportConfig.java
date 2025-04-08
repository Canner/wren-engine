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
import io.wren.base.dto.Manifest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.wren.base.config.BigQueryConfig.BIGQUERY_BUCKET_NAME;
import static io.wren.base.config.BigQueryConfig.BIGQUERY_CRENDITALS_FILE;
import static io.wren.base.config.BigQueryConfig.BIGQUERY_CRENDITALS_KEY;
import static io.wren.base.config.BigQueryConfig.BIGQUERY_LOCATION;
import static io.wren.base.config.BigQueryConfig.BIGQUERY_METADATA_SCHEMA_PREFIX;
import static io.wren.base.config.BigQueryConfig.BIGQUERY_PROJECT_ID;
import static io.wren.base.config.ConfigManager.ConfigEntry.configEntry;
import static io.wren.base.config.DuckdbS3StyleStorageConfig.DUCKDB_STORAGE_ACCESS_KEY;
import static io.wren.base.config.DuckdbS3StyleStorageConfig.DUCKDB_STORAGE_ENDPOINT;
import static io.wren.base.config.DuckdbS3StyleStorageConfig.DUCKDB_STORAGE_REGION;
import static io.wren.base.config.DuckdbS3StyleStorageConfig.DUCKDB_STORAGE_SECRET_KEY;
import static io.wren.base.config.DuckdbS3StyleStorageConfig.DUCKDB_STORAGE_URL_STYLE;
import static io.wren.base.config.PostgresConfig.POSTGRES_JDBC_URL;
import static io.wren.base.config.PostgresConfig.POSTGRES_PASSWORD;
import static io.wren.base.config.PostgresConfig.POSTGRES_USER;
import static io.wren.base.config.PostgresWireProtocolConfig.PG_WIRE_PROTOCOL_AUTH_FILE;
import static io.wren.base.config.PostgresWireProtocolConfig.PG_WIRE_PROTOCOL_ENABLED;
import static io.wren.base.config.PostgresWireProtocolConfig.PG_WIRE_PROTOCOL_NETTY_THREAD_COUNT;
import static io.wren.base.config.PostgresWireProtocolConfig.PG_WIRE_PROTOCOL_PORT;
import static io.wren.base.config.PostgresWireProtocolConfig.PG_WIRE_PROTOCOL_SSL_ENABLED;
import static io.wren.base.config.SQLGlotConfig.SQLGLOT_PORT;
import static io.wren.base.config.SnowflakeConfig.SNOWFLAKE_DATABASE;
import static io.wren.base.config.SnowflakeConfig.SNOWFLAKE_JDBC_URL;
import static io.wren.base.config.SnowflakeConfig.SNOWFLAKE_PASSWORD;
import static io.wren.base.config.SnowflakeConfig.SNOWFLAKE_ROLE;
import static io.wren.base.config.SnowflakeConfig.SNOWFLAKE_SCHEMA;
import static io.wren.base.config.SnowflakeConfig.SNOWFLAKE_USER;
import static io.wren.base.config.SnowflakeConfig.SNOWFLAKE_WAREHOUSE;
import static io.wren.base.config.WrenConfig.DataSourceType.DUCKDB;
import static io.wren.base.config.WrenConfig.WREN_DATASOURCE_TYPE;
import static io.wren.base.config.WrenConfig.WREN_DIRECTORY;
import static io.wren.testing.AbstractTestFramework.withDefaultCatalogSchema;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestBackwardSupportConfig
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
                .put(WREN_DATASOURCE_TYPE, DUCKDB.name())
                // backward support cases
                .put(DUCKDB_STORAGE_ENDPOINT, "http://localhost:9000")
                .put(DUCKDB_STORAGE_ACCESS_KEY, "minioadmin")
                .put(DUCKDB_STORAGE_SECRET_KEY, "minioadmin")
                .put(DUCKDB_STORAGE_REGION, "us-east-1")
                .put(DUCKDB_STORAGE_URL_STYLE, "path")
                .put(SQLGLOT_PORT, "8000")
                .put(BIGQUERY_CRENDITALS_KEY, "http://localhost:9000")
                .put(BIGQUERY_CRENDITALS_FILE, "minioadmin")
                .put(BIGQUERY_PROJECT_ID, "us-east-1")
                .put(BIGQUERY_LOCATION, "path")
                .put(BIGQUERY_BUCKET_NAME, "8000")
                .put(BIGQUERY_METADATA_SCHEMA_PREFIX, "http://localhost:9000")
                .put(POSTGRES_JDBC_URL, "minioadmin")
                .put(POSTGRES_USER, "minioadmin")
                .put(POSTGRES_PASSWORD, "us-east-1")
                .put(PG_WIRE_PROTOCOL_ENABLED, "false")
                .put(PG_WIRE_PROTOCOL_SSL_ENABLED, "false")
                .put(PG_WIRE_PROTOCOL_NETTY_THREAD_COUNT, "4")
                .put(PG_WIRE_PROTOCOL_AUTH_FILE, "minioadmin")
                .put(PG_WIRE_PROTOCOL_PORT, "8080")
                .put(SNOWFLAKE_JDBC_URL, "minioadmin")
                .put(SNOWFLAKE_USER, "minioadmin")
                .put(SNOWFLAKE_PASSWORD, "minioadmin")
                .put(SNOWFLAKE_ROLE, "minioadmin")
                .put(SNOWFLAKE_WAREHOUSE, "minioadmin")
                .put(SNOWFLAKE_DATABASE, "minioadmin")
                .put(SNOWFLAKE_SCHEMA, "minioadmin");

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
}
