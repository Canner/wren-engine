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

package io.accio.base.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.accio.base.AccioException;
import io.accio.base.client.duckdb.CacheStorageConfig;
import io.accio.base.client.duckdb.DuckDBConfig;
import io.accio.base.client.duckdb.DuckdbS3StyleStorageConfig;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.io.File;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.accio.base.client.duckdb.DuckDBConfig.DUCKDB_CACHE_TASK_RETRY_DELAY;
import static io.accio.base.client.duckdb.DuckDBConfig.DUCKDB_HOME_DIRECTORY;
import static io.accio.base.client.duckdb.DuckDBConfig.DUCKDB_MAX_CACHE_QUERY_TIMEOUT;
import static io.accio.base.client.duckdb.DuckDBConfig.DUCKDB_MAX_CONCURRENT_QUERIES;
import static io.accio.base.client.duckdb.DuckDBConfig.DUCKDB_MAX_CONCURRENT_TASKS;
import static io.accio.base.client.duckdb.DuckDBConfig.DUCKDB_MEMORY_LIMIT;
import static io.accio.base.client.duckdb.DuckDBConfig.DUCKDB_TEMP_DIRECTORY;
import static io.accio.base.client.duckdb.DuckdbS3StyleStorageConfig.DUCKDB_STORAGE_ACCESS_KEY;
import static io.accio.base.client.duckdb.DuckdbS3StyleStorageConfig.DUCKDB_STORAGE_ENDPOINT;
import static io.accio.base.client.duckdb.DuckdbS3StyleStorageConfig.DUCKDB_STORAGE_REGION;
import static io.accio.base.client.duckdb.DuckdbS3StyleStorageConfig.DUCKDB_STORAGE_SECRET_KEY;
import static io.accio.base.client.duckdb.DuckdbS3StyleStorageConfig.DUCKDB_STORAGE_URL_STYLE;
import static io.accio.base.config.AccioConfig.ACCIO_DATASOURCE_TYPE;
import static io.accio.base.config.AccioConfig.ACCIO_DIRECTORY;
import static io.accio.base.config.AccioConfig.ACCIO_ENABLE_DYNAMIC_FIELDS;
import static io.accio.base.config.AccioConfig.ACCIO_FILE;
import static io.accio.base.config.BigQueryConfig.BIGQUERY_BUCKET_NAME;
import static io.accio.base.config.BigQueryConfig.BIGQUERY_CRENDITALS_FILE;
import static io.accio.base.config.BigQueryConfig.BIGQUERY_CRENDITALS_KEY;
import static io.accio.base.config.BigQueryConfig.BIGQUERY_LOCATION;
import static io.accio.base.config.BigQueryConfig.BIGQUERY_METADATA_SCHEMA_PREFIX;
import static io.accio.base.config.BigQueryConfig.BIGQUERY_PARENT_PROJECT_ID;
import static io.accio.base.config.BigQueryConfig.BIGQUERY_PROJECT_ID;
import static io.accio.base.config.PostgresConfig.POSTGRES_JDBC_URL;
import static io.accio.base.config.PostgresConfig.POSTGRES_PASSWORD;
import static io.accio.base.config.PostgresConfig.POSTRES_USER;
import static io.accio.base.config.PostgresWireProtocolConfig.PG_WIRE_PROTOCOL_AUTH_FILE;
import static io.accio.base.config.PostgresWireProtocolConfig.PG_WIRE_PROTOCOL_NETTY_THREAD_COUNT;
import static io.accio.base.config.PostgresWireProtocolConfig.PG_WIRE_PROTOCOL_PORT;
import static io.accio.base.config.PostgresWireProtocolConfig.PG_WIRE_PROTOCOL_SSL_ENABLED;
import static io.accio.base.metadata.StandardErrorCode.NOT_FOUND;
import static java.util.stream.Collectors.toList;

public class ConfigManager
{
    private Optional<AccioConfig> accioConfig;
    private Optional<PostgresConfig> postgresConfig;
    private Optional<BigQueryConfig> bigQueryConfig;
    private Optional<DuckDBConfig> duckDBConfig;
    private Optional<PostgresWireProtocolConfig> postgresWireProtocolConfig;
    private Optional<DuckdbS3StyleStorageConfig> duckdbS3StyleStorageConfig;

    private ConcurrentHashMap<String, String> configs = new ConcurrentHashMap<>();

    @Inject
    public ConfigManager(
            AccioConfig accioConfig,
            PostgresConfig postgresConfig,
            BigQueryConfig bigQueryConfig,
            DuckDBConfig duckDBConfig,
            PostgresWireProtocolConfig postgresWireProtocolConfig,
            DuckdbS3StyleStorageConfig duckdbS3StyleStorageConfig)
    {
        this.accioConfig = Optional.of(accioConfig);
        this.postgresConfig = Optional.of(postgresConfig);
        this.bigQueryConfig = Optional.of(bigQueryConfig);
        this.duckDBConfig = Optional.of(duckDBConfig);
        this.postgresWireProtocolConfig = Optional.of(postgresWireProtocolConfig);
        this.duckdbS3StyleStorageConfig = Optional.of(duckdbS3StyleStorageConfig);

        initConfig(
                accioConfig,
                postgresConfig,
                bigQueryConfig,
                duckDBConfig,
                postgresWireProtocolConfig,
                duckdbS3StyleStorageConfig);
    }

    private void initConfig(
            AccioConfig accioConfig,
            PostgresConfig postgresConfig,
            BigQueryConfig bigQueryConfig,
            DuckDBConfig duckDBConfig,
            PostgresWireProtocolConfig postgresWireProtocolConfig,
            DuckdbS3StyleStorageConfig duckdbS3StyleStorageConfig)
    {
        initConfig(ACCIO_FILE, accioConfig.getAccioMDLFile().map(File::getAbsolutePath).orElse(null));
        initConfig(ACCIO_DIRECTORY, accioConfig.getAccioMDLDirectory().getPath());
        initConfig(ACCIO_DATASOURCE_TYPE, Optional.ofNullable(accioConfig.getDataSourceType()).map(Enum::name).orElse(null));
        initConfig(ACCIO_ENABLE_DYNAMIC_FIELDS, Boolean.toString(accioConfig.getEnableDynamicFields()));
        initConfig(DUCKDB_STORAGE_ENDPOINT, duckdbS3StyleStorageConfig.getEndpoint());
        initConfig(DUCKDB_STORAGE_ACCESS_KEY, duckdbS3StyleStorageConfig.getAccessKey().orElse(null));
        initConfig(DUCKDB_STORAGE_SECRET_KEY, duckdbS3StyleStorageConfig.getSecretKey().orElse(null));
        initConfig(DUCKDB_STORAGE_REGION, duckdbS3StyleStorageConfig.getRegion().orElse(null));
        initConfig(DUCKDB_STORAGE_URL_STYLE, duckdbS3StyleStorageConfig.getUrlStyle());
        initConfig(DUCKDB_MEMORY_LIMIT, duckDBConfig.getMemoryLimit().toString());
        initConfig(DUCKDB_HOME_DIRECTORY, duckDBConfig.getHomeDirectory());
        initConfig(DUCKDB_TEMP_DIRECTORY, duckDBConfig.getTempDirectory());
        initConfig(DUCKDB_MAX_CONCURRENT_TASKS, Integer.toString(duckDBConfig.getMaxConcurrentTasks()));
        initConfig(DUCKDB_MAX_CONCURRENT_QUERIES, Integer.toString(duckDBConfig.getMaxConcurrentMetadataQueries()));
        initConfig(DUCKDB_MAX_CACHE_QUERY_TIMEOUT, Long.toString(duckDBConfig.getMaxCacheQueryTimeout()));
        initConfig(DUCKDB_CACHE_TASK_RETRY_DELAY, Long.toString(duckDBConfig.getCacheTaskRetryDelay()));
        initConfig(PG_WIRE_PROTOCOL_PORT, postgresWireProtocolConfig.getPort());
        initConfig(PG_WIRE_PROTOCOL_SSL_ENABLED, Boolean.toString(postgresWireProtocolConfig.isSslEnable()));
        initConfig(PG_WIRE_PROTOCOL_NETTY_THREAD_COUNT, Integer.toString(postgresWireProtocolConfig.getNettyThreadCount()));
        initConfig(PG_WIRE_PROTOCOL_AUTH_FILE, postgresWireProtocolConfig.getAuthFile().getPath());
        initConfig(BIGQUERY_CRENDITALS_KEY, bigQueryConfig.getCredentialsKey().orElse(null));
        initConfig(BIGQUERY_CRENDITALS_FILE, bigQueryConfig.getCredentialsFile().orElse(null));
        initConfig(BIGQUERY_PROJECT_ID, bigQueryConfig.getProjectId().orElse(null));
        initConfig(BIGQUERY_PARENT_PROJECT_ID, bigQueryConfig.getParentProjectId().orElse(null));
        initConfig(BIGQUERY_LOCATION, bigQueryConfig.getLocation().orElse(null));
        initConfig(BIGQUERY_BUCKET_NAME, bigQueryConfig.getBucketName().orElse(null));
        initConfig(BIGQUERY_METADATA_SCHEMA_PREFIX, bigQueryConfig.getMetadataSchemaPrefix());
        initConfig(POSTGRES_JDBC_URL, postgresConfig.getJdbcUrl());
        initConfig(POSTRES_USER, postgresConfig.getUser());
        initConfig(POSTGRES_PASSWORD, postgresConfig.getPassword());
    }

    private void initConfig(String key, String value)
    {
        if (value == null) {
            // ConcurrentHashMap are not allowed value to be null, use empty string instead.
            configs.put(key, "");
        }
        else {
            configs.put(key, value);
        }
    }

    public <T> T getConfig(Class<T> config)
    {
        if (config == AccioConfig.class) {
            return (T) accioConfig.orElseGet(() -> {
                AccioConfig result = new AccioConfig();
                result.setAccioMDLFile(new File(configs.get(ACCIO_FILE)));
                result.setAccioMDLDirectory(new File(configs.get(ACCIO_DIRECTORY)));
                result.setDataSourceType(AccioConfig.DataSourceType.valueOf(configs.get(ACCIO_DATASOURCE_TYPE).toUpperCase(Locale.ROOT)));
                result.setEnableDynamicFields(Boolean.parseBoolean(configs.get(ACCIO_ENABLE_DYNAMIC_FIELDS)));
                accioConfig = Optional.of(result);
                return result;
            });
        }
        if (config == BigQueryConfig.class) {
            return (T) bigQueryConfig.orElseGet(() -> {
                BigQueryConfig result = new BigQueryConfig();
                result.setCredentialsKey(configs.get(BIGQUERY_CRENDITALS_KEY));
                result.setCredentialsFile(configs.get(BIGQUERY_CRENDITALS_FILE));
                result.setProjectId(configs.get(BIGQUERY_PROJECT_ID));
                result.setParentProjectId(configs.get(BIGQUERY_PARENT_PROJECT_ID));
                result.setLocation(configs.get(BIGQUERY_LOCATION));
                result.setBucketName(configs.get(BIGQUERY_BUCKET_NAME));
                result.setMetadataSchemaPrefix(configs.get(BIGQUERY_METADATA_SCHEMA_PREFIX));
                bigQueryConfig = Optional.of(result);
                return result;
            });
        }
        if (config == PostgresConfig.class) {
            return (T) postgresConfig.orElseGet(() -> {
                PostgresConfig result = new PostgresConfig();
                result.setJdbcUrl(configs.get(POSTGRES_JDBC_URL));
                result.setUser(configs.get(POSTRES_USER));
                result.setPassword(configs.get(POSTGRES_PASSWORD));
                postgresConfig = Optional.of(result);
                return result;
            });
        }
        if (config == DuckDBConfig.class) {
            return (T) duckDBConfig.orElseGet(() -> {
                DuckDBConfig result = new DuckDBConfig();
                result.setMemoryLimit(DataSize.valueOf(configs.get(DUCKDB_MEMORY_LIMIT)));
                result.setHomeDirectory(configs.get(DUCKDB_HOME_DIRECTORY));
                result.setTempDirectory(configs.get(DUCKDB_TEMP_DIRECTORY));
                result.setMaxConcurrentTasks(Integer.parseInt(configs.get(DUCKDB_MAX_CONCURRENT_TASKS)));
                result.setMaxConcurrentMetadataQueries(Integer.parseInt(configs.get(DUCKDB_MAX_CONCURRENT_QUERIES)));
                result.setMaxCacheQueryTimeout(Integer.parseInt(configs.get(DUCKDB_MAX_CACHE_QUERY_TIMEOUT)));
                result.setCacheTaskRetryDelay(Integer.parseInt(configs.get(DUCKDB_CACHE_TASK_RETRY_DELAY)));
                duckDBConfig = Optional.of(result);
                return result;
            });
        }
        if (config == PostgresWireProtocolConfig.class) {
            return (T) postgresWireProtocolConfig.orElseGet(() -> {
                PostgresWireProtocolConfig result = new PostgresWireProtocolConfig();
                result.setPort(configs.get(PG_WIRE_PROTOCOL_PORT));
                result.setSslEnable(Boolean.parseBoolean(configs.get(PG_WIRE_PROTOCOL_SSL_ENABLED)));
                result.setNettyThreadCount(Integer.parseInt(configs.get(PG_WIRE_PROTOCOL_NETTY_THREAD_COUNT)));
                result.setAuthFile(new File(configs.get(PG_WIRE_PROTOCOL_AUTH_FILE)));
                postgresWireProtocolConfig = Optional.of(result);
                return result;
            });
        }
        if (config == CacheStorageConfig.class &&
                accioConfig.map(AccioConfig::getDataSourceType).stream().anyMatch(type -> type == AccioConfig.DataSourceType.BIGQUERY)) {
            return (T) duckdbS3StyleStorageConfig.orElseGet(() -> {
                DuckdbS3StyleStorageConfig result = new DuckdbS3StyleStorageConfig();
                result.setEndpoint(configs.get(DUCKDB_STORAGE_ENDPOINT));
                result.setAccessKey(configs.get(DUCKDB_STORAGE_ACCESS_KEY));
                result.setSecretKey(configs.get(DUCKDB_STORAGE_SECRET_KEY));
                result.setRegion(configs.get(DUCKDB_STORAGE_REGION));
                result.setUrlStyle(configs.get(DUCKDB_STORAGE_URL_STYLE));
                duckdbS3StyleStorageConfig = Optional.of(result);
                return result;
            });
        }
        throw new RuntimeException("Unknown config class: " + config.getName());
    }

    public synchronized void setConfigs(List<ConfigEntry> configEntries)
    {
        configEntries.forEach(entry -> setConfigInternal(entry.getName(), entry.getValue()));
        resetCache();
    }

    private void setConfigInternal(String key, String value)
    {
        // Only allow set the config that already exists.
        if (configs.containsKey(key)) {
            configs.put(key, value);
        }
        else {
            throw new AccioException(NOT_FOUND, "Config not found: " + key);
        }
    }

    private synchronized void resetCache()
    {
        accioConfig = Optional.empty();
        postgresConfig = Optional.empty();
        bigQueryConfig = Optional.empty();
        duckDBConfig = Optional.empty();
        postgresWireProtocolConfig = Optional.empty();
        duckdbS3StyleStorageConfig = Optional.empty();
    }

    public synchronized void reset()
    {
        configs.clear();
        initConfig(
                new AccioConfig(),
                new PostgresConfig(),
                new BigQueryConfig(),
                new DuckDBConfig(),
                new PostgresWireProtocolConfig(),
                new DuckdbS3StyleStorageConfig());
        resetCache();
    }

    public List<ConfigEntry> getConfigs()
    {
        return configs.entrySet().stream().map(entry -> new ConfigEntry(entry.getKey(), entry.getValue())).collect(toList());
    }

    public ConfigEntry getConfig(String key)
    {
        if (configs.containsKey(key)) {
            return new ConfigEntry(key, configs.get(key));
        }
        throw new AccioException(NOT_FOUND, "Config not found: " + key);
    }

    public static class ConfigEntry
    {
        public static ConfigEntry configEntry(String name, String value)
        {
            return new ConfigEntry(name, value);
        }

        private final String name;
        private final String value;

        @JsonCreator
        public ConfigEntry(
                @JsonProperty("name") String name,
                @JsonProperty("value") String value)
        {
            this.name = name;
            this.value = value == null || value.isEmpty() ? null : value;
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public String getValue()
        {
            return value;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ConfigEntry that = (ConfigEntry) o;
            return Objects.equals(name, that.name) && Objects.equals(value, that.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, value);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("name", name)
                    .add("value", value)
                    .toString();
        }
    }
}
