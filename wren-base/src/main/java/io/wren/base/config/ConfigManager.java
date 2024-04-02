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

package io.wren.base.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.wren.base.WrenException;
import io.wren.base.client.duckdb.CacheStorageConfig;
import io.wren.base.client.duckdb.DuckDBConfig;
import io.wren.base.client.duckdb.DuckDBConnectorConfig;
import io.wren.base.client.duckdb.DuckdbS3StyleStorageConfig;

import javax.inject.Inject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.wren.base.client.duckdb.DuckDBConfig.DUCKDB_CACHE_TASK_RETRY_DELAY;
import static io.wren.base.client.duckdb.DuckDBConfig.DUCKDB_HOME_DIRECTORY;
import static io.wren.base.client.duckdb.DuckDBConfig.DUCKDB_MAX_CACHE_QUERY_TIMEOUT;
import static io.wren.base.client.duckdb.DuckDBConfig.DUCKDB_MAX_CONCURRENT_METADATA_QUERIES;
import static io.wren.base.client.duckdb.DuckDBConfig.DUCKDB_MAX_CONCURRENT_TASKS;
import static io.wren.base.client.duckdb.DuckDBConfig.DUCKDB_MEMORY_LIMIT;
import static io.wren.base.client.duckdb.DuckDBConfig.DUCKDB_TEMP_DIRECTORY;
import static io.wren.base.client.duckdb.DuckDBConnectorConfig.DUCKDB_CONNECTOR_INIT_SQL_PATH;
import static io.wren.base.client.duckdb.DuckDBConnectorConfig.DUCKDB_CONNECTOR_SESSION_SQL_PATH;
import static io.wren.base.client.duckdb.DuckdbS3StyleStorageConfig.DUCKDB_STORAGE_ACCESS_KEY;
import static io.wren.base.client.duckdb.DuckdbS3StyleStorageConfig.DUCKDB_STORAGE_ENDPOINT;
import static io.wren.base.client.duckdb.DuckdbS3StyleStorageConfig.DUCKDB_STORAGE_REGION;
import static io.wren.base.client.duckdb.DuckdbS3StyleStorageConfig.DUCKDB_STORAGE_SECRET_KEY;
import static io.wren.base.client.duckdb.DuckdbS3StyleStorageConfig.DUCKDB_STORAGE_URL_STYLE;
import static io.wren.base.client.duckdb.FileUtil.ARCHIVED;
import static io.wren.base.config.PostgresConfig.POSTGRES_JDBC_URL;
import static io.wren.base.config.PostgresConfig.POSTGRES_PASSWORD;
import static io.wren.base.config.PostgresConfig.POSTGRES_USER;
import static io.wren.base.config.PostgresWireProtocolConfig.PG_WIRE_PROTOCOL_AUTH_FILE;
import static io.wren.base.config.PostgresWireProtocolConfig.PG_WIRE_PROTOCOL_NETTY_THREAD_COUNT;
import static io.wren.base.config.PostgresWireProtocolConfig.PG_WIRE_PROTOCOL_PORT;
import static io.wren.base.config.PostgresWireProtocolConfig.PG_WIRE_PROTOCOL_SSL_ENABLED;
import static io.wren.base.metadata.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class ConfigManager
{
    private static final Logger LOG = Logger.get(ConfigManager.class);
    private Optional<WrenConfig> wrenConfig;
    private Optional<PostgresConfig> postgresConfig;
    private Optional<BigQueryConfig> bigQueryConfig;
    private Optional<DuckDBConfig> duckDBConfig;
    private Optional<PostgresWireProtocolConfig> postgresWireProtocolConfig;
    private Optional<DuckdbS3StyleStorageConfig> duckdbS3StyleStorageConfig;
    private Optional<DuckDBConnectorConfig> duckDBConnectorConfig;

    private final Map<String, String> configs = new HashMap<>();
    // All configs set by user and config files. It's used to sync with config file.
    private Properties setConfigs = new Properties();
    private final String configFile = System.getProperty("config");
    private final Set<String> requiredReload = new HashSet<>();
    private final Set<String> staticConfigs = new HashSet<>();

    @Inject
    public ConfigManager(
            WrenConfig wrenConfig,
            PostgresConfig postgresConfig,
            BigQueryConfig bigQueryConfig,
            DuckDBConfig duckDBConfig,
            PostgresWireProtocolConfig postgresWireProtocolConfig,
            DuckdbS3StyleStorageConfig duckdbS3StyleStorageConfig,
            DuckDBConnectorConfig duckDBConnectorConfig)
    {
        this.wrenConfig = Optional.of(wrenConfig);
        this.postgresConfig = Optional.of(postgresConfig);
        this.bigQueryConfig = Optional.of(bigQueryConfig);
        this.duckDBConfig = Optional.of(duckDBConfig);
        this.postgresWireProtocolConfig = Optional.of(postgresWireProtocolConfig);
        this.duckdbS3StyleStorageConfig = Optional.of(duckdbS3StyleStorageConfig);
        this.duckDBConnectorConfig = Optional.of(duckDBConnectorConfig);

        initConfig(
                wrenConfig,
                postgresConfig,
                bigQueryConfig,
                duckDBConfig,
                postgresWireProtocolConfig,
                duckdbS3StyleStorageConfig,
                duckDBConnectorConfig);

        try {
            setConfigs.putAll(loadPropertiesFrom(configFile));
        }
        catch (IOException e) {
            throw new WrenException(NOT_FOUND, "Config file not found");
        }
    }

    private void initConfig(
            WrenConfig wrenConfig,
            PostgresConfig postgresConfig,
            BigQueryConfig bigQueryConfig,
            DuckDBConfig duckDBConfig,
            PostgresWireProtocolConfig postgresWireProtocolConfig,
            DuckdbS3StyleStorageConfig duckdbS3StyleStorageConfig,
            DuckDBConnectorConfig duckDBConnectorConfig)
    {
        initConfig(WrenConfig.WREN_DIRECTORY, wrenConfig.getWrenMDLDirectory().getPath(), false, true);
        initConfig(WrenConfig.WREN_DATASOURCE_TYPE, Optional.ofNullable(wrenConfig.getDataSourceType()).map(Enum::name).orElse(null), true, false);
        initConfig(WrenConfig.WREN_ENABLE_DYNAMIC_FIELDS, Boolean.toString(wrenConfig.getEnableDynamicFields()), false, false);
        initConfig(DUCKDB_STORAGE_ENDPOINT, duckdbS3StyleStorageConfig.getEndpoint(), false, true);
        initConfig(DUCKDB_STORAGE_ACCESS_KEY, duckdbS3StyleStorageConfig.getAccessKey().orElse(null), true, false);
        initConfig(DUCKDB_STORAGE_SECRET_KEY, duckdbS3StyleStorageConfig.getSecretKey().orElse(null), true, false);
        initConfig(DUCKDB_STORAGE_REGION, duckdbS3StyleStorageConfig.getRegion().orElse(null), true, false);
        initConfig(DUCKDB_STORAGE_URL_STYLE, duckdbS3StyleStorageConfig.getUrlStyle(), false, false);
        initConfig(DUCKDB_MEMORY_LIMIT, duckDBConfig.getMemoryLimit().toString(), true, false);
        initConfig(DUCKDB_HOME_DIRECTORY, duckDBConfig.getHomeDirectory(), true, false);
        initConfig(DUCKDB_TEMP_DIRECTORY, duckDBConfig.getTempDirectory(), true, false);
        initConfig(DUCKDB_MAX_CONCURRENT_TASKS, Integer.toString(duckDBConfig.getMaxConcurrentTasks()), false, true);
        // TODO: should support reload this config
        initConfig(DUCKDB_MAX_CONCURRENT_METADATA_QUERIES, Integer.toString(duckDBConfig.getMaxConcurrentMetadataQueries()), false, true);
        initConfig(DUCKDB_MAX_CACHE_QUERY_TIMEOUT, Long.toString(duckDBConfig.getMaxCacheQueryTimeout()), false, true);
        initConfig(DUCKDB_CACHE_TASK_RETRY_DELAY, Long.toString(duckDBConfig.getCacheTaskRetryDelay()), false, true);
        initConfig(PG_WIRE_PROTOCOL_PORT, postgresWireProtocolConfig.getPort(), false, true);
        initConfig(PG_WIRE_PROTOCOL_SSL_ENABLED, Boolean.toString(postgresWireProtocolConfig.isSslEnable()), false, true);
        initConfig(PG_WIRE_PROTOCOL_NETTY_THREAD_COUNT, Integer.toString(postgresWireProtocolConfig.getNettyThreadCount()), false, true);
        initConfig(PG_WIRE_PROTOCOL_AUTH_FILE, postgresWireProtocolConfig.getAuthFile().getPath(), false, true);
        initConfig(BigQueryConfig.BIGQUERY_CRENDITALS_KEY, bigQueryConfig.getCredentialsKey().orElse(null), true, false);
        initConfig(BigQueryConfig.BIGQUERY_CRENDITALS_FILE, bigQueryConfig.getCredentialsFile().orElse(null), true, false);
        initConfig(BigQueryConfig.BIGQUERY_PROJECT_ID, bigQueryConfig.getProjectId().orElse(null), true, false);
        initConfig(BigQueryConfig.BIGQUERY_LOCATION, bigQueryConfig.getLocation().orElse(null), true, false);
        initConfig(BigQueryConfig.BIGQUERY_BUCKET_NAME, bigQueryConfig.getBucketName().orElse(null), true, false);
        initConfig(BigQueryConfig.BIGQUERY_METADATA_SCHEMA_PREFIX, bigQueryConfig.getMetadataSchemaPrefix(), true, false);
        initConfig(POSTGRES_JDBC_URL, postgresConfig.getJdbcUrl(), true, false);
        initConfig(POSTGRES_USER, postgresConfig.getUser(), true, false);
        initConfig(POSTGRES_PASSWORD, postgresConfig.getPassword(), true, false);
        initConfig(DUCKDB_CONNECTOR_INIT_SQL_PATH, duckDBConnectorConfig.getInitSQLPath(), false, false);
        initConfig(DUCKDB_CONNECTOR_SESSION_SQL_PATH, duckDBConnectorConfig.getSessionSQLPath(), false, false);
    }

    private void initConfig(String key, String value, boolean requiredReload, boolean isStatic)
    {
        configs.put(key, value);

        if (requiredReload) {
            this.requiredReload.add(key);
        }

        if (isStatic) {
            staticConfigs.add(key);
        }
    }

    public <T> T getConfig(Class<T> config)
    {
        if (config == WrenConfig.class) {
            return (T) wrenConfig.orElseGet(() -> {
                WrenConfig result = getWrenConfig();
                wrenConfig = Optional.of(result);
                return result;
            });
        }
        if (config == BigQueryConfig.class) {
            return (T) bigQueryConfig.orElseGet(() -> {
                BigQueryConfig result = getBigQueryConfig();
                bigQueryConfig = Optional.of(result);
                return result;
            });
        }
        if (config == PostgresConfig.class) {
            return (T) postgresConfig.orElseGet(() -> {
                PostgresConfig result = getPostgresConfig();
                postgresConfig = Optional.of(result);
                return result;
            });
        }
        if (config == DuckDBConfig.class) {
            return (T) duckDBConfig.orElseGet(() -> {
                DuckDBConfig result = getDuckDBConfig();
                duckDBConfig = Optional.of(result);
                return result;
            });
        }
        if (config == PostgresWireProtocolConfig.class) {
            return (T) postgresWireProtocolConfig.orElseGet(() -> {
                PostgresWireProtocolConfig result = getPostgresWireProtocolConfig();
                postgresWireProtocolConfig = Optional.of(result);
                return result;
            });
        }
        if (config == CacheStorageConfig.class &&
                wrenConfig.map(WrenConfig::getDataSourceType).stream().anyMatch(type -> type == WrenConfig.DataSourceType.BIGQUERY)) {
            return (T) duckdbS3StyleStorageConfig.orElseGet(() -> {
                DuckdbS3StyleStorageConfig result = getDuckdbS3StyleStorageConfig();
                duckdbS3StyleStorageConfig = Optional.of(result);
                return result;
            });
        }
        if (config == DuckDBConnectorConfig.class) {
            return (T) duckDBConnectorConfig.orElseGet(() -> {
                DuckDBConnectorConfig result = getDuckDBConnectorConfig();
                duckDBConnectorConfig = Optional.of(result);
                return result;
            });
        }
        throw new RuntimeException("Unknown config class: " + config.getName());
    }

    private WrenConfig getWrenConfig()
    {
        WrenConfig result = new WrenConfig();
        Optional.ofNullable(configs.get(WrenConfig.WREN_DIRECTORY))
                .ifPresent(directory -> result.setWrenMDLDirectory(new File(directory)));
        result.setDataSourceType(WrenConfig.DataSourceType.valueOf(configs.get(WrenConfig.WREN_DATASOURCE_TYPE).toUpperCase(Locale.ROOT)));
        result.setEnableDynamicFields(Boolean.parseBoolean(configs.get(WrenConfig.WREN_ENABLE_DYNAMIC_FIELDS)));
        return result;
    }

    private BigQueryConfig getBigQueryConfig()
    {
        BigQueryConfig result = new BigQueryConfig();
        result.setCredentialsKey(configs.get(BigQueryConfig.BIGQUERY_CRENDITALS_KEY));
        result.setCredentialsFile(configs.get(BigQueryConfig.BIGQUERY_CRENDITALS_FILE));
        result.setProjectId(configs.get(BigQueryConfig.BIGQUERY_PROJECT_ID));
        result.setLocation(configs.get(BigQueryConfig.BIGQUERY_LOCATION));
        result.setBucketName(configs.get(BigQueryConfig.BIGQUERY_BUCKET_NAME));
        result.setMetadataSchemaPrefix(configs.get(BigQueryConfig.BIGQUERY_METADATA_SCHEMA_PREFIX));
        return result;
    }

    private PostgresConfig getPostgresConfig()
    {
        PostgresConfig result = new PostgresConfig();
        result.setJdbcUrl(configs.get(POSTGRES_JDBC_URL));
        result.setUser(configs.get(POSTGRES_USER));
        result.setPassword(configs.get(POSTGRES_PASSWORD));
        return result;
    }

    private DuckDBConfig getDuckDBConfig()
    {
        DuckDBConfig result = new DuckDBConfig();
        result.setMemoryLimit(DataSize.valueOf(configs.get(DUCKDB_MEMORY_LIMIT)));
        result.setHomeDirectory(configs.get(DUCKDB_HOME_DIRECTORY));
        result.setTempDirectory(configs.get(DUCKDB_TEMP_DIRECTORY));
        result.setMaxConcurrentTasks(Integer.parseInt(configs.get(DUCKDB_MAX_CONCURRENT_TASKS)));
        result.setMaxConcurrentMetadataQueries(Integer.parseInt(configs.get(DUCKDB_MAX_CONCURRENT_METADATA_QUERIES)));
        result.setMaxCacheQueryTimeout(Integer.parseInt(configs.get(DUCKDB_MAX_CACHE_QUERY_TIMEOUT)));
        result.setCacheTaskRetryDelay(Integer.parseInt(configs.get(DUCKDB_CACHE_TASK_RETRY_DELAY)));
        return result;
    }

    private PostgresWireProtocolConfig getPostgresWireProtocolConfig()
    {
        PostgresWireProtocolConfig result = new PostgresWireProtocolConfig();
        result.setPort(configs.get(PG_WIRE_PROTOCOL_PORT));
        result.setSslEnable(Boolean.parseBoolean(configs.get(PG_WIRE_PROTOCOL_SSL_ENABLED)));
        result.setNettyThreadCount(Integer.parseInt(configs.get(PG_WIRE_PROTOCOL_NETTY_THREAD_COUNT)));
        result.setAuthFile(new File(configs.get(PG_WIRE_PROTOCOL_AUTH_FILE)));
        return result;
    }

    private DuckdbS3StyleStorageConfig getDuckdbS3StyleStorageConfig()
    {
        DuckdbS3StyleStorageConfig result = new DuckdbS3StyleStorageConfig();
        result.setEndpoint(configs.get(DUCKDB_STORAGE_ENDPOINT));
        result.setAccessKey(configs.get(DUCKDB_STORAGE_ACCESS_KEY));
        result.setSecretKey(configs.get(DUCKDB_STORAGE_SECRET_KEY));
        result.setRegion(configs.get(DUCKDB_STORAGE_REGION));
        result.setUrlStyle(configs.get(DUCKDB_STORAGE_URL_STYLE));
        return result;
    }

    private DuckDBConnectorConfig getDuckDBConnectorConfig()
    {
        DuckDBConnectorConfig result = new DuckDBConnectorConfig();
        result.setInitSQLPath(configs.get(DUCKDB_CONNECTOR_INIT_SQL_PATH));
        result.setSessionSQLPath(configs.get(DUCKDB_CONNECTOR_SESSION_SQL_PATH));
        return result;
    }

    public synchronized boolean setConfigs(List<ConfigEntry> configEntries, boolean reset)
    {
        if (reset) {
            reset();
        }

        Map<String, String> update = configEntries.stream()
                .map(entry -> Map.entry(entry.getName(), entry.getValue()))
                .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().trim()));
        update.forEach(this::setConfigInternal);
        boolean needReload = false;
        for (ConfigEntry configEntry : configEntries) {
            needReload |= setConfigInternal(configEntry.getName(), configEntry.getValue());
        }
        resetCache();
        syncFile(update);
        return needReload;
    }

    private boolean setConfigInternal(String key, String value)
    {
        // ignore all static config changes
        if (staticConfigs.contains(key)) {
            return false;
        }

        // Only allow set the config that already exists.
        if (configs.containsKey(key)) {
            configs.put(key, value);
        }
        else {
            throw new WrenException(NOT_FOUND, "Config not found: " + key);
        }

        return requiredReload.contains(key);
    }

    private void resetCache()
    {
        wrenConfig = Optional.empty();
        postgresConfig = Optional.empty();
        bigQueryConfig = Optional.empty();
        duckDBConfig = Optional.empty();
        postgresWireProtocolConfig = Optional.empty();
        duckdbS3StyleStorageConfig = Optional.empty();
        duckDBConnectorConfig = Optional.empty();
    }

    private void reset()
    {
        configs.clear();
        setConfigs.clear();
        initConfig(
                new WrenConfig(),
                new PostgresConfig(),
                new BigQueryConfig(),
                new DuckDBConfig(),
                new PostgresWireProtocolConfig(),
                new DuckdbS3StyleStorageConfig(),
                new DuckDBConnectorConfig());
    }

    private void syncFile(Map<String, String> updated)
    {
        try {
            archiveConfigs();
            setConfigs.putAll(updated);
            setConfigs.store(new FileWriter(configFile), "sync with file");
            LOG.info("Syncing config file: " + configFile);
        }
        catch (IOException e) {
            throw new WrenException(NOT_FOUND, format("Config file %s not found", configFile), e);
        }
    }

    private void archiveConfigs()
            throws IOException
    {
        Path home = new File(configFile).getParentFile().toPath();
        File archived = home.resolve(ARCHIVED).toFile();
        if (!archived.exists()) {
            if (!archived.mkdir()) {
                throw new IOException("Cannot create archive folder");
            }
        }
        File archivedFile = new File(configFile);
        Files.copy(archivedFile.toPath(),
                archived.toPath().resolve(archivedFile.getName() + "." + LocalDateTime.now().format(DateTimeFormatter.ofPattern("uuuuMMddHHmmssnnnn"))));
        LOG.info("Archiving config file: " + archived);
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
        throw new WrenException(NOT_FOUND, "Config not found: " + key);
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
