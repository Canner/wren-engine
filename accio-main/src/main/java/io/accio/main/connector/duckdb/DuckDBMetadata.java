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

package io.accio.main.connector.duckdb;

import com.google.common.collect.ImmutableMap;
import io.accio.base.AccioException;
import io.accio.base.Column;
import io.accio.base.ConnectorRecordIterator;
import io.accio.base.Parameter;
import io.accio.base.client.AutoCloseableIterator;
import io.accio.base.client.duckdb.CacheStorageConfig;
import io.accio.base.client.duckdb.DuckDBConfig;
import io.accio.base.client.duckdb.DuckDBConnectorConfig;
import io.accio.base.client.duckdb.DuckDBSettingSQL;
import io.accio.base.client.duckdb.DuckdbClient;
import io.accio.base.config.ConfigManager;
import io.accio.base.metadata.TableMetadata;
import io.accio.base.type.DateType;
import io.accio.base.type.PGType;
import io.accio.base.type.VarcharType;
import io.accio.cache.DuckdbRecordIterator;
import io.accio.connector.StorageClient;
import io.accio.main.metadata.Metadata;
import io.accio.main.pgcatalog.builder.DuckDBFunctionBuilder;
import io.accio.main.pgcatalog.builder.PgFunctionBuilder;
import io.airlift.log.Logger;
import io.trino.sql.tree.QualifiedName;

import javax.inject.Inject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.accio.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.accio.main.pgcatalog.PgCatalogUtils.ACCIO_TEMP_NAME;
import static io.accio.main.pgcatalog.PgCatalogUtils.PG_CATALOG_NAME;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class DuckDBMetadata
        implements Metadata
{
    private static final Logger LOG = Logger.get(DuckDBMetadata.class);
    private final ConfigManager configManager;
    private DuckdbClient duckdbClient;
    private final PgFunctionBuilder pgFunctionBuilder;
    private final Map<String, String> pgToDuckDBFunctionNameMappings;
    private final AtomicReference<DuckDBSettingSQL> duckDBSettingSQL = new AtomicReference<>(new DuckDBSettingSQL());

    @Inject
    public DuckDBMetadata(
            ConfigManager configManager)
    {
        this.configManager = requireNonNull(configManager, "configManager is null");
        initDuckDBSettingSQLIfNeed();
        this.duckdbClient = buildDuckDBClient();
        this.pgFunctionBuilder = new DuckDBFunctionBuilder(duckdbClient);
        this.pgToDuckDBFunctionNameMappings = initPgNameToDuckDBFunctions();
    }

    @Override
    public boolean isSchemaExist(String name)
    {
        try (AutoCloseableIterator iter = duckdbClient
                .query("SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?", List.of(new Parameter(VarcharType.VARCHAR, name)))) {
            return iter.hasNext();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createSchema(String name)
    {
        duckdbClient.executeDDL("CREATE SCHEMA " + name);
    }

    @Override
    public void dropSchemaIfExists(String name)
    {
        duckdbClient.executeDDL("DROP SCHEMA " + name);
    }

    @Override
    public List<String> listSchemas()
    {
        return null;
    }

    @Override
    public List<TableMetadata> listTables(String schemaName)
    {
        return null;
    }

    @Override
    public List<String> listFunctionNames(String schemaName)
    {
        return null;
    }

    @Override
    public QualifiedName resolveFunction(String functionName, int numArgument)
    {
        String funcNameLowerCase = functionName.toLowerCase(ENGLISH);

        if (pgToDuckDBFunctionNameMappings.containsKey(funcNameLowerCase)) {
            return QualifiedName.of(pgToDuckDBFunctionNameMappings.get(funcNameLowerCase));
        }

        return QualifiedName.of(functionName);
    }

    @Override
    public String getDefaultCatalog()
    {
        return null;
    }

    @Override
    public void directDDL(String sql)
    {
        duckdbClient.executeDDL(sql);
    }

    @Override
    public ConnectorRecordIterator directQuery(String sql, List<Parameter> parameters)
    {
        try {
            return DuckdbRecordIterator.of(duckdbClient, sql, convertParameters(parameters));
        }
        catch (Exception e) {
            throw new AccioException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    @Override
    public List<Column> describeQuery(String sql, List<Parameter> parameters)
    {
        return duckdbClient.describe(sql, convertParameters(parameters)).stream()
                .map(columnMetadata -> new Column(columnMetadata.getName(), columnMetadata.getType()))
                .collect(toList());
    }

    @Override
    public boolean isPgCompatible()
    {
        return true;
    }

    @Override
    public String getMetadataSchemaName()
    {
        return ACCIO_TEMP_NAME;
    }

    @Override
    public String getPgCatalogName()
    {
        return PG_CATALOG_NAME;
    }

    @Override
    public void reload()
    {
        // Create client success first, then close the old one
        DuckdbClient duckdbClient = buildDuckDBClient();
        this.duckdbClient.close();
        this.duckdbClient = duckdbClient;
    }

    @Override
    public StorageClient getCacheStorageClient()
    {
        throw new UnsupportedOperationException("DuckDB does not support cache storage");
    }

    @Override
    public void close()
    {
        duckdbClient.close();
    }

    @Override
    public PgFunctionBuilder getPgFunctionBuilder()
    {
        return pgFunctionBuilder;
    }

    public DuckdbClient getClient()
    {
        return duckdbClient;
    }

    private DuckdbClient buildDuckDBClient()
    {
        return new DuckdbClient(configManager.getConfig(DuckDBConfig.class), getCacheStorageConfigIfExists(), duckDBSettingSQL.get());
    }

    private CacheStorageConfig getCacheStorageConfigIfExists()
    {
        try {
            return configManager.getConfig(CacheStorageConfig.class);
        }
        catch (Exception e) {
            return null;
        }
    }

    /**
     * @return mapping table for pg function which can be replaced by duckdb function.
     */
    private Map<String, String> initPgNameToDuckDBFunctions()
    {
        return ImmutableMap.<String, String>builder()
                .put("generate_array", "generate_series")
                .build();
    }

    private void initDuckDBSettingSQLIfNeed()
    {
        setSQLFromFile(getInitSQLPath(), this::setInitSQL);
        setSQLFromFile(getSessionSQLPath(), this::setSessionSQL);
    }

    private void setSQLFromFile(Path filePath, Consumer<String> setter)
    {
        if (filePath != null) {
            try {
                setter.accept(Files.readString(filePath));
            }
            catch (NoSuchFileException e) {
                // Do nothing
            }
            catch (IOException e) {
                LOG.error(e, "Failed to read SQL from %s", filePath);
            }
        }
    }

    private List<Parameter> convertParameters(List<Parameter> parameters)
    {
        return parameters.stream().map(this::convertParameter).collect(toList());
    }

    private Parameter convertParameter(Parameter parameter)
    {
        PGType<?> type = parameter.getType();
        Object value = parameter.getValue();
        if (type instanceof DateType && value instanceof LocalDate) {
            value = Date.valueOf((LocalDate) value);
        }
        return new Parameter(type, value);
    }

    public String getInitSQL()
    {
        return duckDBSettingSQL.get().getInitSQL();
    }

    public void setInitSQL(String initSQL)
    {
        duckDBSettingSQL.get().setInitSQL(initSQL);
    }

    public Path getInitSQLPath()
    {
        return Path.of(configManager.getConfig(DuckDBConnectorConfig.class).getInitSQLPath());
    }

    public String getSessionSQL()
    {
        return duckDBSettingSQL.get().getSessionSQL();
    }

    public void setSessionSQL(String sessionSQL)
    {
        duckDBSettingSQL.get().setSessionSQL(sessionSQL);
    }

    public Path getSessionSQLPath()
    {
        return Path.of(configManager.getConfig(DuckDBConnectorConfig.class).getSessionSQLPath());
    }
}
