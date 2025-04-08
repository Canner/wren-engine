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

package io.wren.main.connector.duckdb;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.wren.base.Column;
import io.wren.base.ConnectorRecordIterator;
import io.wren.base.Parameter;
import io.wren.base.WrenException;
import io.wren.base.client.duckdb.DuckDBConfig;
import io.wren.base.client.duckdb.DuckDBConnectorConfig;
import io.wren.base.client.duckdb.DuckDBSettingSQL;
import io.wren.base.client.duckdb.DuckdbClient;
import io.wren.base.config.ConfigManager;
import io.wren.base.config.WrenConfig;
import io.wren.main.metadata.Metadata;

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

import static io.wren.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class DuckDBMetadata
        implements Metadata
{
    public static final Map<String, String> PG_TO_DUCKDB_FUNCTION_NAME_MAPPINGS = initPgNameToDuckDBFunctions();
    private static final Logger LOG = Logger.get(DuckDBMetadata.class);
    private final ConfigManager configManager;
    private DuckdbClient duckdbClient;
    private final AtomicReference<DuckDBSettingSQL> duckDBSettingSQL = new AtomicReference<>(new DuckDBSettingSQL());

    @Inject
    public DuckDBMetadata(
            ConfigManager configManager)
    {
        this.configManager = requireNonNull(configManager, "configManager is null");
        if (configManager.getConfig(WrenConfig.class).getDataSourceType().equals(WrenConfig.DataSourceType.DUCKDB)) {
            initDuckDBSettingSQLIfNeed();
            this.duckdbClient = buildDuckDBClientSafely();
        }
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
            throw new WrenException(GENERIC_INTERNAL_ERROR, e);
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
    public void reload()
    {
        close();
        this.duckdbClient = buildDuckDBClient();
    }

    @Override
    public void close()
    {
        if (duckdbClient != null) {
            duckdbClient.close();
        }
    }

    public DuckdbClient getClient()
    {
        return duckdbClient;
    }

    private DuckdbClient buildDuckDBClient()
    {
        return DuckdbClient.builder()
                .setDuckDBConfig(configManager.getConfig(DuckDBConfig.class))
                .setDuckDBSettingSQL(duckDBSettingSQL.get())
                .build();
    }

    private DuckdbClient buildDuckDBClientSafely()
    {
        DuckdbClient.Builder builder = DuckdbClient.builder()
                .setDuckDBConfig(configManager.getConfig(DuckDBConfig.class))
                .setDuckDBSettingSQL(duckDBSettingSQL.get());
        return builder.buildSafely().orElse(builder.setDuckDBSettingSQL(null).build());
    }

    /**
     * @return mapping table for pg function which can be replaced by duckdb function.
     */
    private static Map<String, String> initPgNameToDuckDBFunctions()
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

    public static List<Parameter> convertParameters(List<Parameter> parameters)
    {
        return parameters.stream().map(DuckDBMetadata::convertParameter).collect(toList());
    }

    private static Parameter convertParameter(Parameter parameter)
    {
        String type = parameter.getType();
        Object value = parameter.getValue();
        if (type.equals("DATE") && value instanceof LocalDate) {
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

    public void appendInitSQL(String sql)
    {
        duckDBSettingSQL.updateAndGet(settingSQL -> {
            String initSQL = settingSQL.getInitSQL();
            if (initSQL == null) {
                settingSQL.setInitSQL(sql);
            }
            else {
                settingSQL.setInitSQL(initSQL + "\n" + sql);
            }
            return settingSQL;
        });
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

    public void appendSessionSQL(String sql)
    {
        duckDBSettingSQL.updateAndGet(settingSQL -> {
            String sessionSQL = settingSQL.getSessionSQL();
            if (sessionSQL == null) {
                settingSQL.setSessionSQL(sql);
            }
            else {
                settingSQL.setSessionSQL(sessionSQL + "\n" + sql);
            }
            return settingSQL;
        });
    }

    public Path getSessionSQLPath()
    {
        return Path.of(configManager.getConfig(DuckDBConnectorConfig.class).getSessionSQLPath());
    }
}
