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

package io.wren.main.connector.snowflake;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.sql.tree.QualifiedName;
import io.wren.base.Column;
import io.wren.base.ConnectorRecordIterator;
import io.wren.base.Parameter;
import io.wren.base.WrenException;
import io.wren.base.config.ConfigManager;
import io.wren.base.config.SnowflakeConfig;
import io.wren.base.config.WrenConfig;
import io.wren.connector.StorageClient;
import io.wren.main.metadata.Metadata;
import io.wren.main.pgcatalog.builder.PgFunctionBuilder;

import java.util.List;

import static io.wren.base.config.WrenConfig.DataSourceType.SNOWFLAKE;
import static io.wren.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.wren.main.pgcatalog.PgCatalogUtils.PG_CATALOG_NAME;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class SnowflakeMetadata
        implements Metadata
{
    private static final Logger LOG = Logger.get(SnowflakeMetadata.class);
    private final ConfigManager configManager;
    private final PgFunctionBuilder pgFunctionBuilder;
    private SnowflakeClient client;

    @Inject
    public SnowflakeMetadata(ConfigManager configManager)
    {
        this.configManager = requireNonNull(configManager, "configManager is null");
        if (configManager.getConfig(WrenConfig.class).getDataSourceType() == SNOWFLAKE) {
            this.client = new SnowflakeClient(configManager.getConfig(SnowflakeConfig.class));
        }
        this.pgFunctionBuilder = new SnowflakeFunctionBuilder();
    }

    @Override
    public void createSchema(String name)
    {
        client.execute(format("CREATE SCHEMA %s", name));
    }

    @Override
    public void dropSchemaIfExists(String name)
    {
        client.execute(format("DROP SCHEMA IF EXISTS %s", name));
    }

    @Override
    public String getDefaultCatalog()
    {
        try (SnowflakeRecordIterator iterator = client.query("SELECT current_database()", emptyList())) {
            return (String) iterator.next()[0];
        }
        catch (Exception e) {
            throw new WrenException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    @Override
    public void directDDL(String sql)
    {
        client.execute(sql);
    }

    @Override
    public ConnectorRecordIterator directQuery(String sql, List<Parameter> parameters)
    {
        try {
            return client.query(sql, parameters);
        }
        catch (Exception e) {
            LOG.error(e);
            LOG.error("Failed SQL: %s", sql);
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Column> describeQuery(String sql, List<Parameter> parameters)
    {
        return client.describe(sql, parameters);
    }

    @Override
    public boolean isPgCompatible()
    {
        return false;
    }

    @Override
    public String getPgCatalogName()
    {
        return PG_CATALOG_NAME;
    }

    @Override
    public void reload()
    {
        this.client = new SnowflakeClient(configManager.getConfig(SnowflakeConfig.class));
    }

    @Override
    public PgFunctionBuilder getPgFunctionBuilder()
    {
        return pgFunctionBuilder;
    }

    @Override
    public void close() {}

    @Override
    public QualifiedName resolveFunction(String functionName, int numArgument)
    {
        throw new UnsupportedOperationException("Unnecessary method");
    }

    @Override
    public StorageClient getCacheStorageClient()
    {
        throw new UnsupportedOperationException("Does not support cache storage client");
    }
}
