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

package io.accio.main.wireprotocol;

import io.accio.base.AccioException;
import io.accio.base.ConnectorRecordIterator;
import io.accio.base.Parameter;
import io.accio.base.client.AutoCloseableIterator;
import io.accio.base.client.duckdb.CacheStorageConfig;
import io.accio.base.client.duckdb.DuckDBConfig;
import io.accio.base.client.duckdb.DuckdbClient;
import io.accio.base.client.duckdb.DuckdbTypes;
import io.accio.base.config.ConfigManager;
import io.accio.base.type.DateType;
import io.accio.base.type.PGType;
import io.accio.base.type.VarcharType;
import io.accio.base.wireprotocol.PgMetastore;
import io.accio.cache.DuckdbRecordIterator;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.sql.Date;
import java.time.LocalDate;
import java.util.List;

import static io.accio.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.accio.main.pgcatalog.PgCatalogUtils.PG_CATALOG_NAME;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class PgMetastoreImpl
        implements PgMetastore
{
    private static final Logger LOG = Logger.get(PgMetastoreImpl.class);
    private final ConfigManager configManager;
    private final DuckdbClient duckdbClient;

    @Inject
    public PgMetastoreImpl(
            ConfigManager configManager)
    {
        this.configManager = requireNonNull(configManager, "configManager is null");
        this.duckdbClient = buildDuckDBClient();
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
    public void dropTableIfExists(String name)
    {
        try {
            duckdbClient.executeDDL(format("BEGIN TRANSACTION;DROP TABLE IF EXISTS %s;COMMIT;", name));
        }
        catch (Exception e) {
            LOG.error(e, "Failed to drop table %s", name);
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
            throw new AccioException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    @Override
    public String getPgCatalogName()
    {
        return PG_CATALOG_NAME;
    }

    @Override
    public String handlePgType(String type)
    {
        if (type.startsWith("_")) {
            return format("%s[]", handlePgType(type.substring(1)));
        }
        else if (!DuckdbTypes.getDuckDBTypeNames().contains(type)) {
            return "VARCHAR";
        }
        return type;
    }

    @Override
    public DuckdbClient getClient()
    {
        return duckdbClient;
    }

    @Override
    public void close()
    {
        duckdbClient.close();
    }

    private DuckdbClient buildDuckDBClient()
    {
        return new DuckdbClient(configManager.getConfig(DuckDBConfig.class), getCacheStorageConfigIfExists(), null);
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
}
