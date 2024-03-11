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

import io.accio.base.AccioException;
import io.accio.base.Column;
import io.accio.base.ConnectorRecordIterator;
import io.accio.base.Parameter;
import io.accio.base.client.AutoCloseableIterator;
import io.accio.base.client.duckdb.DuckdbClient;
import io.accio.base.client.duckdb.DuckdbTypes;
import io.accio.base.metadata.TableMetadata;
import io.accio.base.sql.SqlConverter;
import io.accio.base.type.VarcharType;
import io.accio.cache.DuckdbRecordIterator;
import io.accio.connector.StorageClient;
import io.accio.main.metadata.Metadata;
import io.accio.main.pgcatalog.builder.DuckDBFunctionBuilder;
import io.accio.main.pgcatalog.builder.PgFunctionBuilder;
import io.accio.main.wireprotocol.PgMetastore;
import io.trino.sql.tree.QualifiedName;

import javax.inject.Inject;

import java.util.List;

import static io.accio.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.accio.main.pgcatalog.PgCatalogUtils.ACCIO_TEMP_NAME;
import static io.accio.main.pgcatalog.PgCatalogUtils.PG_CATALOG_NAME;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class DuckDBMetadata
        implements Metadata, PgMetastore
{
    private final DuckdbClient duckdbClient;
    private final SqlConverter sqlConverter = new DuckDBSqlConverter();
    private final PgFunctionBuilder pgFunctionBuilder;

    @Inject
    public DuckDBMetadata(DuckdbClient duckdbClient)
    {
        this.duckdbClient = requireNonNull(duckdbClient, "duckdbClient is null");
        this.pgFunctionBuilder = new DuckDBFunctionBuilder(this);
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
        return null;
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
            return DuckdbRecordIterator.of(duckdbClient, sql, parameters);
        }
        catch (Exception e) {
            throw new AccioException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    @Override
    public List<Column> describeQuery(String sql, List<Parameter> parameters)
    {
        return duckdbClient.describe(sql, parameters).stream()
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
    public SqlConverter getSqlConverter()
    {
        return sqlConverter;
    }

    @Override
    public void reload() {}

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

    public void reset()
    {
        duckdbClient.reset();
    }
}
