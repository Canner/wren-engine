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

package io.accio.main;

import io.accio.base.Column;
import io.accio.base.ConnectorRecordIterator;
import io.accio.base.Parameter;
import io.accio.base.metadata.TableMetadata;
import io.accio.base.type.PGType;
import io.accio.main.metadata.Metadata;
import io.trino.sql.tree.QualifiedName;

import java.util.List;

import static io.accio.main.pgcatalog.PgCatalogUtils.ACCIO_TEMP_NAME;
import static io.accio.main.pgcatalog.PgCatalogUtils.PG_CATALOG_NAME;

public class TestingMetadata
        implements Metadata
{
    @Override
    public void createSchema(String name)
    {
        throw new UnsupportedOperationException("TestingMetadata doesn't support this method");
    }

    @Override
    public void dropSchemaIfExists(String name)
    {
        throw new UnsupportedOperationException("TestingMetadata doesn't support this method");
    }

    @Override
    public boolean isSchemaExist(String name)
    {
        throw new UnsupportedOperationException("TestingMetadata doesn't support this method");
    }

    @Override
    public List<String> listSchemas()
    {
        return List.of("testing_schema1", "testing_schema2");
    }

    @Override
    public List<TableMetadata> listTables(String schemaName)
    {
        throw new UnsupportedOperationException("TestingMetadata doesn't support this method");
    }

    @Override
    public List<String> listFunctionNames(String schemaName)
    {
        throw new UnsupportedOperationException("TestingMetadata doesn't support this method");
    }

    @Override
    public QualifiedName resolveFunction(String functionName, List<PGType<?>> argumentTypes)
    {
        return QualifiedName.of(functionName);
    }

    @Override
    public String getDefaultCatalog()
    {
        throw new UnsupportedOperationException("TestingMetadata doesn't support this method");
    }

    @Override
    public void directDDL(String sql)
    {
        throw new UnsupportedOperationException("TestingMetadata doesn't support this method");
    }

    @Override
    public ConnectorRecordIterator directQuery(String sql, List<Parameter> parameters)
    {
        throw new UnsupportedOperationException("TestingMetadata doesn't support this method");
    }

    @Override
    public List<Column> describeQuery(String sql, List<Parameter> parameters)
    {
        throw new UnsupportedOperationException("TestingMetadata doesn't support this method");
    }

    @Override
    public boolean isPgCompatible()
    {
        return false;
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
}
