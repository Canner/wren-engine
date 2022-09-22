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

package io.cml;

import io.cml.calcite.CmlSchemaUtil;
import io.cml.metadata.Metadata;
import io.cml.metadata.TableSchema;
import io.cml.spi.CatalogSchemaTableName;
import io.cml.spi.Column;
import io.cml.spi.ConnectorRecordIterator;
import io.cml.spi.Parameter;
import io.cml.spi.metadata.MaterializedViewDefinition;
import io.cml.spi.metadata.SchemaTableName;
import io.cml.spi.metadata.TableMetadata;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlOperatorTable;

import java.util.List;

public class TestingMetadata
        implements Metadata
{
    @Override
    public void createSchema(String name)
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
    public void createMaterializedView(SchemaTableName schemaTableName, String sql)
    {
        throw new UnsupportedOperationException("TestingMetadata doesn't support this method");
    }

    @Override
    public List<MaterializedViewDefinition> listMaterializedViews()
    {
        throw new UnsupportedOperationException("TestingMetadata doesn't support this method");
    }

    @Override
    public void deleteMaterializedView(SchemaTableName schemaTableName)
    {
        throw new UnsupportedOperationException("TestingMetadata doesn't support this method");
    }

    @Override
    public List<String> listFunctionNames(String schemaName)
    {
        throw new UnsupportedOperationException("TestingMetadata doesn't support this method");
    }

    @Override
    public String resolveFunction(String functionName, int numArgument)
    {
        return functionName;
    }

    @Override
    public TableSchema getTableSchema(CatalogSchemaTableName catalogSchemaTableName)
    {
        throw new UnsupportedOperationException("TestingMetadata doesn't support this method");
    }

    @Override
    public CmlSchemaUtil.Dialect getDialect()
    {
        throw new UnsupportedOperationException("TestingMetadata doesn't support this method");
    }

    @Override
    public RelDataTypeSystem getRelDataTypeSystem()
    {
        throw new UnsupportedOperationException("TestingMetadata doesn't support this method");
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
    public SqlOperatorTable getCalciteOperatorTable()
    {
        throw new UnsupportedOperationException("TestingMetadata doesn't support this method");
    }

    @Override
    public RelDataTypeFactory getTypeFactory()
    {
        throw new UnsupportedOperationException("TestingMetadata doesn't support this method");
    }
}
