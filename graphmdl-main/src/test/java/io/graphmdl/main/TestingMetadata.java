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

package io.graphmdl.main;

import io.graphmdl.base.Column;
import io.graphmdl.base.ConnectorRecordIterator;
import io.graphmdl.base.Parameter;
import io.graphmdl.base.metadata.TableMetadata;
import io.graphmdl.main.metadata.Metadata;
import io.trino.sql.tree.QualifiedName;

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
    public List<String> listFunctionNames(String schemaName)
    {
        throw new UnsupportedOperationException("TestingMetadata doesn't support this method");
    }

    @Override
    public QualifiedName resolveFunction(String functionName, int numArgument)
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
}
