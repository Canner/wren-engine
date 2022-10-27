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

package io.cml.connector.canner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.cml.calcite.CmlSchemaUtil;
import io.cml.metadata.Metadata;
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

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class CannerMetadta
        implements Metadata

{
    private final CannerClient client;

    @Inject
    public CannerMetadta(CannerClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public void createSchema(String name)
    {
        client.createWorkspace(name);
    }

    @Override
    public boolean isSchemaExist(String name)
    {
        return client.getOneWorkspace(name).isPresent();
    }

    @Override
    public List<String> listSchemas()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<TableMetadata> listTables(String schemaName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createMaterializedView(SchemaTableName schemaTableName, String sql)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<MaterializedViewDefinition> listMaterializedViews()
    {
        return ImmutableList.of();
    }

    @Override
    public void deleteMaterializedView(SchemaTableName schemaTableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listFunctionNames(String schemaName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String resolveFunction(String functionName, int numArgument)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SqlOperatorTable getCalciteOperatorTable()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelDataTypeFactory getTypeFactory()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableMetadata getTableMetadata(CatalogSchemaTableName catalogSchemaTableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CmlSchemaUtil.Dialect getDialect()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelDataTypeSystem getRelDataTypeSystem()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getDefaultCatalog()
    {
        return "canner";
    }

    @Override
    public void directDDL(String sql)
    {
        throw new UnsupportedOperationException("Canner Client doesn't support this method.");
    }

    @Override
    public ConnectorRecordIterator directQuery(String sql, List<Parameter> parameters)
    {
        return client.query(sql, parameters);
    }

    @Override
    public List<Column> describeQuery(String sql, List<Parameter> parameters)
    {
        ConnectorRecordIterator recordIterator = client.describe(sql, parameters);
        return Streams.stream(recordIterator)
                .map(record -> new Column((String) record[0], CannerType.toPGType((String) record[4])))
                .collect(toList());
    }

    @Override
    public String getMaterializedViewSchema()
    {
        throw new UnsupportedOperationException();
    }
}
