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
package io.graphmdl.main.metadata;

import io.graphmdl.main.calcite.GraphMDLSchemaUtil;
import io.graphmdl.spi.CatalogSchemaTableName;
import io.graphmdl.spi.Column;
import io.graphmdl.spi.ConnectorRecordIterator;
import io.graphmdl.spi.Parameter;
import io.graphmdl.spi.metadata.TableMetadata;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlOperatorTable;

import java.util.List;

public interface Metadata
{
    void createSchema(String name);

    boolean isSchemaExist(String name);

    List<String> listSchemas();

    List<TableMetadata> listTables(String schemaName);

    List<String> listFunctionNames(String schemaName);

    String resolveFunction(String functionName, int numArgument);

    SqlOperatorTable getCalciteOperatorTable();

    RelDataTypeFactory getTypeFactory();

    TableMetadata getTableMetadata(CatalogSchemaTableName catalogSchemaTableName);

    GraphMDLSchemaUtil.Dialect getDialect();

    RelDataTypeSystem getRelDataTypeSystem();

    String getDefaultCatalog();

    void directDDL(String sql);

    ConnectorRecordIterator directQuery(String sql, List<Parameter> parameters);

    List<Column> describeQuery(String sql, List<Parameter> parameters);
}
