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

package io.cml.spi.connector;

import io.cml.spi.metadata.MaterializedViewDefinition;
import io.cml.spi.metadata.TableMetadata;

import java.util.List;
import java.util.Optional;

public interface Connector
{
    void createSchema(String name);

    boolean isSchemaExist(String name);

    List<String> listSchemas();

    List<TableMetadata> listTables(String schemaName);

    List<MaterializedViewDefinition> listMaterializedViews(Optional<String> schemaName);

    List<String> listFunctionNames(String schemaName);

    String getCatalogName();

    void directDDL(String sql);

    Iterable<Object[]> directQuery(String sql);
}
