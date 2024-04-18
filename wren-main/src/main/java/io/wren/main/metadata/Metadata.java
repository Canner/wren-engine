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

package io.wren.main.metadata;

import io.trino.sql.tree.QualifiedName;
import io.wren.base.Column;
import io.wren.base.ConnectorRecordIterator;
import io.wren.base.Parameter;
import io.wren.connector.StorageClient;
import io.wren.main.pgcatalog.builder.PgFunctionBuilder;

import java.util.List;

public interface Metadata
{
    void createSchema(String name);

    void dropSchemaIfExists(String name);

    QualifiedName resolveFunction(String functionName, int numArgument);

    String getDefaultCatalog();

    void directDDL(String sql);

    ConnectorRecordIterator directQuery(String sql, List<Parameter> parameters);

    List<Column> describeQuery(String sql, List<Parameter> parameters);

    boolean isPgCompatible();

    String getPgCatalogName();

    void reload();

    StorageClient getCacheStorageClient();

    PgFunctionBuilder getPgFunctionBuilder();

    void close();
}
