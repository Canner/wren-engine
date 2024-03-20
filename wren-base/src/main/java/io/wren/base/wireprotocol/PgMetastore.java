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

package io.wren.base.wireprotocol;

import io.wren.base.ConnectorRecordIterator;
import io.wren.base.Parameter;
import io.wren.base.client.Client;
import io.wren.base.sql.SqlConverter;

import java.util.List;

public interface PgMetastore
{
    void directDDL(String sql);

    ConnectorRecordIterator directQuery(String sql, List<Parameter> parameters);

    String handlePgType(String type);

    String getPgCatalogName();

    boolean isSchemaExist(String schemaName);

    void dropTableIfExists(String name);

    Client getClient();

    SqlConverter getSqlConverter();

    void close();
}
