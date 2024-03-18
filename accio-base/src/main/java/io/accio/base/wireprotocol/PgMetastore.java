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

package io.accio.base.wireprotocol;

import io.accio.base.ConnectorRecordIterator;
import io.accio.base.Parameter;
import io.accio.base.client.Client;
import io.accio.base.sql.SqlConverter;

import java.util.List;

public abstract class PgMetastore
{
    public abstract void directDDL(String sql);

    public abstract ConnectorRecordIterator directQuery(String sql, List<Parameter> parameters);

    public abstract String handlePgType(String type);

    public abstract String getPgCatalogName();

    public abstract boolean isSchemaExist(String schemaName);

    public abstract void dropTableIfExists(String name);

    public abstract Client getClient();

    public abstract SqlConverter getSqlConverter();

    public abstract void close();
}
