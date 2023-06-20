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

package io.graphmdl.base.client;

import io.graphmdl.base.Parameter;
import io.graphmdl.base.metadata.ColumnMetadata;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public interface Client
{
    AutoCloseableIterator<Object[]> query(String sql);

    AutoCloseableIterator<Object[]> query(String sql, List<Parameter> parameters);

    void executeDDL(String sql);

    List<ColumnMetadata> describe(String sql, List<Parameter> parameters);

    List<String> listTables();

    Connection createConnection()
            throws SQLException;
}
