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

package io.wren.base.client;

import io.wren.base.Column;
import io.wren.base.Parameter;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static java.util.Collections.emptyList;

public interface Client
{
    default AutoCloseableIterator<Object[]> query(String sql)
    {
        return query(sql, emptyList());
    }

    AutoCloseableIterator<Object[]> query(String sql, List<Parameter> parameters);

    void executeDDL(String sql);

    List<Column> describe(String sql, List<Parameter> parameters);

    List<String> listTables();

    Connection createConnection()
            throws SQLException;

    void close();
}
