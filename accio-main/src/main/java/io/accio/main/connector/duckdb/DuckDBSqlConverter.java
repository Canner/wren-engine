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

package io.accio.main.connector.duckdb;

import io.accio.base.SessionContext;
import io.accio.base.sql.SqlConverter;
import io.trino.sql.SqlFormatter;

import static io.accio.base.sqlrewrite.Utils.parseSql;

public class DuckDBSqlConverter
        implements SqlConverter
{
    @Override
    public String convert(String sql, SessionContext sessionContext)
    {
        return SqlFormatter.formatSql(parseSql(sql), SqlFormatter.Dialect.DUCKDB);
    }
}
