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

package io.graphmdl.main.connector.postgres;

import io.airlift.log.Logger;
import io.graphmdl.base.SessionContext;
import io.graphmdl.base.sql.SqlConverter;
import io.trino.sql.tree.Node;

import javax.inject.Inject;

import static io.graphmdl.sqlrewrite.Utils.parseSql;
import static io.trino.sql.SqlFormatter.Dialect.POSTGRES;
import static io.trino.sql.SqlFormatter.formatSql;

public class PostgresSqlConverter
        implements SqlConverter
{
    private static final Logger LOG = Logger.get(PostgresSqlConverter.class);

    @Inject
    public PostgresSqlConverter() {}

    @Override
    public String convert(String sql, SessionContext sessionContext)
    {
        Node rewrittenNode = parseSql(sql);
        String dialectSql = formatSql(rewrittenNode, POSTGRES);
        LOG.info("[Dialect sql]: %s", dialectSql);
        return dialectSql;
    }
}
