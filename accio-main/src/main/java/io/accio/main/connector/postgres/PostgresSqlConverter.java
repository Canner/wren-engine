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

package io.accio.main.connector.postgres;

import com.google.common.collect.ImmutableList;
import io.accio.base.SessionContext;
import io.accio.base.sql.SqlConverter;
import io.accio.main.metadata.Metadata;
import io.accio.main.sql.SqlRewrite;
import io.accio.main.sql.postgres.RewriteToPostgresType;
import io.airlift.log.Logger;
import io.trino.sql.tree.Node;

import javax.inject.Inject;

import java.util.List;

import static io.accio.sqlrewrite.Utils.parseSql;
import static io.trino.sql.SqlFormatter.Dialect.POSTGRES;
import static io.trino.sql.SqlFormatter.formatSql;
import static java.util.Objects.requireNonNull;

public class PostgresSqlConverter
        implements SqlConverter
{
    private static final Logger LOG = Logger.get(PostgresSqlConverter.class);
    private final Metadata metadata;

    @Inject
    public PostgresSqlConverter(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public String convert(String sql, SessionContext sessionContext)
    {
        Node rewrittenNode = parseSql(sql);
        List<SqlRewrite> sqlRewrites = ImmutableList.of(RewriteToPostgresType.INSTANCE);
        LOG.info("[Input sql]: %s", sql);

        for (SqlRewrite rewrite : sqlRewrites) {
            LOG.debug("Before %s: %s", rewrite.getClass().getSimpleName(), formatSql(rewrittenNode));
            rewrittenNode = rewrite.rewrite(rewrittenNode, metadata);
            LOG.debug("After %s: %s", rewrite.getClass().getSimpleName(), formatSql(rewrittenNode));
        }

        String dialectSql = formatSql(rewrittenNode, POSTGRES);
        LOG.info("[Dialect sql]: %s", dialectSql);
        return dialectSql;
    }
}
