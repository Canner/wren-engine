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

package io.wren.main.connector.duckdb;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.sql.tree.Node;
import io.wren.base.SessionContext;
import io.wren.base.sql.SqlConverter;
import io.wren.main.metadata.Metadata;
import io.wren.main.sql.SqlRewrite;
import io.wren.main.sql.duckdb.RewriteArray;
import io.wren.main.sql.duckdb.RewriteFunction;
import org.intellij.lang.annotations.Language;

import java.util.List;

import static io.trino.sql.SqlFormatter.Dialect.DUCKDB;
import static io.trino.sql.SqlFormatter.formatSql;
import static io.wren.base.sqlrewrite.Utils.parseSql;
import static java.util.Objects.requireNonNull;

public class DuckDBSqlConverter
        implements SqlConverter
{
    private static final Logger LOG = Logger.get(DuckDBSqlConverter.class);

    private final Metadata metadata;

    @Inject
    public DuckDBSqlConverter(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public String convert(@Language("sql") String sql, SessionContext sessionContext)
    {
        Node rewrittenNode = parseSql(sql);

        List<SqlRewrite> sqlRewrites = ImmutableList.of(
                // DuckDB doesn't support `ARRAY[1,2,3][1]` but `array_value(1,2,3])[1]` is supported
                RewriteArray.INSTANCE,
                RewriteFunction.INSTANCE);

        LOG.info("[Input sql]: %s", sql);

        for (SqlRewrite rewrite : sqlRewrites) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Before %s: %s", rewrite.getClass().getSimpleName(), formatSql(rewrittenNode));
            }
            rewrittenNode = rewrite.rewrite(rewrittenNode, metadata);
            if (LOG.isDebugEnabled()) {
                LOG.debug("After %s: %s", rewrite.getClass().getSimpleName(), formatSql(rewrittenNode));
            }
        }
        String dialectSql = formatSql(rewrittenNode, DUCKDB);
        LOG.info("[Dialect sql]: %s", dialectSql);
        return dialectSql;
    }
}
