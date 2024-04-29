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

package io.wren.main.connector.snowflake;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.sql.tree.Node;
import io.wren.base.SessionContext;
import io.wren.base.sql.SqlConverter;
import io.wren.main.sql.SqlRewrite;
import io.wren.main.sql.snowflake.RewriteCastStringAsByteaWithHexEncode;
import io.wren.main.sqlglot.SQLGlot;
import io.wren.main.sqlglot.SQLGlotConverter;
import org.intellij.lang.annotations.Language;

import java.util.List;

import static io.trino.sql.SqlFormatter.formatSql;
import static io.wren.base.sqlrewrite.Utils.parseSql;

public class SnowflakeSqlConverter
        implements SqlConverter
{
    private static final Logger LOG = Logger.get(SnowflakeSqlConverter.class);

    private final SQLGlotConverter sqlGlotConverter;

    @Inject
    public SnowflakeSqlConverter(SQLGlot sqlGlot)
    {
        sqlGlotConverter = SQLGlotConverter.builder()
                .setSQLGlot(sqlGlot)
                .setWriteDialect(SQLGlot.Dialect.SNOWFLAKE)
                .build();
    }

    @Override
    public String convert(@Language("sql") String sql, SessionContext sessionContext)
    {
        Node rewrittenNode = parseSql(sql);

        LOG.info("[Input sql]: %s", sql);

        // We may use custom dialect of SqlGlot instead of this
        List<SqlRewrite> sqlRewrites = ImmutableList.of(
                RewriteCastStringAsByteaWithHexEncode.INSTANCE);

        for (SqlRewrite rewrite : sqlRewrites) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Before %s: %s", rewrite.getClass().getSimpleName(), formatSql(rewrittenNode));
            }
            rewrittenNode = rewrite.rewrite(rewrittenNode, null);
            if (LOG.isDebugEnabled()) {
                LOG.debug("After %s: %s", rewrite.getClass().getSimpleName(), formatSql(rewrittenNode));
            }
        }

        String sqlGlotConverted = sqlGlotConverter.convert(formatSql(rewrittenNode), sessionContext);

        LOG.info("[Dialect sql]: %s", sqlGlotConverted);

        return sqlGlotConverted;
    }
}
