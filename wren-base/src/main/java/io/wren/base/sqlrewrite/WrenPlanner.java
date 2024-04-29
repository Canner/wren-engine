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

package io.wren.base.sqlrewrite;

import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import io.wren.base.AnalyzedMDL;
import io.wren.base.SessionContext;

import java.util.List;

import static io.wren.base.sqlrewrite.EnumRewrite.ENUM_REWRITE;
import static io.wren.base.sqlrewrite.MetricRollupRewrite.METRIC_ROLLUP_REWRITE;
import static io.wren.base.sqlrewrite.Utils.parseSql;
import static io.wren.base.sqlrewrite.WrenSqlRewrite.WREN_SQL_REWRITE;

public class WrenPlanner
{
    public static final List<WrenRule> ALL_RULES = List.of(
            GenerateViewRewrite.GENERATE_VIEW_REWRITE,
            METRIC_ROLLUP_REWRITE,
            WREN_SQL_REWRITE,
            ENUM_REWRITE);
    private static final SqlParser SQL_PARSER = new SqlParser();

    private WrenPlanner() {}

    public static String rewrite(String sql, SessionContext sessionContext, AnalyzedMDL analyzedMDL)
    {
        return rewrite(sql, sessionContext, analyzedMDL, ALL_RULES);
    }

    public static String rewrite(String sql, SessionContext sessionContext, AnalyzedMDL analyzedMDL, List<WrenRule> rules)
    {
        Statement statement = parseSql(sql);
        for (WrenRule rule : rules) {
            // we will replace or rewrite sql node in sql rewrite, to avoid rewrite rules affect each other, format and parse sql before each rewrite
            statement = rule.apply(parseSql(SqlFormatter.formatSql(statement)), sessionContext, analyzedMDL);
        }
        return SqlFormatter.formatSql(statement);
    }
}
