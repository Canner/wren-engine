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

package io.graphmdl.sqlrewrite;

import io.graphmdl.base.GraphMDL;
import io.graphmdl.base.SessionContext;
import io.graphmdl.sqlrewrite.analyzer.Analysis;
import io.graphmdl.sqlrewrite.analyzer.StatementAnalyzer;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Statement;

import java.util.List;

import static io.graphmdl.sqlrewrite.GraphMDLSqlRewrite.GRAPHMDL_SQL_REWRITE;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;

public class GraphMDLPlanner
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final List<GraphMDLRule> ALL_RULES = List.of(GRAPHMDL_SQL_REWRITE);

    private GraphMDLPlanner() {}

    public static String rewrite(String sql, SessionContext sessionContext, GraphMDL graphMDL)
    {
        return rewrite(sql, sessionContext, graphMDL, ALL_RULES);
    }

    public static String rewrite(String sql, SessionContext sessionContext, GraphMDL graphMDL, List<GraphMDLRule> rules)
    {
        Statement statement = SQL_PARSER.createStatement(sql, new ParsingOptions(AS_DECIMAL));
        Statement scopedStatement = ScopeRewrite.SCOPE_REWRITE.rewrite(statement, graphMDL);
        Analysis analysis = StatementAnalyzer.analyze(scopedStatement, sessionContext, graphMDL);
        Node result = scopedStatement;
        for (GraphMDLRule rule : rules) {
            result = rule.apply(result, sessionContext, analysis, graphMDL);
        }
        return SqlFormatter.formatSql(result);
    }
}
