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

package io.cml.graphml;

import io.cml.graphml.analyzer.Analysis;
import io.cml.graphml.analyzer.Analyzer;
import io.cml.graphml.base.GraphML;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Statement;

import java.util.List;

import static io.cml.graphml.ModelSqlRewrite.MODEL_SQL_REWRITE;
import static io.cml.graphml.RelationshipRewrite.RELATIONSHIP_REWRITE;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;

public class GraphMLPlanner
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static List<GraphMLRule> allRules = List.of(
            // The ordering shouldn't be changed.
            MODEL_SQL_REWRITE,
            RELATIONSHIP_REWRITE);

    private GraphMLPlanner() {}

    public static String rewrite(String sql, GraphML graphML)
    {
        return rewrite(sql, graphML, allRules);
    }

    public static String rewrite(String sql, GraphML graphML, List<GraphMLRule> rules)
    {
        Statement statement = SQL_PARSER.createStatement(sql, new ParsingOptions(AS_DECIMAL));
        Analysis analysis = Analyzer.analyze(statement, graphML);
        Node result = statement;
        for (GraphMLRule rule : rules) {
            result = rule.apply(result, analysis, graphML);
        }
        return SqlFormatter.formatSql(result);
    }
}
