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

package io.wren.base.sqlrewrite.analyzer.decisionpoint;

import com.google.common.collect.ImmutableList;
import io.trino.sql.SqlFormatter;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GroupBy;
import io.trino.sql.tree.GroupingElement;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Statement;
import io.wren.base.CatalogSchemaTableName;
import io.wren.base.SessionContext;
import io.wren.base.WrenMDL;
import io.wren.base.sqlrewrite.analyzer.Analysis;
import io.wren.base.sqlrewrite.analyzer.Scope;
import io.wren.base.sqlrewrite.analyzer.StatementAnalyzer;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.sql.ExpressionFormatter.formatExpression;
import static io.wren.base.sqlrewrite.Utils.toCatalogSchemaTableName;
import static io.wren.base.sqlrewrite.analyzer.decisionpoint.DecisionExpressionAnalyzer.DEFAULT_ANALYSIS;

public class DecisionPointAnalyzer
{
    private DecisionPointAnalyzer() {}

    public static List<QueryAnalysis> analyze(Statement query, SessionContext sessionContext, WrenMDL mdl)
    {
        Analysis analysis = new Analysis(query);
        StatementAnalyzer.analyze(analysis, query, sessionContext, mdl);
        Visitor visitor = new Visitor(analysis, sessionContext);
        visitor.process(query);
        return visitor.queries;
    }

    static class Visitor
            extends DefaultTraversalVisitor<Context>
    {
        private final Analysis analysis;
        private final SessionContext sessionContext;
        private final List<QueryAnalysis> queries = new ArrayList<>();

        public Visitor(Analysis analysis, SessionContext sessionContext)
        {
            this.analysis = analysis;
            this.sessionContext = sessionContext;
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, Context ignored)
        {
            QueryAnalysis.Builder builder = QueryAnalysis.builder();
            Context context = new Context(builder, analysis.getScope(node));
            process(node.getSelect(), context);
            node.getFrom().ifPresent(from -> builder.setRelation(RelationAnalyzer.analyze(from)));
            node.getWhere().ifPresent(where -> builder.setFilter(FilterAnalyzer.analyze(where)));

            if (node.getGroupBy().isPresent()) {
                process(node.getGroupBy().get(), context);
            }

            if (node.getOrderBy().isPresent()) {
                process(node.getOrderBy().get(), context);
            }
            queries.add(builder.build());
            return null;
        }

        @Override
        protected Void visitAllColumns(AllColumns node, Context context)
        {
            if (node.getTarget().isPresent()) {
                String target = formatExpression(node.getTarget().get(), SqlFormatter.Dialect.DEFAULT);
                CatalogSchemaTableName catalogSchemaTableName = toCatalogSchemaTableName(sessionContext, QualifiedName.of(List.of(target.split("\\."))));
                context.scope.getRelationType().getFields().stream()
                        .filter(field -> field.getRelationAlias().filter(alias -> alias.toString().equals(target)).isPresent() || field.getTableName().equals(catalogSchemaTableName))
                        .forEach(field -> {
                            context.builder.addSelectItem(new QueryAnalysis.ColumnAnalysis(Optional.empty(), field.getName().orElse(field.getColumnName()), DEFAULT_ANALYSIS.toMap()));
                        });
            }
            else {
                context.scope.getRelationType().getFields().forEach(field -> {
                    context.builder.addSelectItem(new QueryAnalysis.ColumnAnalysis(Optional.empty(), field.getName().orElse(field.getColumnName()), DEFAULT_ANALYSIS.toMap()));
                });
            }
            return null;
        }

        @Override
        protected Void visitSingleColumn(SingleColumn node, Context context)
        {
            DecisionExpressionAnalyzer.DecisionExpressionAnalysis expressionAnalysis = DecisionExpressionAnalyzer.analyze(node.getExpression());
            String expression = formatExpression(node.getExpression(), SqlFormatter.Dialect.DEFAULT);
            context.builder.addSelectItem(new QueryAnalysis.ColumnAnalysis(node.getAlias().map(Identifier::getValue), expression, expressionAnalysis.toMap()));
            return null;
        }

        @Override
        protected Void visitGroupBy(GroupBy node, Context context)
        {
            ImmutableList.Builder<List<String>> groups = ImmutableList.builder();
            for (GroupingElement groupingElement : node.getGroupingElements()) {
                ImmutableList.Builder<String> keys = ImmutableList.builder();
                for (Expression expression : groupingElement.getExpressions()) {
                    if (expression instanceof LongLiteral) {
                        QueryAnalysis.ColumnAnalysis field = context.builder.getSelectItems().get((int) ((LongLiteral) expression).getValue() - 1);
                        keys.add(field.getAliasName().orElse(field.getExpression()));
                    }
                    keys.add(formatExpression(expression, SqlFormatter.Dialect.DEFAULT));
                }
                groups.add(keys.build());
            }
            context.builder.setGroupByKeys(groups.build());
            return null;
        }

        @Override
        protected Void visitOrderBy(OrderBy node, Context context)
        {
            context.builder.setSortings(
                    node.getSortItems().stream().map(sortItem -> {
                        if (sortItem.getSortKey() instanceof LongLiteral) {
                            QueryAnalysis.ColumnAnalysis field = context.builder.getSelectItems().get((int) ((LongLiteral) sortItem.getSortKey()).getValue() - 1);
                            return new QueryAnalysis.SortItemAnalysis(field.getAliasName().orElse(field.getExpression()), sortItem.getOrdering());
                        }
                        return new QueryAnalysis.SortItemAnalysis(formatExpression(sortItem.getSortKey(), SqlFormatter.Dialect.DEFAULT), sortItem.getOrdering());
                    }).toList());
            return null;
        }
    }

    record Context(QueryAnalysis.Builder builder, Scope scope) {}
}
