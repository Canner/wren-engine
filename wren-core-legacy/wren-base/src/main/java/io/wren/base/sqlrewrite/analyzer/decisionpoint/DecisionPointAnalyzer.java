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
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.With;
import io.wren.base.CatalogSchemaTableName;
import io.wren.base.SessionContext;
import io.wren.base.WrenMDL;
import io.wren.base.dto.Column;
import io.wren.base.sqlrewrite.analyzer.Analysis;
import io.wren.base.sqlrewrite.analyzer.Field;
import io.wren.base.sqlrewrite.analyzer.StatementAnalyzer;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.sql.ExpressionFormatter.formatExpression;
import static io.wren.base.sqlrewrite.Utils.toCatalogSchemaTableName;
import static io.wren.base.sqlrewrite.analyzer.decisionpoint.DecisionExpressionAnalyzer.DEFAULT_ANALYSIS;
import static io.wren.base.sqlrewrite.analyzer.decisionpoint.DecisionPointContext.isSubqueryOrCte;
import static io.wren.base.sqlrewrite.analyzer.decisionpoint.DecisionPointContext.withSubqueryOrCte;
import static io.wren.base.sqlrewrite.analyzer.decisionpoint.QueryAnalysis.ColumnAnalysis;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DecisionPointAnalyzer
{
    private DecisionPointAnalyzer() {}

    public static List<QueryAnalysis> analyze(Statement query, SessionContext sessionContext, WrenMDL mdl)
    {
        Analysis analysis = new Analysis(query);
        StatementAnalyzer.analyze(analysis, query, sessionContext, mdl);
        Visitor visitor = new Visitor(analysis, sessionContext, mdl);
        visitor.process(query);
        return visitor.queries;
    }

    static class Visitor
            extends DefaultTraversalVisitor<DecisionPointContext>
    {
        private final Analysis analysis;
        private final SessionContext sessionContext;
        private final WrenMDL mdl;
        private final List<QueryAnalysis> queries = new ArrayList<>();

        public Visitor(Analysis analysis, SessionContext sessionContext, WrenMDL mdl)
        {
            this.analysis = requireNonNull(analysis, "analysis is null");
            this.sessionContext = requireNonNull(sessionContext, "sessionContext is null");
            this.mdl = requireNonNull(mdl, "mdl is null");
        }

        @Override
        protected Void visitWith(With node, DecisionPointContext decisionPointContext)
        {
            return super.visitWith(node, withSubqueryOrCte(decisionPointContext, true));
        }

        @Override
        protected Void visitTableSubquery(TableSubquery node, DecisionPointContext decisionPointContext)
        {
            return super.visitTableSubquery(node, withSubqueryOrCte(decisionPointContext, true));
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, DecisionPointContext decisionPointContext)
        {
            QueryAnalysis.Builder builder = QueryAnalysis.builder();
            DecisionPointContext selfDecisionPointContext = new DecisionPointContext(builder, analysis.getScope(node), isSubqueryOrCte(decisionPointContext));
            process(node.getSelect(), selfDecisionPointContext);
            node.getFrom().ifPresent(from -> builder.setRelation(RelationAnalyzer.analyze(from, sessionContext, mdl, analysis)));
            node.getWhere().ifPresent(where -> builder.setFilter(FilterAnalyzer.analyze(where, selfDecisionPointContext.getScope())));

            if (node.getGroupBy().isPresent()) {
                process(node.getGroupBy().get(), selfDecisionPointContext);
            }

            if (node.getOrderBy().isPresent()) {
                process(node.getOrderBy().get(), selfDecisionPointContext);
            }
            builder.setSubqueryOrCte(isSubqueryOrCte(decisionPointContext));
            queries.add(builder.build());
            return null;
        }

        @Override
        protected Void visitAllColumns(AllColumns node, DecisionPointContext decisionPointContext)
        {
            List<Field> scopedFields = decisionPointContext.getScope().getRelationType().getFields();
            if (node.getTarget().isPresent()) {
                String target = formatExpression(node.getTarget().get(), SqlFormatter.Dialect.DEFAULT);
                CatalogSchemaTableName catalogSchemaTableName = toCatalogSchemaTableName(sessionContext, QualifiedName.of(List.of(target.split("\\."))));
                if (scopedFields.isEmpty()) {
                    // relation scope can't be analyzed, so we can't determine the fields. It may be a remote table.
                    decisionPointContext.getBuilder().addSelectItem(new ColumnAnalysis(
                            Optional.empty(),
                            format("%s.*", target),
                            DEFAULT_ANALYSIS.toMap(),
                            node.getLocation().orElse(null),
                            List.of()));
                }
                else {
                    scopedFields.stream()
                            .filter(field -> field.getRelationAlias().filter(alias -> alias.toString().equals(target)).isPresent() || field.getTableName().equals(catalogSchemaTableName))
                            .filter(field -> field.getSourceColumn().stream().anyMatch(column -> !column.isCalculated() && column.getRelationship().isEmpty()))
                            .forEach(field -> {
                                decisionPointContext.getBuilder().addSelectItem(new ColumnAnalysis(
                                        Optional.empty(),
                                        field.getName().orElse(field.getColumnName()),
                                        DEFAULT_ANALYSIS.toMap(),
                                        node.getLocation().orElse(null),
                                        List.of(new ExprSource(
                                                field.getName().orElse(field.getColumnName()),
                                                field.getTableName().getSchemaTableName().getTableName(),
                                                field.getSourceColumn().map(Column::getName).orElse(null),
                                                node.getLocation().orElse(null)))));
                            });
                }
            }
            else {
                if (scopedFields.isEmpty()) {
                    // relation scope can't be analyzed, so we can't determine the fields. It may be a remote table.
                    decisionPointContext.getBuilder().addSelectItem(new ColumnAnalysis(
                            Optional.empty(),
                            "*",
                            DEFAULT_ANALYSIS.toMap(),
                            node.getLocation().orElse(null),
                            List.of()));
                }
                else {
                    scopedFields.stream()
                            .filter(field -> field.getSourceColumn().stream().anyMatch(column -> !column.isCalculated() && column.getRelationship().isEmpty()))
                            .forEach(field -> {
                                decisionPointContext.getBuilder().addSelectItem(new ColumnAnalysis(
                                        Optional.empty(),
                                        field.getName().orElse(field.getColumnName()),
                                        DEFAULT_ANALYSIS.toMap(),
                                        node.getLocation().orElse(null),
                                        List.of(new ExprSource(
                                                field.getName().orElse(field.getColumnName()),
                                                field.getTableName().getSchemaTableName().getTableName(),
                                                field.getSourceColumn().map(Column::getName).orElse(null),
                                                node.getLocation().orElse(null)))));
                            });
                }
            }
            return null;
        }

        @Override
        protected Void visitSingleColumn(SingleColumn node, DecisionPointContext decisionPointContext)
        {
            DecisionExpressionAnalyzer.DecisionExpressionAnalysis expressionAnalysis = DecisionExpressionAnalyzer.analyze(node.getExpression());
            String expression = formatExpression(node.getExpression(), SqlFormatter.Dialect.DEFAULT);
            Set<ExprSource> exprSources = RelationAnalyzer.ExpressionSourceAnalyzer.analyze(node.getExpression(), decisionPointContext.getScope());
            decisionPointContext.getBuilder().addSelectItem(new ColumnAnalysis(
                    node.getAlias().map(Identifier::getValue),
                    expression,
                    expressionAnalysis.toMap(),
                    node.getLocation().orElse(null),
                    List.copyOf(exprSources)));
            return null;
        }

        @Override
        protected Void visitGroupBy(GroupBy node, DecisionPointContext decisionPointContext)
        {
            ImmutableList.Builder<List<QueryAnalysis.GroupByKey>> groups = ImmutableList.builder();
            for (GroupingElement groupingElement : node.getGroupingElements()) {
                ImmutableList.Builder<QueryAnalysis.GroupByKey> keys = ImmutableList.builder();
                for (Expression expression : groupingElement.getExpressions()) {
                    if (expression instanceof LongLiteral) {
                        QueryAnalysis.ColumnAnalysis field = decisionPointContext.getBuilder().getSelectItems().get((int) ((LongLiteral) expression).getValue() - 1);
                        keys.add(new QueryAnalysis.GroupByKey(field.getAliasName().orElse(field.getExpression()), expression.getLocation().orElse(null), field.getExprSources()));
                    }
                    else {
                        List<ExprSource> exprSources = List.copyOf(RelationAnalyzer.ExpressionSourceAnalyzer.analyze(expression, decisionPointContext.getScope()));
                        keys.add(new QueryAnalysis.GroupByKey(formatExpression(expression, SqlFormatter.Dialect.DEFAULT), expression.getLocation().orElse(null), exprSources));
                    }
                }
                groups.add(keys.build());
            }
            decisionPointContext.getBuilder().setGroupByKeys(groups.build());
            return null;
        }

        @Override
        protected Void visitOrderBy(OrderBy node, DecisionPointContext decisionPointContext)
        {
            decisionPointContext.getBuilder().setSortings(
                    node.getSortItems().stream().map(sortItem -> {
                        if (sortItem.getSortKey() instanceof LongLiteral) {
                            QueryAnalysis.ColumnAnalysis field = decisionPointContext.getBuilder().getSelectItems().get((int) ((LongLiteral) sortItem.getSortKey()).getValue() - 1);
                            return new QueryAnalysis.SortItemAnalysis(field.getAliasName().orElse(field.getExpression()), sortItem.getOrdering(), sortItem.getLocation().orElse(null), field.getExprSources());
                        }
                        List<ExprSource> exprSources = List.copyOf(RelationAnalyzer.ExpressionSourceAnalyzer.analyze(sortItem.getSortKey(), decisionPointContext.getScope()));
                        return new QueryAnalysis.SortItemAnalysis(formatExpression(sortItem.getSortKey(), SqlFormatter.Dialect.DEFAULT), sortItem.getOrdering(), sortItem.getLocation().orElse(null), exprSources);
                    }).toList());
            return null;
        }
    }
}
