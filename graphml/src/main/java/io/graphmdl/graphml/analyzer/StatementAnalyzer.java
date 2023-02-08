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

package io.graphmdl.graphml.analyzer;

import io.graphmdl.graphml.RelationshipCteGenerator;
import io.graphmdl.graphml.base.GraphML;
import io.graphmdl.graphml.base.dto.Metric;
import io.graphmdl.graphml.base.dto.Model;
import io.graphmdl.graphml.base.dto.Relationship;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GroupingElement;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.Union;
import io.trino.sql.tree.Unnest;
import io.trino.sql.tree.Values;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Collectors.toUnmodifiableSet;

/**
 * Inspired by io.trino.sql.analyzer.StatementAnalyzer
 */
public final class StatementAnalyzer
{
    private StatementAnalyzer() {}

    public static Analysis analyze(Statement statement, GraphML graphML)
    {
        return analyze(statement, graphML, new RelationshipCteGenerator(graphML));
    }

    public static Analysis analyze(Statement statement, GraphML graphML, RelationshipCteGenerator relationshipCteGenerator)
    {
        Analysis analysis = new Analysis(statement, relationshipCteGenerator);
        new Visitor(analysis, graphML, relationshipCteGenerator).process(statement, Optional.empty());

        // add models directly used in sql query
        analysis.addModels(
                graphML.listModels().stream()
                        .filter(model -> analysis.getTables().stream().anyMatch(table -> model.getName().equals(table.toString())))
                        .collect(toUnmodifiableSet()));

        // add models required for relationships
        analysis.addModels(
                analysis.getRelationships().stream()
                        .map(Relationship::getModels)
                        .flatMap(List::stream)
                        .distinct()
                        .map(modelName ->
                                graphML.getModel(modelName)
                                        .orElseThrow(() -> new IllegalArgumentException(format("relationship model %s not exists", modelName))))
                        .collect(toUnmodifiableSet()));

        // add models required for metrics
        analysis.addModels(
                graphML.listMetrics().stream()
                        .filter(metric -> analysis.getTables().stream().anyMatch(table -> metric.getName().equals(table.toString())))
                        .map(Metric::getBaseModel)
                        .distinct()
                        .map(model -> graphML.getModel(model).orElseThrow(() -> new IllegalArgumentException(format("metric model %s not exists", model))))
                        .collect(toUnmodifiableSet()));

        return analysis;
    }

    private static class Visitor
            extends AstVisitor<Scope, Optional<Scope>>
    {
        private final Analysis analysis;
        private final GraphML graphML;
        private final RelationshipCteGenerator relationshipCteGenerator;

        public Visitor(Analysis analysis, GraphML graphML, RelationshipCteGenerator relationshipCteGenerator)
        {
            this.analysis = requireNonNull(analysis, "analysis is null");
            this.graphML = requireNonNull(graphML, "graphML is null");
            this.relationshipCteGenerator = requireNonNull(relationshipCteGenerator, "relationshipCteGenerator is null");
        }

        @Override
        protected Scope visitNode(Node node, Optional<Scope> context)
        {
            throw new IllegalStateException("Unsupported node type: " + node.getClass().getName());
        }

        @Override
        protected Scope visitTable(Table node, Optional<Scope> scope)
        {
            analysis.addTable(node.getName());
            // only record model fields here, others are ignored
            String tableName = node.getName().toString();
            List<Field> modelFields = graphML.getModel(tableName)
                    .map(Model::getColumns)
                    .orElseGet(List::of)
                    .stream()
                    .map(column ->
                            Field.builder()
                                    .relationAlias(Optional.of(QualifiedName.of(tableName)))
                                    .modelName(tableName)
                                    .columnName(column.getName())
                                    .name(Optional.of(column.getName()))
                                    .build())
                    .collect(toImmutableList());
            return Scope.builder()
                    .parent(scope)
                    .relationType(Optional.of(new RelationType(modelFields)))
                    .isTableScope(true)
                    .build();
        }

        @Override
        protected Scope visitQuery(Query node, Optional<Scope> scope)
        {
            Optional<Scope> withScope = analyzeWith(node, scope);
            Scope queryBodyScope = process(node.getQueryBody(), withScope);

            Scope queryScope = Scope.builder()
                    .parent(withScope)
                    .relationType(queryBodyScope.getRelationType())
                    .build();

            return queryScope;
        }

        @Override
        protected Scope visitQuerySpecification(QuerySpecification node, Optional<Scope> scope)
        {
            Scope sourceScope = analyzeFrom(node, scope);
            Set<String> relationshipCTENames = analyzeSelect(node, sourceScope).stream()
                    .map(ExpressionAnalysis::getRelationshipCTENames)
                    .flatMap(Set::stream)
                    .collect(toSet());
            node.getWhere().ifPresent(where -> relationshipCTENames.addAll(analyzeExpression(where, sourceScope).getRelationshipCTENames()));
            node.getGroupBy().ifPresent(groupBy ->
                    groupBy.getGroupingElements().stream()
                            .map(GroupingElement::getExpressions)
                            .flatMap(Collection::stream)
                            .forEach(expression -> relationshipCTENames.addAll(analyzeExpression(expression, sourceScope).getRelationshipCTENames())));
            node.getHaving().ifPresent(having -> relationshipCTENames.addAll(analyzeExpression(having, sourceScope).getRelationshipCTENames()));
            node.getOrderBy().ifPresent(orderBy ->
                    orderBy.getSortItems().stream()
                            .map(SortItem::getSortKey)
                            .forEach(expression -> relationshipCTENames.addAll(analyzeExpression(expression, sourceScope).getRelationshipCTENames())));
            node.getFrom()
                    .filter(from -> from instanceof Table)
                    .map(from -> (Table) from)
                    .ifPresent(table -> analysis.addReplaceTableWithCTEs(NodeRef.of(table), relationshipCTENames));
            // TODO: output scope here isn't right
            return Scope.builder().parent(scope).build();
        }

        @Override
        protected Scope visitValues(Values node, Optional<Scope> scope)
        {
            return Scope.builder().parent(scope).build();
        }

        @Override
        protected Scope visitUnnest(Unnest node, Optional<Scope> scope)
        {
            // TODO: output scope here isn't right
            return Scope.builder().parent(scope).build();
        }

        @Override
        protected Scope visitUnion(Union node, Optional<Scope> scope)
        {
            // TODO: output scope here isn't right
            return Scope.builder().parent(scope).build();
        }

        @Override
        protected Scope visitJoin(Join node, Optional<Scope> scope)
        {
            process(node.getLeft(), scope);
            process(node.getRight(), scope);
            // TODO: output scope here isn't right
            return Scope.builder().parent(scope).build();
        }

        @Override
        protected Scope visitAliasedRelation(AliasedRelation relation, Optional<Scope> scope)
        {
            process(relation.getRelation(), scope);
            // TODO: output scope here isn't right
            return Scope.builder().parent(scope).build();
        }

        @Override
        protected Scope visitTableSubquery(TableSubquery node, Optional<Scope> scope)
        {
            process(node.getQuery());
            // TODO: output scope here isn't right
            return Scope.builder().parent(scope).build();
        }

        // TODO: implement analyze with
        private Optional<Scope> analyzeWith(Query node, Optional<Scope> scope)
        {
            if (node.getWith().isEmpty()) {
                return Optional.empty();
            }

            With with = node.getWith().get();
            for (WithQuery withQuery : with.getQueries()) {
                process(withQuery.getQuery(), scope);
            }
            // TODO: output scope here isn't right
            return Optional.of(Scope.builder().parent(scope).build());
        }

        private Scope analyzeFrom(QuerySpecification node, Optional<Scope> scope)
        {
            if (node.getFrom().isPresent()) {
                return process(node.getFrom().get(), scope);
            }
            return Scope.builder().parent(scope).build();
        }

        private List<ExpressionAnalysis> analyzeSelect(QuerySpecification node, Scope scope)
        {
            List<ExpressionAnalysis> selectExpressionAnalyses = new ArrayList<>();
            for (SelectItem item : node.getSelect().getSelectItems()) {
                if (item instanceof SingleColumn) {
                    selectExpressionAnalyses.add(analyzeSelectSingleColumn((SingleColumn) item, scope));
                }
                else if (item instanceof AllColumns) {
                    // DO NOTHING
                }
                else {
                    throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
                }
            }
            return List.copyOf(selectExpressionAnalyses);
        }

        private ExpressionAnalysis analyzeSelectSingleColumn(SingleColumn singleColumn, Scope scope)
        {
            Expression expression = singleColumn.getExpression();
            return analyzeExpression(expression, scope);
        }

        private ExpressionAnalysis analyzeExpression(Expression expression, Scope scope)
        {
            ExpressionAnalysis expressionAnalysis = ExpressionAnalyzer.analyze(expression, graphML, relationshipCteGenerator, scope);
            analysis.addRelationshipFields(expressionAnalysis.getRelationshipFieldRewrites());
            analysis.addRelationships(expressionAnalysis.getRelationships());
            return expressionAnalysis;
        }
    }
}
