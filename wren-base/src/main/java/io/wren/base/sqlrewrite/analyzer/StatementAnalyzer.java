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

package io.wren.base.sqlrewrite.analyzer;

import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionRelation;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.JoinCriteria;
import io.trino.sql.tree.JoinOn;
import io.trino.sql.tree.Limit;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SetOperation;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.Unnest;
import io.trino.sql.tree.Values;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;
import io.wren.base.CatalogSchemaTableName;
import io.wren.base.SessionContext;
import io.wren.base.Utils;
import io.wren.base.WrenMDL;
import io.wren.base.dto.CumulativeMetric;
import io.wren.base.dto.Metric;
import io.wren.base.dto.Model;
import io.wren.base.dto.TimeUnit;
import io.wren.base.dto.View;
import io.wren.base.sqlrewrite.analyzer.matcher.PredicateMatcher;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.QueryUtil.getQualifiedName;
import static io.wren.base.metadata.StandardErrorCode.TYPE_MISMATCH;
import static io.wren.base.sqlrewrite.Utils.toCatalogSchemaTableName;
import static io.wren.base.sqlrewrite.analyzer.Analysis.SimplePredicate;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableSet;

/**
 * Inspired by io.trino.sql.analyzer.StatementAnalyzer
 */
public final class StatementAnalyzer
{
    private StatementAnalyzer() {}

    public static Scope analyze(Analysis analysis, Statement statement, SessionContext sessionContext, WrenMDL wrenMDL)
    {
        return analyze(analysis, statement, sessionContext, wrenMDL, null);
    }

    public static Scope analyze(Analysis analysis, Statement statement, SessionContext sessionContext, WrenMDL wrenMDL, TypeCoercion typeCoercion)
    {
        Scope queryScope = new Visitor(sessionContext, analysis, wrenMDL, typeCoercion).process(statement, Optional.empty());

        // add models directly used in sql query
        analysis.addModels(
                wrenMDL.listModels().stream()
                        .filter(model -> analysis.getTables().stream()
                                .filter(table -> table.getCatalogName().equals(wrenMDL.getCatalog()))
                                .filter(table -> table.getSchemaTableName().getSchemaName().equals(wrenMDL.getSchema()))
                                .anyMatch(table -> table.getSchemaTableName().getTableName().equals(model.getName())))
                        .collect(toUnmodifiableSet()));

        Set<Metric> metrics = analysis.getTables().stream()
                .map(wrenMDL::getMetric)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toUnmodifiableSet());

        Set<Metric> metricInMetricRollups = analysis.getMetricRollups().values().stream()
                .map(MetricRollupInfo::getMetric)
                .collect(toUnmodifiableSet());

        // TODO: remove this check
        Utils.checkArgument(metrics.stream().noneMatch(metricInMetricRollups::contains), "duplicate metrics in metrics and metric rollups");
        analysis.addMetrics(metrics);

        Set<CumulativeMetric> cumulativeMetrics = analysis.getTables().stream()
                .map(wrenMDL::getCumulativeMetric)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toUnmodifiableSet());
        analysis.addCumulativeMetrics(cumulativeMetrics);

        Set<View> views = analysis.getTables().stream()
                .map(wrenMDL::getView)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toUnmodifiableSet());

        analysis.addViews(views);
        return queryScope;
    }

    private static class Visitor
            extends AstVisitor<Scope, Optional<Scope>>
    {
        private final SessionContext sessionContext;
        private final Analysis analysis;
        private final WrenMDL wrenMDL;
        private final Optional<TypeCoercion> typeCoercionOptional;

        public Visitor(
                SessionContext sessionContext,
                Analysis analysis,
                WrenMDL wrenMDL,
                @Nullable TypeCoercion typeCoercion)
        {
            this.sessionContext = requireNonNull(sessionContext, "sessionContext is null");
            this.analysis = requireNonNull(analysis, "analysis is null");
            this.wrenMDL = requireNonNull(wrenMDL, "wrenMDL is null");
            this.typeCoercionOptional = Optional.ofNullable(typeCoercion);
        }

        public Scope process(Node node)
        {
            return process(node, Optional.empty());
        }

        @Override
        protected Scope visitNode(Node node, Optional<Scope> context)
        {
            throw new IllegalStateException("Unsupported node type: " + node.getClass().getName());
        }

        @Override
        protected Scope visitTable(Table node, Optional<Scope> scope)
        {
            if (node.getName().getPrefix().isEmpty() && scope.isPresent()) {
                // is this a reference to a WITH query?
                Optional<WithQuery> withQuery = scope.get().getNamedQuery(node.getName().getSuffix());
                if (withQuery.isPresent()) {
                    // currently we only care about the table that is actually a model instead of a alias table that use cte table
                    // return empty scope here.
                    Scope outputScope = createScopeForCommonTableExpression(node, withQuery.get(), scope);
                    analysis.setScope(node, outputScope);
                    return outputScope;
                }
            }

            CatalogSchemaTableName tableName = toCatalogSchemaTableName(sessionContext, node.getName());
            analysis.addTable(tableName);
            Scope outputScope;
            if (tableName.getCatalogName().equals(wrenMDL.getCatalog()) && tableName.getSchemaTableName().getSchemaName().equals(wrenMDL.getSchema())) {
                analysis.addSourceNodeName(NodeRef.of(node), QualifiedName.of(tableName.getSchemaTableName().getTableName()));
                List<Field> fields = collectFieldFromMDL(tableName);

                // if catalog and schema matches, but table name doesn't match any model, we assume it's a remote data source table
                if (fields.isEmpty()) {
                    outputScope = Scope.builder()
                            .parent(scope)
                            .isDataSourceScope(true)
                            .build();
                }
                else {
                    outputScope = Scope.builder()
                            .parent(scope)
                            .relationId(RelationId.of(node))
                            .relationType(new RelationType(fields))
                            .build();
                }
            }
            else {
                outputScope = Scope.builder()
                        .parent(scope)
                        .relationId(RelationId.of(node))
                        .relationType(new RelationType())
                        .build();
            }
            analysis.setScope(node, outputScope);
            return outputScope;
        }

        private Scope createScopeForCommonTableExpression(Table table, WithQuery withQuery, Optional<Scope> scope)
        {
            Query query = withQuery.getQuery();
            Analysis analyzed = new Analysis(query);
            Optional<Scope> queryScope = Optional.ofNullable(analyze(analyzed, query, sessionContext, wrenMDL, typeCoercionOptional.orElse(null)));
            List<Field> fields;
            Optional<List<Identifier>> columnNames = withQuery.getColumnNames();
            if (columnNames.isPresent()) {
                List<Identifier> aliasNames = columnNames.get();
                AtomicInteger i = new AtomicInteger();
                List<Field> scopedFields = createScopeForQuery(query, table.getName(), queryScope);
                Utils.checkArgument(aliasNames.size() == scopedFields.size(), "Column alias count does not match query column count");
                fields = scopedFields.stream()
                        .map(field -> Field.builder()
                                .like(field)
                                .name(aliasNames.get(i.getAndIncrement()).getValue())
                                .build())
                        .collect(toImmutableList());
            }
            else {
                fields = createScopeForQuery(query, table.getName(), queryScope);
            }
            return createAndAssignScope(table, scope, new RelationType(fields));
        }

        private List<Field> createScopeForQuery(Query query, QualifiedName scopeName, Optional<Scope> scope)
        {
            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            if (query.getQueryBody() instanceof QuerySpecification) {
                QuerySpecification body = (QuerySpecification) query.getQueryBody();
                for (SelectItem selectItem : body.getSelect().getSelectItems()) {
                    if (selectItem instanceof AllColumns) {
                        scope.ifPresent(s -> s.getRelationType().getFields()
                                .forEach(f -> fields.add(Field.builder()
                                        .columnName(f.getColumnName())
                                        .name(f.getName().orElse(f.getColumnName()))
                                        .tableName(toCatalogSchemaTableName(sessionContext, scopeName))
                                        .build())));
                    }
                    else {
                        SingleColumn singleColumn = (SingleColumn) selectItem;
                        String name = singleColumn.getAlias().map(Identifier::getValue).orElse(singleColumn.getExpression().toString());
                        fields.add(Field.builder()
                                .columnName(name)
                                .name(name)
                                .tableName(toCatalogSchemaTableName(sessionContext, scopeName))
                                .build());
                    }
                }
            }
            return fields.build();
        }

        private List<Field> collectFieldFromMDL(CatalogSchemaTableName tableName)
        {
            if (wrenMDL.getModel(tableName.getSchemaTableName().getTableName()).isPresent()) {
                return wrenMDL.getModel(tableName.getSchemaTableName().getTableName())
                        .map(Model::getColumns)
                        .orElseGet(ImmutableList::of)
                        .stream()
                        .map(column -> Field.builder()
                                .tableName(tableName)
                                .columnName(column.getName())
                                .name(column.getName())
                                .build())
                        .collect(toImmutableList());
            }
            else if (wrenMDL.getMetric(tableName.getSchemaTableName().getTableName()).isPresent()) {
                return wrenMDL.getMetric(tableName.getSchemaTableName().getTableName())
                        .map(Metric::getColumns)
                        .orElseGet(ImmutableList::of)
                        .stream()
                        .map(column -> Field.builder()
                                .tableName(tableName)
                                .columnName(column.getName())
                                .name(column.getName())
                                .build())
                        .collect(toImmutableList());
            }
            else if (wrenMDL.getCumulativeMetric(tableName.getSchemaTableName().getTableName()).isPresent()) {
                CumulativeMetric cumulativeMetric = wrenMDL.getCumulativeMetric(tableName.getSchemaTableName().getTableName()).get();
                return ImmutableList.of(
                        Field.builder()
                                .tableName(tableName)
                                .columnName(cumulativeMetric.getWindow().getName())
                                .name(cumulativeMetric.getWindow().getName())
                                .build(),
                        Field.builder()
                                .tableName(tableName)
                                .columnName(cumulativeMetric.getMeasure().getName())
                                .name(cumulativeMetric.getMeasure().getName())
                                .build());
            }
            return ImmutableList.of();
        }

        @Override
        protected Scope visitQuery(Query node, Optional<Scope> scope)
        {
            Optional<Scope> withScope = analyzeWith(node, scope);
            Scope queryBodyScope = process(node.getQueryBody(), withScope);
            return createAndAssignScope(node, scope, queryBodyScope);
        }

        @Override
        protected Scope visitQuerySpecification(QuerySpecification node, Optional<Scope> scope)
        {
            Scope sourceScope = analyzeFrom(node, scope);
            List<Expression> outputExpressions = analyzeSelect(node, sourceScope);
            node.getWhere().ifPresent(where -> analyzeWhere(where, sourceScope));
            node.getHaving().ifPresent(having -> analyzeExpression(having, sourceScope));
            node.getLimit().ifPresent(limit -> analysis.setLimit(((Limit) limit).getRowCount()));
            node.getOrderBy().ifPresent(orderBy -> orderBy.getSortItems()
                    .forEach(item -> {
                        QualifiedName name;
                        if (item.getSortKey() instanceof LongLiteral) {
                            long index = ((LongLiteral) item.getSortKey()).getValue() - 1;
                            name = getQualifiedName(outputExpressions.get((int) index));
                        }
                        else {
                            name = getQualifiedName(item.getSortKey());
                        }
                        analysis.addSortItem(new Analysis.SortItemAnalysis(name, item.getOrdering().name()));
                    }));
            // TODO: this scope is wrong.
            return createAndAssignScope(node, scope, sourceScope);
        }

        private List<Expression> analyzeSelect(QuerySpecification node, Scope scope)
        {
            ImmutableList.Builder<Expression> outputExpressions = ImmutableList.builder();
            for (SelectItem item : node.getSelect().getSelectItems()) {
                if (item instanceof AllColumns) {
                    analyzeSelectAllColumns((AllColumns) item, scope, outputExpressions);
                }
                else if (item instanceof SingleColumn) {
                    analyzeSelectSingleColumn((SingleColumn) item, scope, outputExpressions);
                }
                else {
                    throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
                }
            }
            return outputExpressions.build();
        }

        private void analyzeSelectAllColumns(AllColumns allColumns, Scope scope, ImmutableList.Builder<Expression> outputExpressions)
        {
            if (allColumns.getTarget().isPresent()) {
                // TODO handle target.*
            }
            else {
                analysis.addCollectedColumns(scope.getRelationType().getFields());
                scope.getRelationType().getFields().stream().map(field ->
                                field.getRelationAlias().map(DereferenceExpression::from)
                                        .orElse(DereferenceExpression.from(QualifiedName.of(field.getTableName().getSchemaTableName().getTableName(), field.getColumnName()))))
                        .forEach(outputExpressions::add);
            }
        }

        private void analyzeSelectSingleColumn(SingleColumn singleColumn, Scope scope, ImmutableList.Builder<Expression> outputExpressions)
        {
            outputExpressions.add(singleColumn.getAlias().map(name -> (Expression) name).orElse(singleColumn.getExpression()));
            // TODO: handle when singleColumn is a subquery
            ExpressionAnalysis expressionAnalysis = analyzeExpression(singleColumn.getExpression(), scope);

            if (expressionAnalysis.isRequireRelation()) {
                analysis.addRequiredSourceNode(scope.getRelationId().getSourceNode()
                        .orElseThrow(() -> new IllegalArgumentException("count(*) should have a followed source")));
            }

            typeCoercionOptional.ifPresent(typeCoercion -> {
                Optional<Expression> coerced = typeCoercion.coerceExpression(singleColumn.getExpression(), scope);
                coerced.ifPresent(expression -> analysis.addTypeCoercion(NodeRef.of(singleColumn.getExpression()), expression));
            });
        }

        private Scope analyzeFrom(QuerySpecification node, Optional<Scope> scope)
        {
            if (node.getFrom().isPresent()) {
                return process(node.getFrom().get(), scope);
            }
            return Scope.builder().parent(scope).build();
        }

        private void analyzeWhere(Expression node, Scope scope)
        {
            ExpressionAnalysis expressionAnalysis = analyzeExpression(node, scope);
            Map<NodeRef<Expression>, Field> fields = expressionAnalysis.getReferencedFields();
            expressionAnalysis.getPredicates().stream()
                    .filter(PredicateMatcher.PREDICATE_MATCHER::shapeMatches)
                    .forEach(comparisonExpression -> {
                        Expression expression = comparisonExpression.getLeft();
                        Optional.ofNullable(fields.get(NodeRef.of(expression)))
                                .ifPresent(field -> analysis.addSimplePredicate(
                                        new SimplePredicate(
                                                field.getTableName(),
                                                field.getColumnName(),
                                                comparisonExpression.getOperator(),
                                                comparisonExpression.getRight())));
                    });
            typeCoercionOptional.flatMap(typeCoercion -> typeCoercion.coerceExpression(node, scope))
                    .ifPresent(expression -> analysis.addTypeCoercion(NodeRef.of(node), expression));
        }

        @Override
        protected Scope visitValues(Values node, Optional<Scope> scope)
        {
            // TODO: output scope here isn't right
            return Scope.builder().parent(scope).build();
        }

        @Override
        protected Scope visitUnnest(Unnest node, Optional<Scope> scope)
        {
            // TODO: output scope here isn't right
            return Scope.builder().parent(scope).build();
        }

        @Override
        protected Scope visitFunctionRelation(FunctionRelation node, Optional<Scope> scope)
        {
            if (node.getName().toString().equalsIgnoreCase("roll_up")) {
                List<Expression> arguments = node.getArguments();
                Utils.checkArgument(arguments.size() == 3, "rollup function should have 3 arguments");

                QualifiedName tableName = getQualifiedName(arguments.get(0));
                Utils.checkArgument(tableName != null, "'%s' cannot be resolved", arguments.get(0));
                Utils.checkArgument(arguments.get(1) instanceof Identifier, "'%s' cannot be resolved", arguments.get(1));
                Utils.checkArgument(arguments.get(2) instanceof Identifier, "'%s' cannot be resolved", arguments.get(2));

                CatalogSchemaTableName catalogSchemaTableName = toCatalogSchemaTableName(sessionContext, tableName);
                Metric metric = wrenMDL.getMetric(catalogSchemaTableName).orElseThrow(() -> new IllegalArgumentException("Metric not found: " + catalogSchemaTableName));
                String timeColumn = ((Identifier) arguments.get(1)).getValue();

                analysis.addMetricRollups(
                        NodeRef.of(node),
                        new MetricRollupInfo(
                                metric,
                                metric.getTimeGrain(timeColumn).orElseThrow(() -> new IllegalArgumentException("Time column not found in metric: " + timeColumn)),
                                TimeUnit.timeUnit(((Identifier) arguments.get(2)).getValue())));
                // currently we don't care about metric rollup output scope
                return Scope.builder().parent(scope).build();
            }
            throw new IllegalArgumentException("FunctionRelation not supported: " + node.getName());
        }

        @Override
        protected Scope visitSetOperation(SetOperation node, Optional<Scope> scope)
        {
            checkState(node.getRelations().size() >= 2);
            List<RelationType> relationTypes = node.getRelations().stream()
                    .map(relation -> process(relation, scope).getRelationType()).collect(toImmutableList());
            String setOperationName = node.getClass().getSimpleName().toUpperCase(ENGLISH);
            List<Field> outputFields = relationTypes.get(0).getFields();
            for (RelationType relationType : relationTypes) {
                int outputFieldSize = outputFields.size();
                int descFieldSize = relationType.getFields().size();
                if (outputFieldSize != descFieldSize) {
                    throw SemanticExceptions.semanticException(
                            TYPE_MISMATCH,
                            node,
                            "%s query has different number of fields: %d, %d",
                            setOperationName,
                            outputFieldSize,
                            descFieldSize);
                }

                // TODO: check type compatibility
            }

            return createAndAssignScope(node, scope, new RelationType(outputFields));
        }

        @Override
        protected Scope visitJoin(Join node, Optional<Scope> scope)
        {
            Scope leftScope = process(node.getLeft(), scope);
            Scope rightScope = process(node.getRight(), scope);
            RelationType relationType = leftScope.getRelationType().joinWith(rightScope.getRelationType());
            Scope outputScope = createAndAssignScope(node, scope, relationType);

            JoinCriteria criteria = node.getCriteria().orElse(null);
            // TODO: handle other join type
            if (criteria instanceof JoinOn) {
                Expression expression = ((JoinOn) criteria).getExpression();
                analyzeExpression(expression, outputScope);
                typeCoercionOptional.ifPresent(typeCoercion -> {
                    Optional<Expression> coerced = typeCoercion.coerceExpression(expression, outputScope);
                    if (coerced.isPresent()) {
                        JoinOn newJoinOn = new JoinOn(coerced.get());
                        Join newJoin = node.getLocation().isPresent() ?
                                new Join(node.getLocation().get(), node.getType(), node.getLeft(), node.getRight(), Optional.of(newJoinOn)) :
                                new Join(node.getType(), node.getLeft(), node.getRight(), Optional.of(newJoinOn));
                        analysis.addTypeCoercion(NodeRef.of(node), newJoin);
                    }
                });
            }
            // TODO: output scope here isn't right
            return createAndAssignScope(node, scope, relationType);
        }

        @Override
        protected Scope visitAliasedRelation(AliasedRelation relation, Optional<Scope> scope)
        {
            Scope relationScope = process(relation.getRelation(), scope);

            List<Field> fields = relationScope.getRelationType().getFields();
            // if scope is a data source scope, we should get the fields from MDL
            if (relationScope.isDataSourceScope()) {
                CatalogSchemaTableName tableName = toCatalogSchemaTableName(sessionContext, QualifiedName.of(relation.getAlias().getValue()));
                fields = collectFieldFromMDL(tableName);
            }

            List<Field> fieldsWithRelationAlias = fields.stream()
                    .map(field -> Field.builder()
                            .like(field)
                            .relationAlias(QualifiedName.of(relation.getAlias().getValue()))
                            .build())
                    .collect(toImmutableList());

            return createAndAssignScope(relation, scope, new RelationType(fieldsWithRelationAlias));
        }

        @Override
        protected Scope visitTableSubquery(TableSubquery node, Optional<Scope> scope)
        {
            return Optional.ofNullable(analyze(analysis, node.getQuery(), sessionContext, wrenMDL, typeCoercionOptional.orElse(null)))
                    .map(value -> createAndAssignScope(node, scope, value))
                    .orElseGet(() -> Scope.builder().parent(scope).build());
        }

        // TODO: will recursive query mess up anything here?
        private Optional<Scope> analyzeWith(Query node, Optional<Scope> scope)
        {
            if (node.getWith().isEmpty()) {
                return Optional.empty();
            }

            With with = node.getWith().get();
            Scope.Builder withScopeBuilder = Scope.builder().parent(scope);

            for (WithQuery withQuery : with.getQueries()) {
                String name = withQuery.getName().getValue();
                if (withScopeBuilder.containsNamedQuery(name)) {
                    throw new IllegalArgumentException(format("WITH query name '%s' specified more than once", name));
                }
                process(withQuery.getQuery(), withScopeBuilder.build());
                withScopeBuilder.namedQuery(name, withQuery);
            }

            return Optional.of(withScopeBuilder.build());
        }

        private Scope process(Node node, Scope scope)
        {
            return process(node, Optional.of(scope));
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, RelationType relationType)
        {
            Scope newScope = Scope.builder()
                    .parent(parentScope)
                    .relationId(RelationId.of(node))
                    .relationType(relationType)
                    .build();
            analysis.setScope(node, newScope);
            return newScope;
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, Scope scope)
        {
            Scope newScope = Scope.builder()
                    .parent(parentScope)
                    .isDataSourceScope(scope.isDataSourceScope())
                    .relationId(RelationId.of(node))
                    .relationType(scope.getRelationType())
                    .build();
            analysis.setScope(node, newScope);
            return newScope;
        }

        private ExpressionAnalysis analyzeExpression(Expression expression, Scope scope)
        {
            ExpressionAnalysis expressionAnalysis = ExpressionAnalyzer.analyze(scope, expression, sessionContext, wrenMDL, analysis);
            analysis.addCollectedColumns(expressionAnalysis.getCollectedFields());
            analysis.addReferenceFields(expressionAnalysis.getReferencedFields());
            return expressionAnalysis;
        }
    }
}
