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

package io.accio.sqlrewrite.analyzer;

import io.accio.base.AccioMDL;
import io.accio.base.CatalogSchemaTableName;
import io.accio.base.SessionContext;
import io.accio.base.dto.CumulativeMetric;
import io.accio.base.dto.Metric;
import io.accio.base.dto.Model;
import io.accio.base.dto.View;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionRelation;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.Union;
import io.trino.sql.tree.Unnest;
import io.trino.sql.tree.Values;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.accio.base.Utils.checkArgument;
import static io.accio.base.dto.TimeUnit.timeUnit;
import static io.accio.sqlrewrite.Utils.toCatalogSchemaTableName;
import static io.trino.sql.QueryUtil.getQualifiedName;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableSet;

/**
 * Inspired by io.trino.sql.analyzer.StatementAnalyzer
 */
public final class StatementAnalyzer
{
    private StatementAnalyzer() {}

    public static Analysis analyze(Statement statement, SessionContext sessionContext, AccioMDL accioMDL)
    {
        Analysis analysis = new Analysis(statement);
        new Visitor(sessionContext, analysis, accioMDL).process(statement, Optional.empty());

        // add models directly used in sql query
        analysis.addModels(
                accioMDL.listModels().stream()
                        .filter(model -> analysis.getTables().stream()
                                .filter(table -> table.getCatalogName().equals(accioMDL.getCatalog()))
                                .filter(table -> table.getSchemaTableName().getSchemaName().equals(accioMDL.getSchema()))
                                .anyMatch(table -> table.getSchemaTableName().getTableName().equals(model.getName())))
                        .collect(toUnmodifiableSet()));

        Set<Metric> metrics = analysis.getTables().stream()
                .map(accioMDL::getMetric)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toUnmodifiableSet());

        Set<Metric> metricInMetricRollups = analysis.getMetricRollups().values().stream()
                .map(MetricRollupInfo::getMetric)
                .collect(toUnmodifiableSet());

        // TODO: remove this check
        checkArgument(metrics.stream().noneMatch(metricInMetricRollups::contains), "duplicate metrics in metrics and metric rollups");
        analysis.addMetrics(metrics);

        Set<CumulativeMetric> cumulativeMetrics = analysis.getTables().stream()
                .map(accioMDL::getCumulativeMetric)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toUnmodifiableSet());
        analysis.addCumulativeMetrics(cumulativeMetrics);

        Set<View> views = analysis.getTables().stream()
                .map(accioMDL::getView)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toUnmodifiableSet());

        analysis.addViews(views);

        return analysis;
    }

    private static class Visitor
            extends AstVisitor<Scope, Optional<Scope>>
    {
        private final SessionContext sessionContext;
        private final Analysis analysis;
        private final AccioMDL accioMDL;

        public Visitor(SessionContext sessionContext, Analysis analysis, AccioMDL accioMDL)
        {
            this.sessionContext = requireNonNull(sessionContext, "sessionContext is null");
            this.analysis = requireNonNull(analysis, "analysis is null");
            this.accioMDL = requireNonNull(accioMDL, "accioMDL is null");
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
                    return Scope.builder()
                            .parent(scope)
                            .relationType(new RelationType(List.of()))
                            .isTableScope(true)
                            .build();
                }
            }

            CatalogSchemaTableName tableName = toCatalogSchemaTableName(sessionContext, node.getName());
            analysis.addTable(tableName);
            // only record model fields here, others are ignored
            List<Field> modelFields = List.of();
            if (tableName.getCatalogName().equals(accioMDL.getCatalog()) && tableName.getSchemaTableName().getSchemaName().equals(accioMDL.getSchema())) {
                analysis.addModelNodeRef(NodeRef.of(node));
                modelFields = accioMDL.getModel(tableName.getSchemaTableName().getTableName())
                        .map(Model::getColumns)
                        .orElseGet(List::of)
                        .stream()
                        .map(column ->
                                Field.builder()
                                        .modelName(tableName)
                                        .columnName(column.getName())
                                        .name(column.getName())
                                        .relationship(column.getRelationship().flatMap(accioMDL::getRelationship))
                                        .type(column.getType())
                                        .build())
                        .collect(toImmutableList());
            }
            return Scope.builder()
                    .parent(scope)
                    .relationType(new RelationType(modelFields))
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
                    .relationType(queryBodyScope.getRelationType().orElse(null))
                    .build();

            return queryScope;
        }

        @Override
        protected Scope visitQuerySpecification(QuerySpecification node, Optional<Scope> scope)
        {
            if (node.getFrom().isPresent()) {
                return process(node.getFrom().get(), scope);
            }
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
        protected Scope visitFunctionRelation(FunctionRelation node, Optional<Scope> scope)
        {
            if (node.getName().toString().equalsIgnoreCase("roll_up")) {
                List<Expression> arguments = node.getArguments();
                checkArgument(arguments.size() == 3, "rollup function should have 3 arguments");

                QualifiedName tableName = getQualifiedName(arguments.get(0));
                checkArgument(tableName != null, format("'%s' cannot be resolved", arguments.get(0)));
                checkArgument(arguments.get(1) instanceof Identifier, format("'%s' cannot be resolved", arguments.get(1)));
                checkArgument(arguments.get(2) instanceof Identifier, format("'%s' cannot be resolved", arguments.get(2)));

                CatalogSchemaTableName catalogSchemaTableName = toCatalogSchemaTableName(sessionContext, tableName);
                Metric metric = accioMDL.getMetric(catalogSchemaTableName).orElseThrow(() -> new IllegalArgumentException("Metric not found: " + catalogSchemaTableName));
                String timeColumn = ((Identifier) arguments.get(1)).getValue();

                analysis.addMetricRollups(
                        NodeRef.of(node),
                        new MetricRollupInfo(
                                metric,
                                metric.getTimeGrain(timeColumn).orElseThrow(() -> new IllegalArgumentException("Time column not found in metric: " + timeColumn)),
                                timeUnit(((Identifier) arguments.get(2)).getValue())));
                // currently we don't care about metric rollup output scope
                return Scope.builder().parent(scope).build();
            }
            throw new IllegalArgumentException("FunctionRelation not supported: " + node.getName());
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
            Scope relationScope = process(relation.getRelation(), scope);

            if (!relationScope.isTableScope()) {
                return relationScope;
            }

            checkArgument(relationScope.getRelationType().isPresent(), "relationType is missing");
            List<Field> fieldsWithRelationAlias = relationScope.getRelationType().get().getFields().stream()
                    .map(field -> Field.builder().like(field)
                            .relationAlias(QualifiedName.of(relation.getAlias().getValue())).build())
                    .collect(toImmutableList());

            return Scope.builder()
                    .parent(scope)
                    .relationType(new RelationType(fieldsWithRelationAlias))
                    .isTableScope(true)
                    .build();
        }

        @Override
        protected Scope visitTableSubquery(TableSubquery node, Optional<Scope> scope)
        {
            process(node.getQuery());
            // TODO: output scope here isn't right
            return Scope.builder().parent(scope).build();
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
    }
}
