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

package io.accio.sqlrewrite;

import io.accio.base.AccioMDL;
import io.accio.base.dto.Column;
import io.accio.base.dto.Model;
import io.accio.base.dto.Relationship;
import io.accio.sqlrewrite.analyzer.ExpressionRelationshipAnalyzer;
import io.accio.sqlrewrite.analyzer.ExpressionRelationshipInfo;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Query;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.accio.base.Utils.checkArgument;
import static io.accio.sqlrewrite.Utils.parseExpression;
import static io.accio.sqlrewrite.Utils.parseQuery;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class ModelInfo
        implements QueryDescriptor
{
    private final Model model;
    private final Set<String> requiredModels;
    private final Query query;

    public static ModelInfo get(Model model, AccioMDL mdl)
    {
        return new Analyzer(model, mdl).getModelSql();
    }

    private ModelInfo(
            Model model,
            Set<String> requiredModels,
            Query query)
    {
        this.model = requireNonNull(model);
        this.requiredModels = requireNonNull(requiredModels);
        this.query = requireNonNull(query);
    }

    @Override
    public String getName()
    {
        return model.getName();
    }

    @Override
    public Set<String> getRequiredObjects()
    {
        return requiredModels;
    }

    @Override
    public Query getQuery()
    {
        return query;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ModelInfo modelInfo = (ModelInfo) o;
        return Objects.equals(model, modelInfo.model)
                && Objects.equals(requiredModels, modelInfo.requiredModels)
                && Objects.equals(query, modelInfo.query);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(model, requiredModels, query);
    }

    private static class Analyzer
    {
        private final Model model;
        private final AccioMDL mdl;
        private final String refSql;
        // collect dependent models
        private final Set<String> requiredModels;

        public Analyzer(Model model, AccioMDL mdl)
        {
            this.model = requireNonNull(model);
            this.mdl = requireNonNull(mdl);
            if (model.getRefSql() != null) {
                this.refSql = model.getRefSql();
            }
            else if (model.getBaseModel() != null) {
                this.refSql = "SELECT * FROM " + model.getBaseModel();
            }
            else {
                throw new IllegalArgumentException("cannot get reference sql from model");
            }

            this.requiredModels = new HashSet<>();
            if (model.getBaseModel() != null) {
                requiredModels.add(model.getBaseModel());
            }
        }

        public ModelInfo getModelSql()
        {
            requireNonNull(model, "model is null");
            if (model.getColumns().isEmpty()) {
                return new ModelInfo(model, Set.of(), parseQuery(refSql));
            }

            // key is alias_name.column_name, value is column name, this map is used to compose select items in model sql
            Map<String, String> selectItems = new LinkedHashMap<>();
            // key is alias name, value is query contains join condition, this map is used to compose join conditions in model sql
            Map<String, String> tableJoinSqls = new LinkedHashMap<>();
            // key is column name in model, value is column expression, this map store columns not use relationships
            Map<String, String> columnWithoutRelationships = new LinkedHashMap<>();
            model.getColumns().stream()
                    .filter(column -> column.getRelationship().isEmpty())
                    .forEach(column -> {
                        if (column.getExpression().isPresent()) {
                            Expression expression = parseExpression(column.getExpression().get());
                            List<ExpressionRelationshipInfo> relationshipInfos = ExpressionRelationshipAnalyzer.getRelationships(expression, mdl, model);
                            if (!relationshipInfos.isEmpty()) {
                                Expression newExpression = (Expression) RelationshipRewriter.rewrite(relationshipInfos, expression);
                                String tableJoins = format("(%s) AS \"%s\" %s",
                                        getSubquerySql(model, relationshipInfos.stream().map(ExpressionRelationshipInfo::getBaseModelRelationship).collect(toList()), mdl),
                                        model.getName(),
                                        relationshipInfos.stream()
                                                .map(ExpressionRelationshipInfo::getRelationships)
                                                .flatMap(List::stream)
                                                .distinct()
                                                .map(relationship -> format(" LEFT JOIN \"%s\" ON %s", relationship.getModels().get(1), relationship.getCondition()))
                                                .collect(Collectors.joining()));

                                checkArgument(model.getPrimaryKey() != null, format("primary key in model %s contains relationship shouldn't be null", model.getName()));

                                tableJoinSqls.put(
                                        column.getName(),
                                        format("SELECT \"%s\".\"%s\", %s AS \"%s\" FROM (%s)",
                                                model.getName(),
                                                model.getPrimaryKey(),
                                                newExpression,
                                                column.getName(),
                                                tableJoins));
                                // collect all required models in relationships
                                requiredModels.addAll(
                                        relationshipInfos.stream()
                                                .map(ExpressionRelationshipInfo::getRelationships)
                                                .flatMap(List::stream)
                                                .map(Relationship::getModels)
                                                .flatMap(List::stream)
                                                .filter(modelName -> !modelName.equals(model.getName()))
                                                .collect(toSet()));

                                // output from column use relationship will use another subquery which use column name from model as alias name
                                selectItems.put(column.getName(), format("\"%s\".\"%s\"", column.getName(), column.getName()));
                            }
                            else {
                                selectItems.put(column.getName(), format("\"%s\".\"%s\"", model.getName(), column.getName()));
                                columnWithoutRelationships.put(column.getName(), column.getExpression().get());
                            }
                        }
                        else {
                            selectItems.put(column.getName(), format("\"%s\".\"%s\"", model.getName(), column.getName()));
                            columnWithoutRelationships.put(column.getName(), format("\"%s\".\"%s\"", model.getName(), column.getName()));
                        }
                    });

            String modelSubQuery = format("(SELECT %s FROM (%s) AS \"%s\") AS \"%s\"",
                    columnWithoutRelationships.entrySet().stream()
                            .map(e -> format("%s AS \"%s\"", e.getValue(), e.getKey()))
                            .collect(joining(", ")),
                    refSql,
                    model.getName(),
                    model.getName());
            Function<String, String> tableJoinCondition = (name) -> format("\"%s\".\"%s\" = \"%s\".\"%s\"", model.getName(), model.getPrimaryKey(), name, model.getPrimaryKey());
            String tableJoinsSql = modelSubQuery +
                    tableJoinSqls.entrySet().stream()
                            .map(e -> format(" LEFT JOIN (%s) AS \"%s\" ON %s", e.getValue(), e.getKey(), tableJoinCondition.apply(e.getKey())))
                            .collect(joining());

            String selectItemsSql = selectItems.entrySet().stream()
                    .map(e -> format("%s AS \"%s\"", e.getValue(), e.getKey()))
                    .collect(joining(", "));

            return new ModelInfo(
                    model,
                    requiredModels,
                    parseQuery(format("SELECT %s FROM %s", selectItemsSql, tableJoinsSql)));
        }

        private String getSubquerySql(Model model, List<Relationship> relationships, AccioMDL mdl)
        {
            Column primaryKey = model.getColumns().stream()
                    .filter(column -> column.getName().equals(model.getPrimaryKey()))
                    .findAny()
                    .orElseThrow(() -> new IllegalArgumentException("primary key not found in model " + model.getName()));
            // TODO: this should be checked in validator too
            primaryKey.getExpression().ifPresent(expression ->
                    checkArgument(ExpressionRelationshipAnalyzer.getRelationships(parseExpression(expression), mdl, model).isEmpty(),
                            format("found relation in model %s primary key expression", model.getName())));

            String joinKeys = relationships.stream()
                    .map(relationship -> {
                        String joinColumnName = findJoinColumn(model, relationship);
                        Column joinColumn = model.getColumns().stream()
                                .filter(column -> column.getName().equals(joinColumnName))
                                .findAny()
                                .orElseThrow(() -> new IllegalArgumentException(format("join column %s not found in model %s", joinColumnName, model.getName())));
                        // TODO: this should be checked in validator too
                        joinColumn.getExpression().ifPresent(expression ->
                                checkArgument(ExpressionRelationshipAnalyzer.getRelationships(parseExpression(expression), mdl, model).isEmpty(),
                                        format("found relation in relation join condition in %s.%s", model.getName(), joinColumn.getName())));
                        return format("%s AS \"%s\"", joinColumn.getExpression().orElse(joinColumn.getName()), joinColumn.getName());
                    })
                    .collect(joining(","));

            return format("SELECT %s, %s FROM (%s) AS \"%s\"",
                    format("%s AS \"%s\"", primaryKey.getExpression().orElse(primaryKey.getName()), primaryKey.getName()),
                    joinKeys,
                    refSql,
                    model.getName());
        }

        private static String findJoinColumn(Model model, Relationship relationship)
        {
            checkArgument(relationship.getModels().contains(model.getName()), format("model %s not found in relationship %s", model.getName(), relationship.getName()));
            ComparisonExpression joinCondition = (ComparisonExpression) parseExpression(relationship.getCondition());
            checkArgument(joinCondition.getLeft() instanceof DereferenceExpression, "invalid join condition");
            checkArgument(joinCondition.getRight() instanceof DereferenceExpression, "invalid join condition");
            DereferenceExpression left = (DereferenceExpression) joinCondition.getLeft();
            DereferenceExpression right = (DereferenceExpression) joinCondition.getRight();
            if (left.getBase().toString().equals(model.getName())) {
                return left.getField().orElseThrow().getValue();
            }
            if (right.getBase().toString().equals(model.getName())) {
                return right.getField().orElseThrow().getValue();
            }
            throw new IllegalArgumentException(format("join column in relationship %s not found in model %s", relationship.getName(), model.getName()));
        }
    }
}
