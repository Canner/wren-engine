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
import io.accio.base.dto.Relationable;
import io.accio.base.dto.Relationship;
import io.accio.sqlrewrite.analyzer.ExpressionRelationshipAnalyzer;
import io.accio.sqlrewrite.analyzer.ExpressionRelationshipInfo;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static io.accio.base.Utils.checkArgument;
import static io.accio.sqlrewrite.RelationshipRewriter.toDereferenceExpression;
import static io.accio.sqlrewrite.Utils.parseExpression;
import static io.accio.sqlrewrite.Utils.parseQuery;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public abstract class RelationableSqlRender
{
    protected final Relationable relationable;
    protected final AccioMDL mdl;
    protected final String refSql;
    // collect dependent models
    protected final Set<String> requiredObjects;
    // key is alias_name.column_name, value is column name, this map is used to compose select items in model sql
    protected final List<String> selectItems = new ArrayList<>();

    // `requiredRelationshipInfos` collects all join condition and the original column name.
    // It is used to compose join conditions in relationable sql.
    protected final List<ColumnAliasExpressionRelationshipInfo> requiredRelationshipInfos = new ArrayList<>();
    // calculatedRequiredRelationshipInfos collects all join condition needed in model calculated field and the original column name.
    // It is used to compose join conditions in model sql.
    protected final List<CalculatedFieldRelationshipInfo> calculatedRequiredRelationshipInfos = new ArrayList<>();
    // key is column name in model, value is column expression, this map store columns not use relationships
    protected final Map<String, String> columnWithoutRelationships = new LinkedHashMap<>();

    public RelationableSqlRender(Relationable relationable, AccioMDL mdl)
    {
        this.relationable = requireNonNull(relationable);
        this.mdl = requireNonNull(mdl);
        this.refSql = initRefSql(relationable);
        this.requiredObjects = new HashSet<>();
        if (relationable.getBaseObject() != null) {
            requiredObjects.add(relationable.getBaseObject());
        }
    }

    protected abstract String initRefSql(Relationable relationable);

    public abstract RelationInfo render();

    protected RelationInfo render(Model baseModel)
    {
        requireNonNull(baseModel, "baseModel is null");
        relationable.getColumns().stream()
                .filter(column -> column.getRelationship().isEmpty() && column.getExpression().isEmpty())
                .forEach(column -> {
                    selectItems.add(getSelectItemsExpression(column, Optional.empty()));
                    columnWithoutRelationships.put(column.getName(), format("\"%s\".\"%s\"", relationable.getName(), column.getName()));
                });

        relationable.getColumns().stream()
                .filter(column -> column.getRelationship().isEmpty() && column.getExpression().isPresent())
                .forEach(column -> collectRelationship(column, baseModel));
        String modelSubQuerySelectItemsExpression = getModelSubQuerySelectItemsExpression(columnWithoutRelationships);

        String modelSubQuery = format("(SELECT %s FROM (%s) AS \"%s\") AS \"%s\"",
                modelSubQuerySelectItemsExpression,
                refSql,
                baseModel.getName(),
                baseModel.getName());
        Function<String, String> tableJoinCondition =
                (name) -> format("\"%s\".\"%s\" = \"%s\".\"%s\"", baseModel.getName(), baseModel.getPrimaryKey(), name, baseModel.getPrimaryKey());
        StringBuilder tableJoinsSql = new StringBuilder(modelSubQuery);
        if (!requiredRelationshipInfos.isEmpty()) {
            tableJoinsSql.append(format(" LEFT JOIN (%s) AS \"%s\" ON %s",
                    getRelationshipSubQuery(baseModel, requiredRelationshipInfos),
                    getRelationableAlias(baseModel.getName()),
                    tableJoinCondition.apply(getRelationableAlias(baseModel.getName()))));
        }
        if (!calculatedRequiredRelationshipInfos.isEmpty()) {
            for (CalculatedFieldRelationshipInfo calculatedFieldRelationshipInfo : calculatedRequiredRelationshipInfos) {
                tableJoinsSql.append(format(" LEFT JOIN (%s) AS \"%s\" ON %s",
                        getCalculatedSubQuery(baseModel, calculatedFieldRelationshipInfo),
                        calculatedFieldRelationshipInfo.getAlias(),
                        tableJoinCondition.apply(calculatedFieldRelationshipInfo.getAlias())));
            }
        }

        return new RelationInfo(
                relationable,
                requiredObjects,
                parseQuery(getQuerySql(relationable, join(", ", selectItems), tableJoinsSql.toString())));
    }

    protected static String getRelationableAlias(String baseModelName)
    {
        return baseModelName + "_relationsub";
    }

    protected String getCalculatedSubQuery(Model baseModel, CalculatedFieldRelationshipInfo calculatedFieldRelationshipInfo)
    {
        throw new UnsupportedOperationException();
    }

    private String getRelationshipSubQuery(Model baseModel, Collection<ColumnAliasExpressionRelationshipInfo> relationshipInfos)
    {
        String requiredExpressions = relationshipInfos.stream()
                .map(info -> format("%s AS \"%s\"", toDereferenceExpression(info.getExpressionRelationshipInfo()).toString(), info.getAlias()))
                .collect(joining(", "));

        String tableJoins = format("(%s) AS \"%s\" %s",
                getSqlForBaseModelKey(baseModel, relationshipInfos.stream()
                        .map(ColumnAliasExpressionRelationshipInfo::getExpressionRelationshipInfo)
                        .map(ExpressionRelationshipInfo::getBaseModelRelationship)
                        .collect(toList()), mdl),
                baseModel.getName(),
                relationshipInfos.stream()
                        .map(ColumnAliasExpressionRelationshipInfo::getExpressionRelationshipInfo)
                        .map(ExpressionRelationshipInfo::getRelationships)
                        .flatMap(List::stream)
                        .distinct()
                        .map(relationship -> format(" LEFT JOIN \"%s\" ON %s", relationship.getModels().get(1), relationship.getCondition()))
                        .collect(joining()));

        checkArgument(baseModel.getPrimaryKey() != null, format("primary key in model %s contains relationship shouldn't be null", baseModel.getName()));
        return format("SELECT \"%s\".\"%s\", %s FROM (%s)",
                baseModel.getName(),
                baseModel.getPrimaryKey(),
                requiredExpressions,
                tableJoins);
    }

    protected abstract void collectRelationship(Column column, Model baseModel);

    protected abstract String getQuerySql(Relationable relationable, String selectItemsSql, String tableJoinsSql);

    protected abstract String getModelSubQuerySelectItemsExpression(Map<String, String> columnWithoutRelationships);

    protected abstract String getSelectItemsExpression(Column column, Optional<String> relationalBase);

    /**
     * Get sql for base model key (primary key and join key), this sql will be used to join with other models.
     * To avoid cycle reference for base model, we must use subquery to get base model key.
     */
    protected String getSqlForBaseModelKey(Model model, List<Relationship> relationships, AccioMDL mdl)
    {
        Column primaryKey = model.getColumns().stream()
                .filter(column -> column.getName().equals(model.getPrimaryKey()))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("primary key not found in model " + model.getName()));
        // TODO: this should be checked in validator too
        primaryKey.getExpression().ifPresent(expression ->
                checkArgument(ExpressionRelationshipAnalyzer.getToOneRelationships(parseExpression(expression), mdl, model).isEmpty(),
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
                            checkArgument(ExpressionRelationshipAnalyzer.getToOneRelationships(parseExpression(expression), mdl, model).isEmpty(),
                                    format("found relation in relation join condition in %s.%s", model.getName(), joinColumn.getName())));
                    return getModelExpression(joinColumn);
                })
                .distinct()
                .collect(joining(","));

        String primaryKeyExpression = getModelExpression(primaryKey);
        return format("SELECT %s FROM (%s) AS \"%s\"",
                Objects.equals(primaryKeyExpression, joinKeys) ? primaryKeyExpression : primaryKeyExpression + ", " + joinKeys,
                refSql,
                model.getName());
    }

    /**
     * If this relation build for metric, use the name of column.
     * otherwise use the expression of Column if existed.
     */
    protected abstract String getModelExpression(Column column);

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

    public static class ColumnAliasExpressionRelationshipInfo
    {
        private final String alias;
        private final ExpressionRelationshipInfo expressionRelationshipInfo;

        public ColumnAliasExpressionRelationshipInfo(String alias, ExpressionRelationshipInfo expressionRelationshipInfo)
        {
            this.alias = alias;
            this.expressionRelationshipInfo = expressionRelationshipInfo;
        }

        public String getAlias()
        {
            return alias;
        }

        public ExpressionRelationshipInfo getExpressionRelationshipInfo()
        {
            return expressionRelationshipInfo;
        }
    }

    public static class CalculatedFieldRelationshipInfo
    {
        private final Column column;
        private final List<ExpressionRelationshipInfo> expressionRelationshipInfo;

        public CalculatedFieldRelationshipInfo(Column column, List<ExpressionRelationshipInfo> expressionRelationshipInfo)
        {
            this.column = requireNonNull(column);
            this.expressionRelationshipInfo = requireNonNull(expressionRelationshipInfo);
        }

        public String getAlias()
        {
            return column.getName();
        }

        public Column getColumn()
        {
            return column;
        }

        public List<ExpressionRelationshipInfo> getExpressionRelationshipInfo()
        {
            return expressionRelationshipInfo;
        }
    }
}
