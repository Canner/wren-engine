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

import com.google.common.collect.ImmutableList;
import io.accio.base.AccioMDL;
import io.accio.base.dto.Column;
import io.accio.base.dto.Model;
import io.accio.base.dto.Relationable;
import io.accio.base.dto.Relationship;
import io.accio.sqlrewrite.analyzer.ExpressionRelationshipAnalyzer;
import io.accio.sqlrewrite.analyzer.ExpressionRelationshipInfo;
import io.trino.sql.tree.Expression;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.accio.base.Utils.checkArgument;
import static io.accio.sqlrewrite.Utils.parseExpression;
import static io.accio.sqlrewrite.Utils.parseQuery;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

public class ModelSqlRender
        extends RelationableSqlRender
{
    public ModelSqlRender(Relationable relationable, AccioMDL mdl)
    {
        super(relationable, mdl);
    }

    @Override
    protected String initRefSql(Relationable relationable)
    {
        checkArgument(relationable instanceof Model, "relationable must be model");
        Model model = (Model) relationable;
        if (model.getRefSql() != null) {
            return model.getRefSql();
        }
        else if (model.getBaseObject() != null) {
            return "SELECT * FROM " + model.getBaseObject();
        }
        else {
            throw new IllegalArgumentException("cannot get reference sql from model");
        }
    }

    @Override
    public RelationInfo render()
    {
        requireNonNull(relationable, "model is null");
        if (relationable.getColumns().isEmpty()) {
            return new RelationInfo(relationable, Set.of(), parseQuery(refSql));
        }

        return render((Model) relationable);
    }

    @Override
    protected String getQuerySql(Relationable relationable, String selectItemsSql, String tableJoinsSql)
    {
        return format("SELECT %s FROM %s", selectItemsSql, tableJoinsSql);
    }

    @Override
    protected String getModelSubQuerySelectItemsExpression(Map<String, String> columnWithoutRelationships)
    {
        return columnWithoutRelationships.entrySet().stream()
                .map(e -> format("%s AS \"%s\"", e.getValue(), e.getKey()))
                .collect(joining(", "));
    }

    @Override
    protected String getSelectItemsExpression(Column column, Optional<String> relationalBase)
    {
        if (relationalBase.isPresent()) {
            return format("\"%s\".\"%s\" AS \"%s\"", relationalBase.get(), column.getName(), column.getName());
        }
        return format("\"%s\".\"%s\" AS \"%s\"", relationable.getName(), column.getName(), column.getName());
    }

    @Override
    protected void collectRelationship(Column column, Model baseModel)
    {
        Expression expression = parseExpression(column.getExpression().orElseThrow(() -> new IllegalArgumentException("expression not found in column")));
        List<ExpressionRelationshipInfo> relationshipInfos = ExpressionRelationshipAnalyzer.getRelationships(expression, mdl, baseModel);
        if (column.isCalculated() && relationshipInfos.size() > 0) {
            CalculatedFieldRelationshipInfo calculatedFieldRelationshipInfo = new CalculatedFieldRelationshipInfo(column, relationshipInfos);
            calculatedRequiredRelationshipInfos.add(calculatedFieldRelationshipInfo);
            // Collect all required models in relationships
            requiredObjects.addAll(relationshipInfos.stream()
                    .map(ExpressionRelationshipInfo::getRelationships)
                    .flatMap(List::stream)
                    .map(Relationship::getModels)
                    .flatMap(List::stream)
                    .filter(modelName -> !modelName.equals(baseModel.getName()))
                    .collect(toSet()));

            // Add select items based on the type of column
            if (calculatedFieldRelationshipInfo.isAggregated()) {
                selectItems.add(getSelectItemsExpression(column, Optional.of(calculatedFieldRelationshipInfo.getAlias())));
            }
            else {
                selectItems.add(getSelectItemsExpression(column, Optional.of(getRelationableAlias(baseModel.getName()))));
            }
        }
        else {
            // No relationships, add select item with no alias
            selectItems.add(getSelectItemsExpression(column, Optional.empty()));
            columnWithoutRelationships.put(column.getName(), column.getExpression().get());
        }
    }

    @Override
    protected List<SubQueryJoinInfo> getCalculatedSubQuery(Model baseModel, List<CalculatedFieldRelationshipInfo> relationshipInfos)
    {
        ImmutableList.Builder<SubQueryJoinInfo> queries = ImmutableList.builder();
        queries.addAll(getToOneRelationshipsQuery(baseModel, relationshipInfos));
        queries.addAll(getToManyRelationshipsQuery(baseModel, relationshipInfos));
        return queries.build();
    }

    // only accept to-one relationship(s) in this method
    private List<SubQueryJoinInfo> getToOneRelationshipsQuery(Model baseModel, Collection<CalculatedFieldRelationshipInfo> relationshipInfos)
    {
        List<CalculatedFieldRelationshipInfo> toOneRelationships = relationshipInfos.stream()
                .filter(relationshipInfo -> !relationshipInfo.isAggregated())
                .collect(toImmutableList());

        if (toOneRelationships.isEmpty()) {
            return ImmutableList.of();
        }

        String requiredExpressions = toOneRelationships.stream()
                .map(info -> format("%s AS \"%s\"",
                        RelationshipRewriter.rewrite(
                                        info.getExpressionRelationshipInfo(),
                                        parseExpression(info.getColumn().getExpression().orElseThrow()))
                                .toString(),
                        info.getAlias()))
                .collect(joining(", "));

        List<Relationship> requiredRelationships = toOneRelationships.stream()
                .map(CalculatedFieldRelationshipInfo::getExpressionRelationshipInfo)
                .flatMap(List::stream)
                .map(ExpressionRelationshipInfo::getRelationships)
                .flatMap(List::stream)
                .distinct()
                .collect(toImmutableList());
        String tableJoins = format("(%s) AS \"%s\" %s",
                getBaseModelSql(baseModel),
                baseModel.getName(),
                requiredRelationships.stream()
                        .map(relationship -> format(" LEFT JOIN \"%s\" ON %s", relationship.getModels().get(1), relationship.getCondition()))
                        .collect(joining()));

        Function<String, String> tableJoinCondition =
                (name) -> format("\"%s\".\"%s\" = \"%s\".\"%s\"", baseModel.getName(), baseModel.getPrimaryKey(), name, baseModel.getPrimaryKey());
        return ImmutableList.of(
                new SubQueryJoinInfo(
                        format("SELECT \"%s\".\"%s\", %s FROM (%s)",
                                baseModel.getName(),
                                baseModel.getPrimaryKey(),
                                requiredExpressions,
                                tableJoins),
                        getRelationableAlias(baseModel.getName()),
                        tableJoinCondition.apply(getRelationableAlias(baseModel.getName()))));
    }

    // accept to-one relationship(s) and at least one to-many relationship in this method, and use group by model primary key
    // to aggregate the query result as to-many relationship could lead to duplicate rows. Here we didn't check if there
    // is an aggregation function or not, we should add aggregation function in expression to avoid sql syntax error.
    private List<SubQueryJoinInfo> getToManyRelationshipsQuery(Model baseModel, Collection<CalculatedFieldRelationshipInfo> relationshipInfos)
    {
        return relationshipInfos.stream()
                .filter(CalculatedFieldRelationshipInfo::isAggregated)
                .map(relationshipInfo -> {
                    String requiredExpressions = format("%s AS \"%s\"",
                            RelationshipRewriter.rewrite(relationshipInfo.getExpressionRelationshipInfo(), parseExpression(relationshipInfo.getColumn().getExpression().get())),
                            relationshipInfo.getAlias());

                    String tableJoins = format("(%s) AS \"%s\" %s",
                            getBaseModelSql(baseModel),
                            baseModel.getName(),
                            relationshipInfo.getExpressionRelationshipInfo().stream()
                                    .map(ExpressionRelationshipInfo::getRelationships)
                                    .flatMap(List::stream)
                                    .distinct()
                                    .map(relationship -> format(" LEFT JOIN \"%s\" ON %s", relationship.getModels().get(1), relationship.getCondition()))
                                    .collect(joining()));

                    checkArgument(baseModel.getPrimaryKey() != null, format("primary key in model %s contains relationship shouldn't be null", baseModel.getName()));
                    Function<String, String> tableJoinCondition =
                            (name) -> format("\"%s\".\"%s\" = \"%s\".\"%s\"", baseModel.getName(), baseModel.getPrimaryKey(), name, baseModel.getPrimaryKey());
                    return new SubQueryJoinInfo(
                            format("SELECT %s, %s FROM (%s) GROUP BY 1",
                                    format("\"%s\".\"%s\"", baseModel.getName(), baseModel.getPrimaryKey()),
                                    requiredExpressions,
                                    tableJoins),
                            relationshipInfo.getAlias(),
                            tableJoinCondition.apply(relationshipInfo.getAlias()));
                })
                .collect(toImmutableList());
    }

    private String getBaseModelSql(Model model)
    {
        String selectItems = model.getColumns().stream()
                .filter(column -> !column.isCalculated())
                .filter(column -> column.getRelationship().isEmpty())
                .map(Column::getSqlExpression)
                .collect(joining(", "));
        return format("SELECT %s FROM (%s) AS \"%s\"", selectItems, refSql, model.getName());
    }
}
