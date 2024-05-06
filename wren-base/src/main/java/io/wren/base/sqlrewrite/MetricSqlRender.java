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

package io.wren.base.sqlrewrite;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.Identifier;
import io.wren.base.WrenMDL;
import io.wren.base.dto.Column;
import io.wren.base.dto.CumulativeMetric;
import io.wren.base.dto.Metric;
import io.wren.base.dto.Model;
import io.wren.base.dto.Relationable;
import io.wren.base.dto.Relationship;
import io.wren.base.sqlrewrite.analyzer.ExpressionRelationshipAnalyzer;
import io.wren.base.sqlrewrite.analyzer.ExpressionRelationshipInfo;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.SqlFormatter.formatSql;
import static io.wren.base.sqlrewrite.Utils.parseExpression;
import static io.wren.base.sqlrewrite.Utils.parseQuery;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class MetricSqlRender
        extends RelationableSqlRender
{
    private final Set<String> requiredDims;
    private final Set<String> requiredMeasures;

    public MetricSqlRender(Metric metric, WrenMDL mdl)
    {
        super(metric, mdl);
        this.requiredDims = metric.getDimension().stream().map(Column::getName).collect(toImmutableSet());
        this.requiredMeasures = metric.getMeasure().stream().map(Column::getName).collect(toImmutableSet());
    }

    public MetricSqlRender(Metric metric, WrenMDL mdl, Set<String> requiredFields)
    {
        super(metric, mdl);
        requireNonNull(requiredFields);
        this.requiredDims = metric.getDimension().stream()
                .map(Column::getName)
                .filter(requiredFields::contains)
                .collect(toImmutableSet());
        this.requiredMeasures = metric.getMeasure().stream()
                .map(Column::getName)
                .filter(requiredFields::contains)
                .collect(toImmutableSet());
    }

    @Override
    protected String initRefSql(Relationable relationable)
    {
        return "SELECT * FROM \"" + relationable.getBaseObject() + "\"";
    }

    @Override
    public RelationInfo render()
    {
        Optional<Model> metricBaseModel = mdl.getModel(relationable.getBaseObject());
        // metric on model
        if (metricBaseModel.isPresent()) {
            return render(metricBaseModel.orElseThrow(() -> new IllegalArgumentException("model not found")));
        }
        // metric on metric
        Optional<Metric> metricBaseMetric = mdl.getMetric(relationable.getBaseObject());
        if (metricBaseMetric.isPresent()) {
            return renderBasedOnMetric(metricBaseMetric.orElseThrow(() -> new IllegalArgumentException("metric not found")).getName());
        }
        // metric on cumulative metric
        Optional<CumulativeMetric> metricBaseCumulativeMetric = mdl.getCumulativeMetric(relationable.getBaseObject());
        if (metricBaseCumulativeMetric.isPresent()) {
            return renderBasedOnMetric(metricBaseCumulativeMetric.orElseThrow(() -> new IllegalArgumentException("metric not found")).getName());
        }
        throw new IllegalArgumentException("invalid metric, cannot render metric sql");
    }

    private RelationInfo renderBasedOnMetric(String metricName)
    {
        Metric metric = (Metric) relationable;
        List<String> selectItems = metric.getColumns().stream()
                .filter(column -> isRequiredColumn(column.getName()))
                .map(column -> format("%s AS \"%s\"", column.getSqlExpression(), column.getName()))
                .collect(toList());
        addCountAllIfNeeded();
        String sql = getQuerySql(Joiner.on(", ").join(selectItems), metricName);
        return new RelationInfo(
                relationable,
                Set.of(metricName),
                parseQuery(sql));
    }

    @Override
    protected String getQuerySql(String selectItemsSql, String tableJoinsSql)
    {
        if (requiredDims.isEmpty()) {
            return format("SELECT %s FROM %s", selectItemsSql, tableJoinsSql);
        }
        String groupByItems = IntStream.rangeClosed(1, requiredDims.size()).mapToObj(String::valueOf).collect(joining(","));
        return format("SELECT %s FROM %s GROUP BY %s", selectItemsSql, tableJoinsSql, groupByItems);
    }

    @Override
    protected String getModelSubQuerySelectItemsExpression(Map<String, String> columnWithoutRelationships)
    {
        // TODO: consider column projection
        return "*";
    }

    @Override
    protected String getSelectItemsExpression(Column column, Optional<String> relationableBase)
    {
        Metric metric = (Metric) relationable;
        boolean isMeasure = metric.getMeasure().stream().anyMatch(measure -> measure.getName().equals(column.getName()));
        Model baseModel = mdl.getModel(metric.getBaseObject()).orElseThrow(() -> new IllegalArgumentException(format("cannot find model %s", metric.getBaseObject())));
        Expression expression = parseExpression(column.getSqlExpression());
        Set<ExpressionRelationshipInfo> relationshipInfos = ExpressionRelationshipAnalyzer.getRelationships(expression, mdl, baseModel).stream().collect(toImmutableSet());

        if (!relationshipInfos.isEmpty() && relationableBase.isPresent()) {
            Expression newExpression = (Expression) RelationshipRewriter.relationshipAware(relationshipInfos, relationableBase.get(), expression);
            return format("%s AS \"%s\"", newExpression, column.getName());
        }

        if (isMeasure) {
            return format("%s AS \"%s\"",
                    awareModel(column.getExpression().orElseThrow(() -> new IllegalArgumentException("measure column must have expression")), baseModel), column.getName());
        }

        return format("%s AS \"%s\"", awareModel(column.getSqlExpression(), baseModel), column.getName());
    }

    private String awareModel(String expression, Model baseModel)
    {
        return formatSql(awareModel(parseExpression(expression), baseModel));
    }

    private Expression awareModel(Expression expression, Model baseModel)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<>()
        {
            @Override
            public Expression rewriteIdentifier(Identifier node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                if (baseModel.getColumns().stream().anyMatch(c -> c.getName().equalsIgnoreCase(node.getValue()))) {
                    return new DereferenceExpression(new Identifier(baseModel.getName(), true), new Identifier(node.getValue(), true));
                }
                return node;
            }
        }, expression);
    }

    @Override
    protected void collectRelationship(Column column, Model baseModel)
    {
        if (!isRequiredColumn(column.getName())) {
            return;
        }
        Expression expression = parseExpression(column.getSqlExpression());
        Set<ExpressionRelationshipInfo> relationshipInfos = ExpressionRelationshipAnalyzer.getRelationships(expression, mdl, baseModel);
        if (!relationshipInfos.isEmpty()) {
            calculatedRequiredRelationshipInfos.add(new CalculatedFieldRelationshipInfo(column, relationshipInfos));
            // collect all required models in relationships
            requiredObjects.addAll(
                    relationshipInfos.stream()
                            .map(ExpressionRelationshipInfo::getRelationships)
                            .flatMap(List::stream)
                            .map(Relationship::getModels)
                            .flatMap(List::stream)
                            .filter(modelName -> !modelName.equals(baseModel.getName()))
                            .collect(toSet()));

            // output from column use relationship will use another subquery which use column name from model as alias name
            selectItems.add(getSelectItemsExpression(column, Optional.of(getRelationableAlias(baseModel.getName()))));
        }
        else {
            selectItems.add(getSelectItemsExpression(column, Optional.empty()));
            calculatedScopeSelectItems.put(column.getName(), column.getSqlExpression());
        }
    }

    @Override
    protected List<SubQueryJoinInfo> getCalculatedSubQuery(Model baseModel, List<CalculatedFieldRelationshipInfo> relationshipInfos)
    {
        if (relationshipInfos.isEmpty()) {
            return ImmutableList.of();
        }

        String requiredExpressions = relationshipInfos.stream()
                .map(CalculatedFieldRelationshipInfo::getExpressionRelationshipInfo)
                .flatMap(Set::stream)
                .map(RelationshipRewriter::toDereferenceExpression)
                .map(Expression::toString)
                .distinct()
                .collect(joining(", "));

        List<Relationship> requiredRelationships = relationshipInfos.stream()
                .map(CalculatedFieldRelationshipInfo::getExpressionRelationshipInfo)
                .flatMap(Set::stream)
                .map(ExpressionRelationshipInfo::getRelationships)
                .flatMap(List::stream)
                .distinct()
                .collect(toImmutableList());
        String tableJoins = format("\"%s\"\n%s",
                baseModel.getName(),
                requiredRelationships.stream()
                        .map(relationship -> format("LEFT JOIN \"%s\" ON %s\n", relationship.getModels().get(1), relationship.getQualifiedCondition()))
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

    private RelationInfo render(Model baseModel)
    {
        requireNonNull(baseModel, "baseModel is null");
        relationable.getColumns().stream()
                .filter(column -> column.getRelationship().isEmpty() && column.getExpression().isEmpty())
                .filter(column -> isRequiredColumn(column.getName()))
                .forEach(column -> selectItems.add(getSelectItemsExpression(column, Optional.empty())));
        relationable.getColumns().stream()
                .filter(column -> column.getRelationship().isEmpty() && column.getExpression().isPresent())
                .forEach(column -> collectRelationship(column, baseModel));
        addCountAllIfNeeded();

        String modelSubQuerySelectItemsExpression = getModelSubQuerySelectItemsExpression(calculatedScopeSelectItems);

        String modelSubQuery = format("(SELECT %s FROM (%s) AS \"%s\") AS \"%s\"",
                modelSubQuerySelectItemsExpression,
                refSql,
                baseModel.getName(),
                baseModel.getName());

        StringBuilder tableJoinsSql = new StringBuilder(modelSubQuery);
        if (!calculatedRequiredRelationshipInfos.isEmpty()) {
            tableJoinsSql.append(
                    getCalculatedSubQuery(baseModel, calculatedRequiredRelationshipInfos).stream()
                            .map(info -> format("\nLEFT JOIN (%s) AS \"%s\" ON %s", info.getSql(), info.getSubqueryAlias(), info.getJoinCriteria()))
                            .collect(joining("")));
        }
        tableJoinsSql.append("\n");

        return new RelationInfo(
                relationable,
                requiredObjects,
                parseQuery(getQuerySql(join(", ", selectItems), tableJoinsSql.toString())));
    }

    private boolean isRequiredColumn(String name)
    {
        return requiredDims.contains(name) || requiredMeasures.contains(name);
    }

    // dynamic metric may not contain measure, while metric will use group by clause, so we still need at least one aggregate function
    // in metric CTE, that's why added a filler column count(*) here
    private void addCountAllIfNeeded()
    {
        if (requiredMeasures.isEmpty()) {
            selectItems.add("COUNT(*) AS _count_filler");
        }
    }
}
