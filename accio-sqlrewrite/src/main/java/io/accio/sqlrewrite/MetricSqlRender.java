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

import com.google.common.base.Joiner;
import io.accio.base.AccioMDL;
import io.accio.base.dto.Column;
import io.accio.base.dto.CumulativeMetric;
import io.accio.base.dto.Metric;
import io.accio.base.dto.Model;
import io.accio.base.dto.Relationable;
import io.accio.base.dto.Relationship;
import io.accio.sqlrewrite.analyzer.ExpressionRelationshipAnalyzer;
import io.accio.sqlrewrite.analyzer.ExpressionRelationshipInfo;
import io.trino.sql.tree.Expression;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static io.accio.sqlrewrite.Utils.parseExpression;
import static io.accio.sqlrewrite.Utils.parseQuery;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class MetricSqlRender
        extends RelationableSqlRender
{
    public MetricSqlRender(Relationable relationable, AccioMDL mdl)
    {
        super(relationable, mdl);
    }

    @Override
    protected String initRefSql(Relationable relationable)
    {
        return "SELECT * FROM " + relationable.getBaseObject();
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

    // TODO: Refactor this out of MetricSqlRender since Metric currently can't be used in relationship in MDL
    //  thus no need to take care relations in column expression.
    private RelationInfo renderBasedOnMetric(String metricName)
    {
        Metric metric = (Metric) relationable;
        List<String> selectItems = metric.getColumns().stream()
                .filter(column -> column.getRelationship().isEmpty())
                .map(column -> format("%s AS %s", column.getExpression().orElse(column.getName()), column.getName()))
                .collect(toList());
        String sql = getQuerySql(metric, Joiner.on(", ").join(selectItems), metricName);
        return new RelationInfo(
                relationable,
                Set.of(metricName),
                parseQuery(sql));
    }

    @Override
    protected String getQuerySql(Relationable relationable, String selectItemsSql, String tableJoinsSql)
    {
        Metric metric = (Metric) relationable;
        String groupByItems = IntStream.rangeClosed(1, metric.getDimension().size()).mapToObj(String::valueOf).collect(joining(","));
        return format("SELECT %s FROM %s GROUP BY %s", selectItemsSql, tableJoinsSql, groupByItems);
    }

    @Override
    protected String getModelExpression(Column column)
    {
        return column.getName();
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
        Expression expression = parseExpression(column.getExpression().orElse(column.getName()));
        List<ExpressionRelationshipInfo> relationshipInfos = ExpressionRelationshipAnalyzer.getRelationships(expression, mdl, baseModel);

        if (!relationshipInfos.isEmpty() && relationableBase.isPresent()) {
            Expression newExpression = (Expression) RelationshipRewriter.relationshipAware(relationshipInfos, relationableBase.get(), expression);
            return format("%s AS \"%s\"", newExpression, column.getName());
        }

        if (isMeasure) {
            return column.getSqlExpression();
        }

        return format("\"%s\".\"%s\" AS \"%s\"", relationable.getBaseObject(), column.getExpression().orElse(column.getName()), column.getName());
    }

    @Override
    protected void collectRelationship(Column column, Model baseModel)
    {
        // TODO: There're some issue about sql keyword as expression, e.g. column named as "order"
        Expression expression = parseExpression(column.getExpression().get());
        List<ExpressionRelationshipInfo> relationshipInfos = ExpressionRelationshipAnalyzer.getRelationships(expression, mdl, baseModel);
        if (!relationshipInfos.isEmpty()) {
            for (ExpressionRelationshipInfo relationshipInfo : relationshipInfos) {
                requiredRelationshipInfos.add(new ColumnAliasExpressionRelationshipInfo(relationshipInfo.getRemainingParts().get(0), relationshipInfo));
            }
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
            columnWithoutRelationships.put(column.getName(), column.getExpression().get());
        }
    }
}
