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
import io.trino.sql.tree.Expression;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
    protected String getModelExpression(Column column)
    {
        return format("%s AS \"%s\"", column.getExpression().orElse(column.getName()), column.getName());
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
        Expression expression = parseExpression(column.getExpression().get());
        List<ExpressionRelationshipInfo> relationshipInfos = ExpressionRelationshipAnalyzer.getToOneRelationships(expression, mdl, baseModel);
        if (!relationshipInfos.isEmpty()) {
            requiredRelationshipInfos.addAll(relationshipInfos.stream()
                    .map(info -> new ColumnAliasExpressionRelationshipInfo(column.getName(), info))
                    .collect(toSet()));
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
