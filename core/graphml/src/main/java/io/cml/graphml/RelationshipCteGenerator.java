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

package io.cml.graphml;

import com.google.common.collect.ImmutableSet;
import io.cml.graphml.base.GraphML;
import io.cml.graphml.base.dto.Column;
import io.cml.graphml.base.dto.Model;
import io.cml.graphml.base.dto.Relationship;
import io.trino.sql.QueryUtil;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.WithQuery;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.cml.graphml.Utils.randomTableSuffix;
import static io.trino.sql.QueryUtil.aliased;
import static io.trino.sql.QueryUtil.equal;
import static io.trino.sql.QueryUtil.getConditionNode;
import static io.trino.sql.QueryUtil.identifier;
import static io.trino.sql.QueryUtil.joinOn;
import static io.trino.sql.QueryUtil.leftJoin;
import static io.trino.sql.QueryUtil.nameReference;
import static io.trino.sql.QueryUtil.selectList;
import static io.trino.sql.QueryUtil.simpleQuery;
import static io.trino.sql.QueryUtil.table;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class RelationshipCteGenerator
{
    private final GraphML graphML;
    private final Map<String, WithQuery> registeredCte = new LinkedHashMap<>();
    private final Map<String, String> nameMapping = new HashMap<>();

    public RelationshipCteGenerator(GraphML graphML)
    {
        this.graphML = requireNonNull(graphML);
    }

    public void register(String name, List<RsItem> rsItems)
    {
        RelationshipCTE relationshipCTE = createRelationshipCTE(rsItems);
        WithQuery withQuery = transferToCte(relationshipCTE);
        registeredCte.put(name, withQuery);
        nameMapping.put(name, withQuery.getName().getValue());
    }

    private WithQuery transferToCte(RelationshipCTE relationshipCTE)
    {
        List<Expression> selectItems =
                ImmutableSet.<String>builder()
                        .addAll(relationshipCTE.getRight().getColumns())
                        .add(relationshipCTE.getRight().getJoinKey())
                        .build()
                        .stream()
                        .map(column -> nameReference("r", column))
                        .collect(toList());
        List<Identifier> outputSchema = selectItems.stream().map(this::getReferenceField).map(QueryUtil::identifier).collect(toList());

        return new WithQuery(identifier(relationshipCTE.getName()),
                simpleQuery(
                        selectList(selectItems),
                        leftJoin(aliased(table(QualifiedName.of(relationshipCTE.getLeft().getName())), "l"),
                                aliased(table(QualifiedName.of(relationshipCTE.getRight().getName())), "r"),
                                joinOn(equal(nameReference("l", relationshipCTE.getLeft().getJoinKey()), nameReference("r", relationshipCTE.getRight().getJoinKey()))))),
                Optional.of(outputSchema));
    }

    private RelationshipCTE createRelationshipCTE(List<RsItem> rsItems)
    {
        RelationshipCTE.Relation left;
        RelationshipCTE.Relation right;
        Relationship relationship;
        // If the first item is CTE, the second one is RS or REVERSE_RS.
        if (rsItems.get(0).getType().equals(RsItem.Type.CTE)) {
            relationship = graphML.listRelationships().stream().filter(r -> r.getName().equals(rsItems.get(1).getName())).findAny().get();
            ComparisonExpression comparisonExpression = getConditionNode(relationship.getCondition());
            WithQuery leftQuery = registeredCte.get(rsItems.get(0).getName());

            if (rsItems.get(1).getType() == RsItem.Type.RS) {
                left = new RelationshipCTE.Relation(
                        leftQuery.getName().getValue(),
                        leftQuery.getColumnNames().get().stream().map(Identifier::getValue).collect(toList()),
                        null,
                        getReferenceField(comparisonExpression.getLeft()));
                Model rightModel = graphML.listModels().stream().filter(model -> model.getName().equals(
                        relationship.getModels().get(1))).findAny().get();
                right = new RelationshipCTE.Relation(
                        rightModel.getName(),
                        rightModel.getColumns().stream().map(Column::getName).collect(toList()),
                        rightModel.getPrimaryKey(),
                        getReferenceField(comparisonExpression.getRight()));
            }
            else {
                left = new RelationshipCTE.Relation(
                        leftQuery.getName().getValue(),
                        leftQuery.getColumnNames().get().stream().map(Identifier::getValue).collect(toList()),
                        null,
                        // If it's a REVERSE relationship, the left and right side will be swapped.
                        getReferenceField(comparisonExpression.getRight()));
                Model rightModel = graphML.listModels().stream().filter(model -> model.getName().equals(
                        // If it's a REVERSE relationship, the left and right side will be swapped.
                        relationship.getModels().get(0))).findAny().get();
                right = new RelationshipCTE.Relation(
                        rightModel.getName(),
                        rightModel.getColumns().stream().map(Column::getName).collect(toList()),
                        rightModel.getPrimaryKey(),
                        // If it's a REVERSE relationship, the left and right side will be swapped.
                        getReferenceField(comparisonExpression.getLeft()));
            }
        }
        else {
            relationship = graphML.listRelationships().stream().filter(r -> r.getName().equals(rsItems.get(0).getName())).findAny().get();
            ComparisonExpression comparisonExpression = getConditionNode(relationship.getCondition());
            Model leftModel = graphML.listModels().stream().filter(model -> model.getName().equals(relationship.getModels().get(0))).findAny().get();
            left = new RelationshipCTE.Relation(
                    leftModel.getName(),
                    leftModel.getColumns().stream().map(Column::getName).collect(toList()),
                    leftModel.getPrimaryKey(), getReferenceField(comparisonExpression.getLeft()));

            Model rightModel = graphML.listModels().stream().filter(model -> model.getName().equals(relationship.getModels().get(1))).findAny().get();
            right = new RelationshipCTE.Relation(
                    rightModel.getName(),
                    rightModel.getColumns().stream().map(Column::getName).collect(toList()),
                    rightModel.getPrimaryKey(), getReferenceField(comparisonExpression.getRight()));
        }
        return new RelationshipCTE("rs_" + randomTableSuffix(), left, right, relationship.getCondition());
    }

    private String getReferenceField(Expression expression)
    {
        if (expression instanceof DereferenceExpression) {
            return ((DereferenceExpression) expression).getField().getValue();
        }

        return expression.toString();
    }

    public Map<String, WithQuery> getRegisteredCte()
    {
        return registeredCte;
    }

    public Map<String, String> getNameMapping()
    {
        return nameMapping;
    }

    public static class RsItem
    {
        public static RsItem rsItem(String name, Type type)
        {
            return new RsItem(name, type);
        }

        public enum Type
        {
            CTE,
            RS,
            REVERSE_RS
        }

        private final String name;
        private final Type type;

        private RsItem(String name, Type type)
        {
            this.name = name;
            this.type = type;
        }

        public String getName()
        {
            return name;
        }

        public Type getType()
        {
            return type;
        }
    }
}
