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

package io.graphmdl.graphml;

import com.google.common.collect.ImmutableSet;
import io.graphmdl.graphml.base.GraphML;
import io.graphmdl.graphml.base.dto.Column;
import io.graphmdl.graphml.base.dto.Model;
import io.graphmdl.graphml.base.dto.Relationship;
import io.trino.sql.QueryUtil;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.JoinCriteria;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.WithQuery;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.graphmdl.graphml.RelationshipCteGenerator.RsItem.Type.RS;
import static io.graphmdl.graphml.Utils.randomTableSuffix;
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

/**
 * <p>This class will generate a registered relationship accessing to be a CTE SQL. e.g.,
 * If given a model, `Book`, with a relationship column, `author`, related to the model, `User`,
 * when a sql try to access the field fo a relationship:</p>
 * <p>SELECT Book.author.name FROM Book</p>
 * <p>The generator will produce the followed sql:</p>
 * <p>
 * rs_1inddfmsuy (bookId, name, author, authorId) AS ( <br>
 * SELECT r.bookId, r.name, r.author, r.authorId <br>
 * FROM (User l LEFT JOIN Book r ON (l.userId = r.authorId)))
 * </p>
 * <p>
 * The output result of a relationship CTE is the all column of the right-side model.
 * We can think the right-side model is the output side.
 */
public class RelationshipCteGenerator
{
    private final GraphML graphML;
    private final Map<String, WithQuery> registeredCte = new LinkedHashMap<>();
    private final Map<String, String> nameMapping = new HashMap<>();
    private final Map<String, RelationshipCTEJoinInfo> relationshipInfoMapping = new HashMap<>();

    public RelationshipCteGenerator(GraphML graphML)
    {
        this.graphML = requireNonNull(graphML);
    }

    public void register(List<String> nameParts, List<RsItem> rsItems)
    {
        requireNonNull(nameParts, "nameParts is null");
        requireNonNull(rsItems, "rsItems is null");
        checkArgument(!nameParts.isEmpty(), "nameParts is empty");
        checkArgument(!rsItems.isEmpty() && rsItems.size() <= 2, "The size of rsItems should be 1 or 2");

        RelationshipCTE relationshipCTE = createRelationshipCTE(rsItems);
        String name = String.join(".", nameParts);
        WithQuery withQuery = transferToCte(relationshipCTE);
        registeredCte.put(name, withQuery);
        nameMapping.put(name, withQuery.getName().getValue());
        relationshipInfoMapping.put(withQuery.getName().getValue(), transferToRelationshipCTEJoinInfo(withQuery.getName().getValue(), relationshipCTE, nameParts.get(0)));
    }

    /**
     * Generate the join condition between the base model and the relationship CTE.
     * We used the output-side(right-side) model to decide the join condition with the base model.
     * <p>
     *
     * @param rsName the QualifiedName of the relationship column, e.g., `Book.author.book`.
     * @param relationshipCTE the parsed information of relationship.
     * @param baseModel the base model of the relationship column, e.g., `Book`.
     * @return The join condition between the base model and this relationship cte.
     */
    private RelationshipCTEJoinInfo transferToRelationshipCTEJoinInfo(String rsName, RelationshipCTE relationshipCTE, String baseModel)
    {
        if (baseModel.equals(relationshipCTE.getRight().getName())) {
            // The base model is same as the right side of `relationshipCTE`.
            return new RelationshipCTEJoinInfo(relationshipCTE.getName(),
                    // Use the primary key to do 1-1 mapping with the relationship CTE.
                    buildCondition(baseModel, relationshipCTE.getRight().getPrimaryKey(), rsName, relationshipCTE.getRight().getPrimaryKey()));
        }
        else {
            // The base model is same as the left side of `relationshipCTE`.
            return new RelationshipCTEJoinInfo(relationshipCTE.getName(),
                    // Use the join key to do 1-1 mapping with the relationship CTE.
                    buildCondition(baseModel, relationshipCTE.getLeft().getJoinKey(), rsName, relationshipCTE.getRight().getJoinKey()));
        }
    }

    private JoinCriteria buildCondition(String leftName, String leftKey, String rightName, String rightKey)
    {
        return joinOn(equal(nameReference(leftName, leftKey), nameReference(rightName, rightKey)));
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

            if (rsItems.get(1).getType() == RS) {
                left = new RelationshipCTE.Relation(
                        leftQuery.getName().getValue(),
                        leftQuery.getColumnNames().get().stream().map(Identifier::getValue).collect(toList()),
                        null,
                        getReferenceField(comparisonExpression.getLeft()));
                Model rightModel = getRightModel(relationship, graphML.listModels());
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
                // If it's a REVERSE relationship, the left and right side will be swapped.
                Model rightModel = getLeftModel(relationship, graphML.listModels());
                right = new RelationshipCTE.Relation(
                        rightModel.getName(),
                        rightModel.getColumns().stream().map(Column::getName).collect(toList()),
                        rightModel.getPrimaryKey(),
                        // If it's a REVERSE relationship, the left and right side will be swapped.
                        getReferenceField(comparisonExpression.getLeft()));
            }
        }
        else {
            if (rsItems.get(0).getType() == RS) {
                relationship = graphML.listRelationships().stream().filter(r -> r.getName().equals(rsItems.get(0).getName())).findAny().get();
                ComparisonExpression comparisonExpression = getConditionNode(relationship.getCondition());
                Model leftModel = getLeftModel(relationship, graphML.listModels());
                left = new RelationshipCTE.Relation(
                        leftModel.getName(),
                        leftModel.getColumns().stream().map(Column::getName).collect(toList()),
                        leftModel.getPrimaryKey(), getReferenceField(comparisonExpression.getLeft()));

                Model rightModel = getRightModel(relationship, graphML.listModels());
                right = new RelationshipCTE.Relation(
                        rightModel.getName(),
                        rightModel.getColumns().stream().map(Column::getName).collect(toList()),
                        rightModel.getPrimaryKey(), getReferenceField(comparisonExpression.getRight()));
            }
            else {
                relationship = graphML.listRelationships().stream().filter(r -> r.getName().equals(rsItems.get(0).getName())).findAny().get();
                ComparisonExpression comparisonExpression = getConditionNode(relationship.getCondition());
                // If it's a REVERSE relationship, the left and right side will be swapped.
                Model leftModel = getRightModel(relationship, graphML.listModels());
                left = new RelationshipCTE.Relation(
                        leftModel.getName(),
                        leftModel.getColumns().stream().map(Column::getName).collect(toList()),
                        // If it's a REVERSE relationship, the left and right side will be swapped.
                        leftModel.getPrimaryKey(), getReferenceField(comparisonExpression.getRight()));

                // If it's a REVERSE relationship, the left and right side will be swapped.
                Model rightModel = getLeftModel(relationship, graphML.listModels());
                right = new RelationshipCTE.Relation(
                        rightModel.getName(),
                        rightModel.getColumns().stream().map(Column::getName).collect(toList()),
                        // If it's a REVERSE relationship, the left and right side will be swapped.
                        rightModel.getPrimaryKey(), getReferenceField(comparisonExpression.getLeft()));
            }
        }
        return new RelationshipCTE("rs_" + randomTableSuffix(), left, right, relationship);
    }

    private static Model getLeftModel(Relationship relationship, List<Model> models)
    {
        return models.stream().filter(model -> model.getName().equals(relationship.getModels().get(0))).findAny()
                .orElseThrow(() -> new IllegalArgumentException(String.format("Left model %s not found in the given models.", relationship.getModels().get(0))));
    }

    private static Model getRightModel(Relationship relationship, List<Model> models)
    {
        return models.stream().filter(model -> model.getName().equals(relationship.getModels().get(1))).findAny()
                .orElseThrow(() -> new IllegalArgumentException(String.format("Right model %s not found in the given models.", relationship.getModels().get(1))));
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

    public Map<String, RelationshipCTEJoinInfo> getRelationshipInfoMapping()
    {
        return relationshipInfoMapping;
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

    /**
     * Used for build the join node between the base model and relationship cte.
     */
    public static class RelationshipCTEJoinInfo
    {
        private final String cteName;
        private final JoinCriteria condition;

        public RelationshipCTEJoinInfo(String cteName, JoinCriteria condition)
        {
            this.cteName = cteName;
            this.condition = condition;
        }

        public String getCteName()
        {
            return cteName;
        }

        public JoinCriteria getCondition()
        {
            return condition;
        }
    }
}
