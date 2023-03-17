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

package io.graphmdl.sqlrewrite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.graphmdl.base.GraphMDL;
import io.graphmdl.base.dto.Column;
import io.graphmdl.base.dto.JoinType;
import io.graphmdl.base.dto.Model;
import io.graphmdl.base.dto.Relationship;
import io.trino.sql.QueryUtil;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GroupBy;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.JoinCriteria;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SimpleGroupBy;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.WithQuery;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getLast;
import static io.graphmdl.base.dto.Relationship.SortKey.Ordering.ASC;
import static io.graphmdl.sqlrewrite.RelationshipCteGenerator.RsItem.Type.RS;
import static io.graphmdl.sqlrewrite.Utils.randomTableSuffix;
import static io.trino.sql.QueryUtil.aliased;
import static io.trino.sql.QueryUtil.equal;
import static io.trino.sql.QueryUtil.getConditionNode;
import static io.trino.sql.QueryUtil.identifier;
import static io.trino.sql.QueryUtil.joinOn;
import static io.trino.sql.QueryUtil.leftJoin;
import static io.trino.sql.QueryUtil.nameReference;
import static io.trino.sql.QueryUtil.quotedIdentifier;
import static io.trino.sql.QueryUtil.selectList;
import static io.trino.sql.QueryUtil.selectListDistinct;
import static io.trino.sql.QueryUtil.simpleQuery;
import static io.trino.sql.QueryUtil.subscriptExpression;
import static io.trino.sql.QueryUtil.table;
import static java.lang.String.format;
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
    private static final String ONE_REFERENCE = "one";
    private static final String MANY_REFERENCE = "many";

    private static final String SOURCE_REFERENCE = "s";

    private static final String TARGET_REFERENCE = "t";
    private final GraphMDL graphMDL;
    private final Map<String, WithQuery> registeredCte = new LinkedHashMap<>();
    private final Map<String, String> nameMapping = new HashMap<>();
    private final Map<String, RelationshipCTEJoinInfo> relationshipInfoMapping = new HashMap<>();

    public RelationshipCteGenerator(GraphMDL graphMDL)
    {
        this.graphMDL = requireNonNull(graphMDL);
    }

    public void register(List<String> nameParts, List<RsItem> rsItems)
    {
        requireNonNull(nameParts, "nameParts is null");
        requireNonNull(rsItems, "rsItems is null");
        checkArgument(!nameParts.isEmpty(), "nameParts is empty");
        checkArgument(!rsItems.isEmpty() && rsItems.size() <= 2, "The size of rsItems should be 1 or 2");

        RelationshipCTE relationshipCTE = createRelationshipCTE(rsItems);
        String name = String.join(".", nameParts);
        WithQuery withQuery = transferToCte(getLast(nameParts), relationshipCTE);
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
        if (relationshipCTE.getIndex().isPresent()) {
            checkArgument(relationshipCTE.getRelationship().getJoinType() == JoinType.ONE_TO_MANY, "Only one-to-many relationship can have index");
            if (baseModel.equals(relationshipCTE.getTarget().getName())) {
                // The base model is same as the output side of `relationshipCTE`.
                return new RelationshipCTEJoinInfo(relationshipCTE.getName(),
                        buildCondition(baseModel, relationshipCTE.getTarget().getPrimaryKey(), rsName, relationshipCTE.getTarget().getPrimaryKey()));
            }
            else {
                // The base model is same as the source side of `relationshipCTE`.
                return new RelationshipCTEJoinInfo(relationshipCTE.getName(),
                        buildCondition(baseModel, relationshipCTE.getSource().getJoinKey(), rsName, relationshipCTE.getTarget().getJoinKey()));
            }
        }

        // For one-to-many relationship, the source is always the one side.
        if (relationshipCTE.getRelationship().getJoinType().equals(JoinType.ONE_TO_MANY)) {
            if (baseModel.equals(relationshipCTE.getTarget().getName())) {
                // The base model is the many side of `relationshipCTE`.
                return new RelationshipCTEJoinInfo(relationshipCTE.getName(),
                        // Because one-to-many cte only output the many-side result in the relationship fields, the cte is the one-side object.
                        // Use the many-side's join key to do 1-1 mapping with the relationship CTE (one-side object).
                        buildCondition(baseModel, relationshipCTE.getTarget().getJoinKey(), rsName, relationshipCTE.getSource().getJoinKey()));
            }
            else {
                // The base model is the one side of `relationshipCTE`.
                return new RelationshipCTEJoinInfo(relationshipCTE.getName(),
                        buildCondition(baseModel, relationshipCTE.getSource().getJoinKey(), rsName, relationshipCTE.getSource().getJoinKey()));
            }
        }

        if (baseModel.equals(relationshipCTE.getTarget().getName())) {
            // The base model is same as the target side of `relationshipCTE`.
            return new RelationshipCTEJoinInfo(relationshipCTE.getName(),
                    // Use the primary key to do 1-1 mapping with the relationship CTE.
                    buildCondition(baseModel, relationshipCTE.getTarget().getPrimaryKey(), rsName, relationshipCTE.getTarget().getPrimaryKey()));
        }
        else {
            // The base model is same as the source side of `relationshipCTE`.
            return new RelationshipCTEJoinInfo(relationshipCTE.getName(),
                    // Use the join key to do 1-1 mapping with the relationship CTE.
                    buildCondition(baseModel, relationshipCTE.getSource().getJoinKey(), rsName, relationshipCTE.getTarget().getJoinKey()));
        }
    }

    private JoinCriteria buildCondition(String leftName, String leftKey, String rightName, String rightKey)
    {
        return joinOn(equal(nameReference(leftName, leftKey), nameReference(rightName, rightKey)));
    }

    private WithQuery transferToCte(String originalName, RelationshipCTE relationshipCTE)
    {
        switch (relationshipCTE.getRelationship().getJoinType()) {
            case ONE_TO_ONE:
                return oneToOneResultRelationship(relationshipCTE);
            case MANY_TO_ONE:
                return manyToOneResultRelationship(relationshipCTE);
            case ONE_TO_MANY:
                return relationshipCTE.getIndex().isPresent() ?
                        oneToManyRelationshipAccessByIndex(originalName, relationshipCTE) :
                        oneToManyResultRelationship(originalName, relationshipCTE);
        }
        throw new UnsupportedOperationException(format("%s relationship accessing is unsupported", relationshipCTE.getRelationship().getJoinType()));
    }

    private WithQuery oneToOneResultRelationship(RelationshipCTE relationshipCTE)
    {
        List<Expression> selectItems =
                ImmutableSet.<String>builder()
                        // make sure the primary key come first.
                        .add(relationshipCTE.getTarget().getPrimaryKey())
                        .addAll(relationshipCTE.getTarget().getColumns())
                        .add(relationshipCTE.getTarget().getJoinKey())
                        .build()
                        .stream()
                        .map(column -> nameReference(TARGET_REFERENCE, column))
                        .collect(toList());
        List<Identifier> outputSchema = selectItems.stream().map(this::getReferenceField).map(QueryUtil::identifier).collect(toList());

        return new WithQuery(identifier(relationshipCTE.getName()),
                simpleQuery(
                        selectList(selectItems),
                        leftJoin(aliased(table(QualifiedName.of(relationshipCTE.getSource().getName())), SOURCE_REFERENCE),
                                aliased(table(QualifiedName.of(relationshipCTE.getTarget().getName())), TARGET_REFERENCE),
                                joinOn(equal(nameReference(SOURCE_REFERENCE, relationshipCTE.getSource().getJoinKey()), nameReference(TARGET_REFERENCE, relationshipCTE.getTarget().getJoinKey()))))),
                Optional.of(outputSchema));
    }

    private WithQuery manyToOneResultRelationship(RelationshipCTE relationshipCTE)
    {
        List<Expression> selectItems =
                ImmutableSet.<String>builder()
                        // make sure the primary key come first.
                        .add(relationshipCTE.getTarget().getPrimaryKey())
                        .addAll(relationshipCTE.getTarget().getColumns())
                        .add(relationshipCTE.getTarget().getJoinKey())
                        .build()
                        .stream()
                        .map(column -> nameReference(TARGET_REFERENCE, column))
                        .collect(toList());
        List<Identifier> outputSchema = selectItems.stream().map(this::getReferenceField).map(QueryUtil::identifier).collect(toList());

        return new WithQuery(identifier(relationshipCTE.getName()),
                simpleQuery(
                        selectListDistinct(selectItems),
                        leftJoin(aliased(table(QualifiedName.of(relationshipCTE.getSource().getName())), SOURCE_REFERENCE),
                                aliased(table(QualifiedName.of(relationshipCTE.getTarget().getName())), TARGET_REFERENCE),
                                joinOn(equal(nameReference(SOURCE_REFERENCE, relationshipCTE.getSource().getJoinKey()), nameReference(TARGET_REFERENCE, relationshipCTE.getTarget().getJoinKey()))))),
                Optional.of(outputSchema));
    }

    private WithQuery oneToManyResultRelationship(String originalName, RelationshipCTE relationshipCTE)
    {
        List<Expression> oneTableFields =
                ImmutableSet.<String>builder()
                        // make sure the primary key be first.
                        .add(relationshipCTE.getSource().getPrimaryKey())
                        // remove duplicate relationship column name
                        .addAll(relationshipCTE.getSource().getColumns().stream().filter(column -> !column.equals(originalName)).collect(toList()))
                        .add(relationshipCTE.getSource().getJoinKey())
                        .build()
                        .stream()
                        .map(column -> nameReference(ONE_REFERENCE, column))
                        .collect(toList());
        List<Relationship.SortKey> sortKeys = relationshipCTE.getRelationship().getManySideSortKeys().isEmpty() ?
                List.of(new Relationship.SortKey(relationshipCTE.getManySide().getPrimaryKey(), ASC)) :
                relationshipCTE.getRelationship().getManySideSortKeys();

        SingleColumn relationshipField = new SingleColumn(
                toArrayAgg(nameReference(MANY_REFERENCE, relationshipCTE.getTarget().getPrimaryKey()), sortKeys),
                identifier(originalName));

        List<SingleColumn> normalFields = oneTableFields
                .stream()
                .map(field -> new SingleColumn(field, identifier(requireNonNull(getQualifiedName(field)).getSuffix())))
                .collect(toList());

        List<SelectItem> selectItems = ImmutableList.<SelectItem>builder().addAll(normalFields).add(relationshipField).build();

        List<Identifier> outputSchema = selectItems.stream()
                .map(selectItem -> (SingleColumn) selectItem)
                .map(singleColumn ->
                        singleColumn.getAlias()
                                .orElse(quotedIdentifier(singleColumn.getExpression().toString())))
                .collect(toList());

        return new WithQuery(identifier(relationshipCTE.getName()),
                simpleQuery(
                        new Select(false, selectItems),
                        leftJoin(aliased(table(QualifiedName.of(relationshipCTE.getSource().getName())), ONE_REFERENCE),
                                aliased(table(QualifiedName.of(relationshipCTE.getTarget().getName())), MANY_REFERENCE),
                                joinOn(equal(nameReference(ONE_REFERENCE, relationshipCTE.getSource().getJoinKey()), nameReference(MANY_REFERENCE, relationshipCTE.getTarget().getJoinKey())))),
                        Optional.empty(),
                        Optional.of(new GroupBy(false, IntStream.range(1, oneTableFields.size() + 1)
                                .mapToObj(number -> new LongLiteral(String.valueOf(number)))
                                .map(longLiteral -> new SimpleGroupBy(List.of(longLiteral))).collect(toList()))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                Optional.of(outputSchema));
    }

    private WithQuery oneToManyRelationshipAccessByIndex(String originalName, RelationshipCTE relationshipCTE)
    {
        checkArgument(relationshipCTE.getIndex().isPresent(), "index is null");
        List<SelectItem> selectItems =
                ImmutableSet.<String>builder()
                        // make sure the primary key be first.
                        .add(relationshipCTE.getTarget().getPrimaryKey())
                        .addAll(relationshipCTE.getTarget().getColumns())
                        .add(relationshipCTE.getTarget().getJoinKey())
                        .build()
                        .stream()
                        .map(column -> nameReference("r", column))
                        .map(field -> new SingleColumn(field, identifier(requireNonNull(getQualifiedName(field)).getSuffix())))

                        .collect(toList());

        List<Identifier> outputSchema = selectItems.stream()
                .map(selectItem -> (SingleColumn) selectItem)
                .map(singleColumn ->
                        singleColumn.getAlias()
                                .orElse(quotedIdentifier(singleColumn.getExpression().toString())))
                .collect(toList());

        return new WithQuery(identifier(relationshipCTE.getName()),
                simpleQuery(
                        new Select(false, selectItems),
                        leftJoin(aliased(table(QualifiedName.of(relationshipCTE.getSource().getName())), "l"),
                                aliased(table(QualifiedName.of(relationshipCTE.getTarget().getName())), "r"),
                                joinOn(equal(subscriptExpression(nameReference("l", originalName.split("\\[")[0]), relationshipCTE.getIndex().get()), nameReference("r", relationshipCTE.getTarget().getPrimaryKey()))))),
                Optional.of(outputSchema));
    }

    private QualifiedName getQualifiedName(Expression expression)
    {
        if (expression instanceof DereferenceExpression) {
            return DereferenceExpression.getQualifiedName((DereferenceExpression) expression);
        }
        if (expression instanceof Identifier) {
            return QualifiedName.of(ImmutableList.of((Identifier) expression));
        }
        return null;
    }

    private Expression toArrayAgg(Expression field, List<Relationship.SortKey> sortKeys)
    {
        // TODO: BigQuery doesn't allow array element is null.
        //  We need to surround the array_agg with ifnull function or other null handling.

        return new FunctionCall(
                Optional.empty(),
                QualifiedName.of("array_agg"),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new OrderBy(sortKeys.stream()
                        .map(sortKey ->
                                new SortItem(nameReference(MANY_REFERENCE, sortKey.getName()), sortKey.isDescending() ? SortItem.Ordering.DESCENDING : SortItem.Ordering.ASCENDING,
                                        SortItem.NullOrdering.UNDEFINED))
                        .collect(toList()))),
                false,
                Optional.empty(),
                Optional.empty(),
                List.of(field));
    }

    private RelationshipCTE createRelationshipCTE(List<RsItem> rsItems)
    {
        RelationshipCTE.Relation source;
        RelationshipCTE.Relation target;
        Relationship relationship;
        // If the first item is CTE, the second one is RS or REVERSE_RS.
        if (rsItems.get(0).getType().equals(RsItem.Type.CTE)) {
            relationship = graphMDL.listRelationships().stream().filter(r -> r.getName().equals(rsItems.get(1).getName())).findAny().get();
            ComparisonExpression comparisonExpression = getConditionNode(relationship.getCondition());
            WithQuery leftQuery = registeredCte.get(rsItems.get(0).getName());

            if (rsItems.get(1).getType() == RS) {
                source = new RelationshipCTE.Relation(
                        leftQuery.getName().getValue(),
                        leftQuery.getColumnNames().get().stream().map(Identifier::getValue).collect(toList()),
                        // TODO: we should make sure the first field is its primary key.
                        leftQuery.getColumnNames().map(columns -> columns.get(0).getValue()).get(),
                        getReferenceField(comparisonExpression.getLeft()));
                Model rightModel = getRightModel(relationship, graphMDL.listModels());
                target = new RelationshipCTE.Relation(
                        rightModel.getName(),
                        rightModel.getColumns().stream().map(Column::getName).collect(toList()),
                        rightModel.getPrimaryKey(),
                        getReferenceField(comparisonExpression.getRight()));
            }
            else {
                source = new RelationshipCTE.Relation(
                        leftQuery.getName().getValue(),
                        leftQuery.getColumnNames().get().stream().map(Identifier::getValue).collect(toList()),
                        // TODO: we should make sure the first field is its primary key.
                        leftQuery.getColumnNames().map(columns -> columns.get(0).getValue()).get(),
                        // If it's a REVERSE relationship, the left and right side will be swapped.
                        getReferenceField(comparisonExpression.getRight()));
                // If it's a REVERSE relationship, the left and right side will be swapped.
                Model rightModel = getLeftModel(relationship, graphMDL.listModels());
                target = new RelationshipCTE.Relation(
                        rightModel.getName(),
                        rightModel.getColumns().stream().map(Column::getName).collect(toList()),
                        rightModel.getPrimaryKey(),
                        // If it's a REVERSE relationship, the left and right side will be swapped.
                        getReferenceField(comparisonExpression.getLeft()));
            }
        }
        else {
            if (rsItems.get(0).getType() == RS) {
                relationship = graphMDL.listRelationships().stream().filter(r -> r.getName().equals(rsItems.get(0).getName())).findAny().get();
                ComparisonExpression comparisonExpression = getConditionNode(relationship.getCondition());
                Model leftModel = getLeftModel(relationship, graphMDL.listModels());
                source = new RelationshipCTE.Relation(
                        leftModel.getName(),
                        leftModel.getColumns().stream().map(Column::getName).collect(toList()),
                        leftModel.getPrimaryKey(), getReferenceField(comparisonExpression.getLeft()));

                Model rightModel = getRightModel(relationship, graphMDL.listModels());
                target = new RelationshipCTE.Relation(
                        rightModel.getName(),
                        rightModel.getColumns().stream().map(Column::getName).collect(toList()),
                        rightModel.getPrimaryKey(), getReferenceField(comparisonExpression.getRight()));
            }
            else {
                relationship = graphMDL.listRelationships().stream().filter(r -> r.getName().equals(rsItems.get(0).getName())).findAny().get();
                ComparisonExpression comparisonExpression = getConditionNode(relationship.getCondition());
                // If it's a REVERSE relationship, the left and right side will be swapped.
                Model leftModel = getRightModel(relationship, graphMDL.listModels());
                source = new RelationshipCTE.Relation(
                        leftModel.getName(),
                        leftModel.getColumns().stream().map(Column::getName).collect(toList()),
                        // If it's a REVERSE relationship, the left and right side will be swapped.
                        leftModel.getPrimaryKey(), getReferenceField(comparisonExpression.getRight()));

                // If it's a REVERSE relationship, the left and right side will be swapped.
                Model rightModel = getLeftModel(relationship, graphMDL.listModels());
                target = new RelationshipCTE.Relation(
                        rightModel.getName(),
                        rightModel.getColumns().stream().map(Column::getName).collect(toList()),
                        // If it's a REVERSE relationship, the left and right side will be swapped.
                        rightModel.getPrimaryKey(), getReferenceField(comparisonExpression.getLeft()));
            }
        }

        return new RelationshipCTE("rs_" + randomTableSuffix(), source, target,
                getLast(rsItems).getType().equals(RS) ? relationship : Relationship.reverse(relationship),
                rsItems.get(0).getIndex().orElse(null));
    }

    private static Model getLeftModel(Relationship relationship, List<Model> models)
    {
        return models.stream().filter(model -> model.getName().equals(relationship.getModels().get(0))).findAny()
                .orElseThrow(() -> new IllegalArgumentException(format("Left model %s not found in the given models.", relationship.getModels().get(0))));
    }

    private static Model getRightModel(Relationship relationship, List<Model> models)
    {
        return models.stream().filter(model -> model.getName().equals(relationship.getModels().get(1))).findAny()
                .orElseThrow(() -> new IllegalArgumentException(format("Right model %s not found in the given models.", relationship.getModels().get(1))));
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
            return rsItem(name, type, null);
        }

        public static RsItem rsItem(String name, Type type, String index)
        {
            return new RsItem(name, type, index);
        }

        public enum Type
        {
            CTE,
            RS,
            REVERSE_RS
        }

        private final String name;
        private final Type type;

        private final String index;

        private RsItem(String name, Type type, String index)
        {
            this.name = name;
            this.type = type;
            this.index = index;
        }

        public Optional<String> getIndex()
        {
            return Optional.ofNullable(index);
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
