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
import com.google.common.collect.ImmutableSet;
import io.accio.base.AccioMDL;
import io.accio.base.dto.Column;
import io.accio.base.dto.Model;
import io.accio.base.dto.Relationship;
import io.accio.base.dto.Relationship.SortKey;
import io.trino.sql.QueryUtil;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GroupBy;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IsNotNullPredicate;
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
import static io.accio.base.dto.Relationship.SortKey.Ordering.ASC;
import static io.accio.base.dto.Relationship.SortKey.sortKey;
import static io.accio.sqlrewrite.RelationshipCteGenerator.RsItem.Type.RS;
import static io.accio.sqlrewrite.Utils.randomTableSuffix;
import static io.trino.sql.QueryUtil.aliased;
import static io.trino.sql.QueryUtil.crossJoin;
import static io.trino.sql.QueryUtil.equal;
import static io.trino.sql.QueryUtil.getConditionNode;
import static io.trino.sql.QueryUtil.identifier;
import static io.trino.sql.QueryUtil.joinOn;
import static io.trino.sql.QueryUtil.leftJoin;
import static io.trino.sql.QueryUtil.nameReference;
import static io.trino.sql.QueryUtil.quotedIdentifier;
import static io.trino.sql.QueryUtil.simpleQuery;
import static io.trino.sql.QueryUtil.subscriptExpression;
import static io.trino.sql.QueryUtil.table;
import static io.trino.sql.QueryUtil.unnest;
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
    private static final String ONE_REFERENCE = "o";
    private static final String MANY_REFERENCE = "m";

    public static final String SOURCE_REFERENCE = "s";

    public static final String TARGET_REFERENCE = "t";
    public static final String LAMBDA_RESULT_NAME = "f1";

    private static final String UNNEST_REFERENCE = "u";

    private static final String UNNEST_COLUMN_REFERENCE = "uc";
    private static final String BASE_KEY_ALIAS = "bk";
    private final AccioMDL accioMDL;
    private final Map<String, WithQuery> registeredWithQuery = new LinkedHashMap<>();
    private final Map<String, RelationshipCTE> registeredCte = new HashMap<>();
    private final Map<String, String> nameMapping = new HashMap<>();
    private final Map<String, RelationshipCTEJoinInfo> relationshipInfoMapping = new HashMap<>();

    public RelationshipCteGenerator(AccioMDL accioMDL)
    {
        this.accioMDL = requireNonNull(accioMDL);
    }

    public void register(List<String> nameParts, RelationshipOperation operation)
    {
        requireNonNull(nameParts, "nameParts is null");
        checkArgument(!nameParts.isEmpty(), "nameParts is empty");
        register(nameParts, operation, nameParts.get(0), getLast(nameParts));
    }

    public void register(List<String> nameParts, RelationshipOperation operation, String baseModel)
    {
        requireNonNull(nameParts, "nameParts is null");
        checkArgument(!nameParts.isEmpty(), "nameParts is empty");
        register(nameParts, operation, baseModel, getLast(nameParts));
    }

    public void register(List<String> nameParts, RelationshipOperation operation, String baseModel, String originalName)
    {
        requireNonNull(nameParts, "nameParts is null");
        requireNonNull(operation.getRsItems(), "rsItems is null");
        checkArgument(!nameParts.isEmpty(), "nameParts is empty");
        checkArgument(!operation.getRsItems().isEmpty() && operation.getRsItems().size() <= 2, "The size of rsItems should be 1 or 2");

        // avoid duplicate cte registering
        if (nameMapping.containsKey(String.join(".", nameParts))) {
            return;
        }

        RelationshipCTE relationshipCTE = createRelationshipCTE(operation.getRsItems());
        String name = String.join(".", nameParts);
        WithQuery withQuery = transferToCte(originalName, relationshipCTE, operation);
        registeredWithQuery.put(name, withQuery);
        registeredCte.put(name, relationshipCTE);
        nameMapping.put(name, withQuery.getName().getValue());
        relationshipInfoMapping.put(withQuery.getName().getValue(), transferToRelationshipCTEJoinInfo(withQuery.getName().getValue(), relationshipCTE, baseModel));
    }

    /**
     * Generate the join condition between the base model and the relationship CTE.
     * We used the output-side(right-side) model to decide the join condition with the base model.
     * <p>
     *
     * @param rsName the QualifiedName of the relationship column, e.g., `Book.author.book`.
     * @param relationshipCTE the parsed information of relationship.
     * @param baseModelName the base model of the relationship column, e.g., `Book`.
     * @return The join condition between the base model and this relationship cte.
     */
    private RelationshipCTEJoinInfo transferToRelationshipCTEJoinInfo(String rsName, RelationshipCTE relationshipCTE, String baseModelName)
    {
        Model baseModel = accioMDL.getModel(baseModelName).orElseThrow(() -> new IllegalArgumentException(format("Model %s is not found", baseModelName)));
        return new RelationshipCTEJoinInfo(relationshipCTE.getName(),
                buildCondition(baseModelName, baseModel.getPrimaryKey(), rsName, BASE_KEY_ALIAS), baseModelName);
    }

    private JoinCriteria buildCondition(String leftName, String leftKey, String rightName, String rightKey)
    {
        return joinOn(equal(nameReference(leftName, leftKey), nameReference(rightName, rightKey)));
    }

    private WithQuery transferToCte(String originalName, RelationshipCTE relationshipCTE, RelationshipOperation operation)
    {
        List<Expression> arguments = operation.getFunctionCallArguments();
        switch (operation.getOperatorType()) {
            case ACCESS:
                return transferToAccessCte(originalName, relationshipCTE);
            case TRANSFORM:
                checkArgument(operation.getLambdaExpression().isPresent(), "Lambda expression is missing");
                return transferToTransformCte(
                        operation.getManySideResultField().orElse(operation.getLambdaExpression().get().toString()),
                        operation.getLambdaExpression().get(), relationshipCTE, operation.getUnnestField());
            case FILTER:
                checkArgument(operation.getLambdaExpression().isPresent(), "Lambda expression is missing");
                return transferToFilterCte(
                        operation.getManySideResultField().orElse(operation.getLambdaExpression().get().toString()),
                        operation.getLambdaExpression().get(), relationshipCTE, operation.getUnnestField());
            case AGGREGATE:
                checkArgument(operation.getAggregateOperator().isPresent(), "Aggregate operator is missing");
                return transferToAggregateCte(
                        operation.getManySideResultField()
                                .orElseThrow(() -> new IllegalArgumentException(operation.getAggregateOperator().get() + " relationship field not found")),
                        relationshipCTE, operation.getUnnestField(), operation.getAggregateOperator().get());
            case ARRAY_SORT:
                checkArgument(arguments.size() == 3, "array_sort function should have 3 arguments");
                SortKey sortKey = sortKey(arguments.get(1).toString(), SortKey.Ordering.get(arguments.get(2).toString()));
                return transferToArraySortCte(
                        operation.getManySideResultField().orElseThrow(() -> new IllegalArgumentException("array_sort relationship field not found")),
                        sortKey,
                        relationshipCTE,
                        operation.getUnnestField());
            case SLICE:
                checkArgument(arguments.size() == 3, "slice function should have 3 arguments");
                checkArgument(arguments.get(1) instanceof LongLiteral, "Incorrect argument in slice function second argument");
                checkArgument(arguments.get(2) instanceof LongLiteral, "Incorrect argument in slice function third argument");
                return transferToSliceCte(
                        operation.getManySideResultField().orElseThrow(() -> new IllegalArgumentException("array_sort relationship field not found")),
                        relationshipCTE,
                        arguments.subList(1, 3));
        }
        throw new UnsupportedOperationException(format("%s relationship operation is unsupported", operation.getOperatorType()));
    }

    private WithQuery transferToAccessCte(String originalName, RelationshipCTE relationshipCTE)
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
        List<SelectItem> targetSelectItem =
                ImmutableSet.<String>builder()
                        // make sure the primary key come first.
                        .add(relationshipCTE.getTarget().getPrimaryKey())
                        .addAll(relationshipCTE.getTarget().getColumns())
                        .add(relationshipCTE.getTarget().getJoinKey())
                        .build()
                        .stream()
                        .map(column -> nameReference(TARGET_REFERENCE, column))
                        .map(SingleColumn::new)
                        .collect(toList());

        ImmutableSet.Builder<SelectItem> builder = ImmutableSet
                .<SelectItem>builder()
                .addAll(targetSelectItem)
                .add(new SingleColumn(nameReference(SOURCE_REFERENCE, relationshipCTE.getBaseKey()), identifier(BASE_KEY_ALIAS)));
        List<SelectItem> selectItems = ImmutableList.copyOf(builder.build());

        List<Identifier> outputSchema = selectItems.stream()
                .map(item -> (SingleColumn) item)
                .map(item -> item.getAlias().map(alias -> (Expression) alias).orElse(item.getExpression()))
                .map(this::getReferenceField).map(QueryUtil::identifier).collect(toList());

        return new WithQuery(identifier(relationshipCTE.getName()),
                simpleQuery(
                        new Select(false, selectItems),
                        leftJoin(aliased(table(QualifiedName.of(relationshipCTE.getSource().getName())), SOURCE_REFERENCE),
                                aliased(table(QualifiedName.of(relationshipCTE.getTarget().getName())), TARGET_REFERENCE),
                                joinOn(equal(nameReference(SOURCE_REFERENCE, relationshipCTE.getSource().getJoinKey()), nameReference(TARGET_REFERENCE, relationshipCTE.getTarget().getJoinKey()))))),
                Optional.of(outputSchema));
    }

    private WithQuery manyToOneResultRelationship(RelationshipCTE relationshipCTE)
    {
        List<SelectItem> targetSelectItem =
                ImmutableSet.<String>builder()
                        // make sure the primary key come first.
                        .add(relationshipCTE.getTarget().getPrimaryKey())
                        .addAll(relationshipCTE.getTarget().getColumns())
                        .add(relationshipCTE.getTarget().getJoinKey())
                        .build()
                        .stream()
                        .map(column -> nameReference(TARGET_REFERENCE, column))
                        .map(SingleColumn::new)
                        .collect(toList());

        ImmutableSet.Builder<SelectItem> builder = ImmutableSet
                .<SelectItem>builder()
                .addAll(targetSelectItem)
                .add(new SingleColumn(nameReference(SOURCE_REFERENCE, relationshipCTE.getBaseKey()), identifier(BASE_KEY_ALIAS)));
        List<SelectItem> selectItems = ImmutableList.copyOf(builder.build());

        List<Identifier> outputSchema = selectItems.stream()
                .map(item -> (SingleColumn) item)
                .map(item -> item.getAlias().map(alias -> (Expression) alias).orElse(item.getExpression()))
                .map(this::getReferenceField).map(QueryUtil::identifier).collect(toList());

        return new WithQuery(identifier(relationshipCTE.getName()),
                simpleQuery(
                        new Select(true, selectItems),
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
                        .add(relationshipCTE.getSource().getJoinKey())
                        .build()
                        .stream()
                        .map(column -> nameReference(ONE_REFERENCE, column))
                        .collect(toList());
        List<Relationship.SortKey> sortKeys = relationshipCTE.getRelationship().getManySideSortKeys().isEmpty() ?
                List.of(new Relationship.SortKey(relationshipCTE.getManySide().getPrimaryKey(), ASC)) :
                relationshipCTE.getRelationship().getManySideSortKeys();

        SingleColumn relationshipField = new SingleColumn(
                toArrayAgg(nameReference(MANY_REFERENCE, relationshipCTE.getTarget().getPrimaryKey()), MANY_REFERENCE, sortKeys),
                identifier(originalName));

        List<SingleColumn> normalFields = oneTableFields
                .stream()
                .map(field -> new SingleColumn(field, identifier(requireNonNull(getQualifiedName(field)).getSuffix())))
                .collect(toList());
        normalFields.add(new SingleColumn(nameReference(ONE_REFERENCE, relationshipCTE.getBaseKey()), identifier(BASE_KEY_ALIAS)));

        ImmutableSet.Builder<SelectItem> builder = ImmutableSet
                .<SelectItem>builder()
                .addAll(normalFields)
                .add(relationshipField);
        List<SelectItem> selectItems = ImmutableList.copyOf(builder.build());

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
                        Optional.of(new GroupBy(false, IntStream.rangeClosed(1, normalFields.size())
                                .mapToObj(number -> new LongLiteral(String.valueOf(number)))
                                .map(longLiteral -> new SimpleGroupBy(List.of(longLiteral)))
                                .collect(toList()))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                Optional.of(outputSchema));
    }

    private WithQuery oneToManyRelationshipAccessByIndex(String originalName, RelationshipCTE relationshipCTE)
    {
        checkArgument(relationshipCTE.getIndex().isPresent(), "index is null");
        List<SelectItem> targetSelectItem =
                ImmutableSet.<String>builder()
                        // make sure the primary key be first.
                        .add(relationshipCTE.getTarget().getPrimaryKey())
                        .addAll(relationshipCTE.getTarget().getColumns())
                        .add(relationshipCTE.getTarget().getJoinKey())
                        .build()
                        .stream()
                        .map(column -> nameReference(TARGET_REFERENCE, column))
                        .map(field -> new SingleColumn(field, identifier(requireNonNull(getQualifiedName(field)).getSuffix())))
                        .collect(toList());

        ImmutableSet.Builder<SelectItem> builder = ImmutableSet
                .<SelectItem>builder()
                .addAll(targetSelectItem)
                .add(new SingleColumn(nameReference(SOURCE_REFERENCE, relationshipCTE.getBaseKey()), identifier(BASE_KEY_ALIAS)));
        List<SelectItem> selectItems = ImmutableList.copyOf(builder.build());

        List<Identifier> outputSchema = selectItems.stream()
                .map(selectItem -> (SingleColumn) selectItem)
                .map(singleColumn ->
                        singleColumn.getAlias()
                                .orElse(quotedIdentifier(singleColumn.getExpression().toString())))
                .collect(toList());

        return new WithQuery(identifier(relationshipCTE.getName()),
                simpleQuery(
                        new Select(false, selectItems),
                        leftJoin(aliased(table(QualifiedName.of(relationshipCTE.getSource().getName())), SOURCE_REFERENCE),
                                aliased(table(QualifiedName.of(relationshipCTE.getTarget().getName())), TARGET_REFERENCE),
                                joinOn(equal(subscriptExpression(nameReference(SOURCE_REFERENCE, originalName.split("\\[")[0]), relationshipCTE.getIndex().get()), nameReference(TARGET_REFERENCE, relationshipCTE.getTarget().getPrimaryKey()))))),
                Optional.of(outputSchema));
    }

    private WithQuery transferToTransformCte(
            String manyResultField,
            Expression lambdaExpressionBody,
            RelationshipCTE relationshipCTE,
            Optional<Expression> outputField)
    {
        List<Expression> oneTableFields =
                ImmutableSet.<String>builder()
                        // make sure the primary key be first.
                        .add(relationshipCTE.getSource().getPrimaryKey())
                        // remove duplicate column name
                        .addAll(relationshipCTE.getSource().getColumns().stream()
                                .filter(column -> !column.equals(manyResultField) && !column.equals(LAMBDA_RESULT_NAME))
                                .collect(toList()))
                        .add(relationshipCTE.getSource().getJoinKey())
                        .build()
                        .stream()
                        .map(column -> nameReference(SOURCE_REFERENCE, column))
                        .collect(toList());
        List<Relationship.SortKey> sortKeys = relationshipCTE.getRelationship().getManySideSortKeys().isEmpty() ?
                List.of(new Relationship.SortKey(relationshipCTE.getManySide().getPrimaryKey(), ASC)) :
                relationshipCTE.getRelationship().getManySideSortKeys();

        SingleColumn arrayAggField = new SingleColumn(
                toArrayAgg(lambdaExpressionBody, TARGET_REFERENCE, sortKeys),
                identifier(LAMBDA_RESULT_NAME));

        List<SingleColumn> normalFields = oneTableFields
                .stream()
                .map(field -> new SingleColumn(field, identifier(requireNonNull(getQualifiedName(field)).getSuffix())))
                .collect(toList());

        ImmutableSet.Builder<SelectItem> builder = ImmutableSet
                .<SelectItem>builder()
                .addAll(normalFields)
                .add(arrayAggField)
                .add(new SingleColumn(nameReference(SOURCE_REFERENCE, relationshipCTE.getBaseKey()), identifier(BASE_KEY_ALIAS)));
        List<SelectItem> selectItems = ImmutableList.copyOf(builder.build());

        List<Identifier> outputSchema = selectItems.stream()
                .map(selectItem -> (SingleColumn) selectItem)
                .map(singleColumn ->
                        singleColumn.getAlias()
                                .orElse(quotedIdentifier(singleColumn.getExpression().toString())))
                .collect(toList());

        Expression unnestField = outputField.orElse(nameReference(SOURCE_REFERENCE, manyResultField));

        return new WithQuery(identifier(relationshipCTE.getName()),
                simpleQuery(
                        new Select(false, selectItems),
                        leftJoin(
                                crossJoin(aliased(table(QualifiedName.of(relationshipCTE.getSource().getName())), SOURCE_REFERENCE),
                                        aliased(unnest(unnestField), UNNEST_REFERENCE, List.of(UNNEST_COLUMN_REFERENCE))),
                                aliased(table(QualifiedName.of(relationshipCTE.getTarget().getName())), TARGET_REFERENCE),
                                joinOn(equal(nameReference(UNNEST_REFERENCE, UNNEST_COLUMN_REFERENCE), nameReference(TARGET_REFERENCE, relationshipCTE.getTarget().getPrimaryKey())))),
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

    private WithQuery transferToAggregateCte(
            String manyResultField,
            RelationshipCTE relationshipCTE,
            Optional<Expression> outputField,
            String operator)
    {
        List<Expression> oneTableFields =
                ImmutableSet.<String>builder()
                        // make sure the primary key be first.
                        .add(relationshipCTE.getSource().getPrimaryKey())
                        // remove duplicate column name
                        .addAll(relationshipCTE.getSource().getColumns().stream()
                                .filter(column -> !column.equals(manyResultField) && !column.equals(LAMBDA_RESULT_NAME))
                                .collect(toList()))
                        .add(relationshipCTE.getSource().getJoinKey())
                        .build()
                        .stream()
                        .map(column -> nameReference(SOURCE_REFERENCE, column))
                        .collect(toList());

        SingleColumn aggField = new SingleColumn(
                toAggregate(DereferenceExpression.from(QualifiedName.of(UNNEST_REFERENCE, UNNEST_COLUMN_REFERENCE)), operator),
                identifier(LAMBDA_RESULT_NAME));

        List<SingleColumn> normalFields = oneTableFields
                .stream()
                .map(field -> new SingleColumn(field, identifier(requireNonNull(getQualifiedName(field)).getSuffix())))
                .collect(toList());

        ImmutableSet.Builder<SelectItem> builder = ImmutableSet
                .<SelectItem>builder()
                .addAll(normalFields)
                .add(aggField)
                .add(new SingleColumn(nameReference(SOURCE_REFERENCE, relationshipCTE.getBaseKey()), identifier(BASE_KEY_ALIAS)));
        List<SelectItem> selectItems = ImmutableList.copyOf(builder.build());

        List<Identifier> outputSchema = selectItems.stream()
                .map(selectItem -> (SingleColumn) selectItem)
                .map(singleColumn ->
                        singleColumn.getAlias()
                                .orElse(quotedIdentifier(singleColumn.getExpression().toString())))
                .collect(toList());

        Expression unnestField = outputField.orElse(nameReference(SOURCE_REFERENCE, manyResultField));

        return new WithQuery(identifier(relationshipCTE.getName()),
                simpleQuery(
                        new Select(false, selectItems),
                        crossJoin(aliased(table(QualifiedName.of(relationshipCTE.getSource().getName())), SOURCE_REFERENCE),
                                aliased(unnest(unnestField), UNNEST_REFERENCE, List.of(UNNEST_COLUMN_REFERENCE))),
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

    private WithQuery transferToFilterCte(
            String manyResultField,
            Expression lambdaExpressionBody,
            RelationshipCTE relationshipCTE,
            Optional<Expression> outputField)
    {
        List<Expression> oneTableFields =
                ImmutableSet.<String>builder()
                        // make sure the primary key be first.
                        .add(relationshipCTE.getSource().getPrimaryKey())
                        // remove column name
                        .addAll(relationshipCTE.getSource().getColumns().stream()
                                .filter(column -> !column.equals(manyResultField) && !column.equals(LAMBDA_RESULT_NAME))
                                .collect(toList()))
                        .add(relationshipCTE.getSource().getJoinKey())
                        .build()
                        .stream()
                        .map(column -> nameReference(SOURCE_REFERENCE, column))
                        .collect(toList());
        List<Relationship.SortKey> sortKeys = relationshipCTE.getRelationship().getManySideSortKeys().isEmpty() ?
                List.of(new Relationship.SortKey(relationshipCTE.getManySide().getPrimaryKey(), ASC)) :
                relationshipCTE.getRelationship().getManySideSortKeys();

        SingleColumn arrayAggField = new SingleColumn(
                toArrayAgg(DereferenceExpression.from(QualifiedName.of(TARGET_REFERENCE, relationshipCTE.getTarget().getPrimaryKey())), TARGET_REFERENCE, sortKeys),
                identifier(LAMBDA_RESULT_NAME));

        List<SingleColumn> normalFields = oneTableFields
                .stream()
                .map(field -> new SingleColumn(field, identifier(requireNonNull(getQualifiedName(field)).getSuffix())))
                .collect(toList());

        ImmutableSet.Builder<SelectItem> builder = ImmutableSet
                .<SelectItem>builder()
                .addAll(normalFields)
                .add(arrayAggField)
                .add(new SingleColumn(nameReference(SOURCE_REFERENCE, relationshipCTE.getBaseKey()), identifier(BASE_KEY_ALIAS)));
        List<SelectItem> selectItems = ImmutableList.copyOf(builder.build());

        List<Identifier> outputSchema = selectItems.stream()
                .map(selectItem -> (SingleColumn) selectItem)
                .map(singleColumn ->
                        singleColumn.getAlias()
                                .orElse(quotedIdentifier(singleColumn.getExpression().toString())))
                .collect(toList());

        Expression unnestField = outputField.orElse(nameReference(SOURCE_REFERENCE, manyResultField));

        return new WithQuery(identifier(relationshipCTE.getName()),
                simpleQuery(
                        new Select(false, selectItems),
                        leftJoin(
                                crossJoin(aliased(table(QualifiedName.of(relationshipCTE.getSource().getName())), SOURCE_REFERENCE),
                                        aliased(unnest(unnestField), UNNEST_REFERENCE, List.of(UNNEST_COLUMN_REFERENCE))),
                                aliased(table(QualifiedName.of(relationshipCTE.getTarget().getName())), TARGET_REFERENCE),
                                joinOn(equal(nameReference(UNNEST_REFERENCE, UNNEST_COLUMN_REFERENCE), nameReference(TARGET_REFERENCE, relationshipCTE.getTarget().getPrimaryKey())))),
                        Optional.of(lambdaExpressionBody),
                        Optional.of(new GroupBy(false, IntStream.range(1, oneTableFields.size() + 1)
                                .mapToObj(number -> new LongLiteral(String.valueOf(number)))
                                .map(longLiteral -> new SimpleGroupBy(List.of(longLiteral))).collect(toList()))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                Optional.of(outputSchema));
    }

    private WithQuery transferToArraySortCte(
            String manyResultField,
            SortKey sortKey,
            RelationshipCTE relationshipCTE,
            Optional<Expression> outputField)
    {
        List<Expression> oneTableFields =
                ImmutableSet.<String>builder()
                        // make sure the primary key be first.
                        .add(relationshipCTE.getSource().getPrimaryKey())
                        // remove column name
                        .addAll(relationshipCTE.getSource().getColumns().stream()
                                .filter(column -> !column.equals(manyResultField) && !column.equals(LAMBDA_RESULT_NAME))
                                .collect(toList()))
                        .add(relationshipCTE.getSource().getJoinKey())
                        .build()
                        .stream()
                        .map(column -> nameReference(SOURCE_REFERENCE, column))
                        .collect(toList());
        List<Relationship.SortKey> sortKeys = List.of(sortKey);

        SingleColumn arrayAggField = new SingleColumn(
                toArrayAgg(DereferenceExpression.from(QualifiedName.of(TARGET_REFERENCE, relationshipCTE.getTarget().getPrimaryKey())), TARGET_REFERENCE, sortKeys),
                identifier(LAMBDA_RESULT_NAME));

        List<SingleColumn> normalFields = oneTableFields
                .stream()
                .map(field -> new SingleColumn(field, identifier(requireNonNull(getQualifiedName(field)).getSuffix())))
                .collect(toList());

        ImmutableSet.Builder<SelectItem> builder = ImmutableSet
                .<SelectItem>builder()
                .addAll(normalFields)
                .add(arrayAggField)
                .add(new SingleColumn(nameReference(SOURCE_REFERENCE, relationshipCTE.getBaseKey()), identifier(BASE_KEY_ALIAS)));
        List<SelectItem> selectItems = ImmutableList.copyOf(builder.build());

        List<Identifier> outputSchema = selectItems.stream()
                .map(selectItem -> (SingleColumn) selectItem)
                .map(singleColumn ->
                        singleColumn.getAlias()
                                .orElse(quotedIdentifier(singleColumn.getExpression().toString())))
                .collect(toList());

        Expression unnestField = outputField.orElse(nameReference(SOURCE_REFERENCE, manyResultField));

        return new WithQuery(identifier(relationshipCTE.getName()),
                simpleQuery(
                        new Select(false, selectItems),
                        leftJoin(
                                crossJoin(aliased(table(QualifiedName.of(relationshipCTE.getSource().getName())), SOURCE_REFERENCE),
                                        aliased(unnest(unnestField), UNNEST_REFERENCE, List.of(UNNEST_COLUMN_REFERENCE))),
                                aliased(table(QualifiedName.of(relationshipCTE.getTarget().getName())), TARGET_REFERENCE),
                                joinOn(equal(nameReference(UNNEST_REFERENCE, UNNEST_COLUMN_REFERENCE), nameReference(TARGET_REFERENCE, relationshipCTE.getTarget().getPrimaryKey())))),
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

    // TODO: find a way to combine slice to upper ctes https://github.com/Canner/canner-metric-layer/issues/302
    private WithQuery transferToSliceCte(
            String manyResultField,
            RelationshipCTE relationshipCTE,
            List<Expression> startEnd)
    {
        List<Expression> oneTableFields =
                ImmutableSet.<String>builder()
                        // make sure the primary key be first.
                        .add(relationshipCTE.getSource().getPrimaryKey())
                        // remove column name
                        .addAll(relationshipCTE.getSource().getColumns().stream()
                                .filter(column -> !column.equals(manyResultField) && !column.equals(LAMBDA_RESULT_NAME))
                                .collect(toList()))
                        .add(relationshipCTE.getSource().getJoinKey())
                        .build()
                        .stream()
                        .map(column -> nameReference(SOURCE_REFERENCE, column))
                        .collect(toList());

        SingleColumn sliceColumn = new SingleColumn(
                new FunctionCall(
                        QualifiedName.of("slice"),
                        List.of(
                                DereferenceExpression.from(QualifiedName.of(SOURCE_REFERENCE, manyResultField)),
                                startEnd.get(0),
                                startEnd.get(1))),
                identifier(LAMBDA_RESULT_NAME));

        List<SingleColumn> normalFields = oneTableFields
                .stream()
                .map(field -> new SingleColumn(field, identifier(requireNonNull(getQualifiedName(field)).getSuffix())))
                .collect(toList());

        ImmutableSet.Builder<SelectItem> builder = ImmutableSet
                .<SelectItem>builder()
                .addAll(normalFields)
                .add(sliceColumn)
                .add(new SingleColumn(nameReference(SOURCE_REFERENCE, relationshipCTE.getBaseKey()), identifier(BASE_KEY_ALIAS)));
        List<SelectItem> selectItems = ImmutableList.copyOf(builder.build());

        List<Identifier> outputSchema = selectItems.stream()
                .map(selectItem -> (SingleColumn) selectItem)
                .map(singleColumn ->
                        singleColumn.getAlias()
                                .orElse(quotedIdentifier(singleColumn.getExpression().toString())))
                .collect(toList());

        return new WithQuery(identifier(relationshipCTE.getName()),
                simpleQuery(
                        new Select(false, selectItems),
                        aliased(table(QualifiedName.of(relationshipCTE.getSource().getName())), SOURCE_REFERENCE),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
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

    private Expression toArrayAgg(Expression field, String sortKeyPrefix, List<Relationship.SortKey> sortKeys)
    {
        return new FunctionCall(
                Optional.empty(),
                QualifiedName.of("array_agg"),
                Optional.empty(),
                Optional.of(new IsNotNullPredicate(field)),
                Optional.of(new OrderBy(sortKeys.stream()
                        .map(sortKey ->
                                new SortItem(nameReference(sortKeyPrefix, sortKey.getName()), sortKey.isDescending() ? SortItem.Ordering.DESCENDING : SortItem.Ordering.ASCENDING,
                                        SortItem.NullOrdering.UNDEFINED))
                        .collect(toList()))),
                false,
                Optional.empty(),
                Optional.empty(),
                List.of(field));
    }

    private Expression toAggregate(Expression field, String operator)
    {
        return new FunctionCall(
                Optional.empty(),
                QualifiedName.of(operator),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
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
        String baseKey;
        // If the first item is CTE, the second one is RS or REVERSE_RS.
        if (rsItems.get(0).getType().equals(RsItem.Type.CTE)) {
            relationship = accioMDL.listRelationships().stream().filter(r -> r.getName().equals(rsItems.get(1).getName())).findAny().get();
            ComparisonExpression comparisonExpression = getConditionNode(relationship.getCondition());
            WithQuery leftQuery = registeredWithQuery.get(rsItems.get(0).getName());
            baseKey = BASE_KEY_ALIAS;

            if (rsItems.get(1).getType() == RS) {
                source = new RelationshipCTE.Relation(
                        leftQuery.getName().getValue(),
                        leftQuery.getColumnNames().get().stream().map(Identifier::getValue).collect(toList()),
                        // TODO: we should make sure the first field is its primary key.
                        leftQuery.getColumnNames().map(columns -> columns.get(0).getValue()).get(),
                        getReferenceField(comparisonExpression.getLeft()));
                Model rightModel = getRightModel(relationship, accioMDL.listModels());
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
                Model rightModel = getLeftModel(relationship, accioMDL.listModels());
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
                relationship = accioMDL.listRelationships().stream().filter(r -> r.getName().equals(rsItems.get(0).getName())).findAny().get();
                ComparisonExpression comparisonExpression = getConditionNode(relationship.getCondition());
                Model leftModel = getLeftModel(relationship, accioMDL.listModels());
                source = new RelationshipCTE.Relation(
                        leftModel.getName(),
                        leftModel.getColumns().stream().map(Column::getName).collect(toList()),
                        leftModel.getPrimaryKey(), getReferenceField(comparisonExpression.getLeft()));

                Model rightModel = getRightModel(relationship, accioMDL.listModels());
                target = new RelationshipCTE.Relation(
                        rightModel.getName(),
                        rightModel.getColumns().stream().map(Column::getName).collect(toList()),
                        rightModel.getPrimaryKey(), getReferenceField(comparisonExpression.getRight()));
            }
            else {
                relationship = accioMDL.listRelationships().stream().filter(r -> r.getName().equals(rsItems.get(0).getName())).findAny().get();
                ComparisonExpression comparisonExpression = getConditionNode(relationship.getCondition());
                // If it's a REVERSE relationship, the left and right side will be swapped.
                Model leftModel = getRightModel(relationship, accioMDL.listModels());
                source = new RelationshipCTE.Relation(
                        leftModel.getName(),
                        leftModel.getColumns().stream().map(Column::getName).collect(toList()),
                        // If it's a REVERSE relationship, the left and right side will be swapped.
                        leftModel.getPrimaryKey(), getReferenceField(comparisonExpression.getRight()));

                // If it's a REVERSE relationship, the left and right side will be swapped.
                Model rightModel = getLeftModel(relationship, accioMDL.listModels());
                target = new RelationshipCTE.Relation(
                        rightModel.getName(),
                        rightModel.getColumns().stream().map(Column::getName).collect(toList()),
                        // If it's a REVERSE relationship, the left and right side will be swapped.
                        rightModel.getPrimaryKey(), getReferenceField(comparisonExpression.getLeft()));
            }
            baseKey = source.getPrimaryKey();
        }

        return new RelationshipCTE("rs_" + randomTableSuffix(), source, target,
                getLast(rsItems).getType().equals(RS) ? relationship : Relationship.reverse(relationship),
                rsItems.get(0).getIndex().orElse(null), baseKey);
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
            return ((DereferenceExpression) expression).getField().orElseThrow().getValue();
        }

        return expression.toString();
    }

    public Map<String, WithQuery> getRegisteredWithQuery()
    {
        return registeredWithQuery;
    }

    public Map<String, String> getNameMapping()
    {
        return nameMapping;
    }

    public Map<String, RelationshipCTEJoinInfo> getRelationshipInfoMapping()
    {
        return relationshipInfoMapping;
    }

    public Map<String, RelationshipCTE> getRelationshipCTEs()
    {
        return registeredCte;
    }

    public static class RelationshipOperation
    {
        enum OperatorType
        {
            ACCESS,
            TRANSFORM,
            FILTER,
            AGGREGATE,
            ARRAY_SORT,
            SLICE,
        }

        public static RelationshipOperation access(List<RsItem> rsItems)
        {
            return new RelationshipOperation(rsItems, OperatorType.ACCESS, null, null, null, null, null);
        }

        public static RelationshipOperation transform(List<RsItem> rsItems, Expression lambdaExpression, String manySideResultField, Expression unnestField)
        {
            return new RelationshipOperation(rsItems, OperatorType.TRANSFORM, lambdaExpression, manySideResultField, unnestField, null, null);
        }

        public static RelationshipOperation filter(List<RsItem> rsItems, Expression lambdaExpression, String manySideResultField, Expression unnestField)
        {
            return new RelationshipOperation(rsItems, OperatorType.FILTER, lambdaExpression, manySideResultField, unnestField, null, null);
        }

        public static RelationshipOperation aggregate(List<RsItem> rsItems, String manySideResultField, String aggregateOperator)
        {
            return new RelationshipOperation(rsItems, OperatorType.AGGREGATE, null, manySideResultField, null, aggregateOperator, null);
        }

        public static RelationshipOperation arraySort(List<RsItem> rsItems, String manySideResultField, Expression unnestField, List<Expression> functionCallArguments)
        {
            return new RelationshipOperation(rsItems, OperatorType.ARRAY_SORT, null, manySideResultField, unnestField, null, functionCallArguments);
        }

        public static RelationshipOperation slice(List<RsItem> rsItems, String manySideResultField, List<Expression> functionCallArguments)
        {
            return new RelationshipOperation(rsItems, OperatorType.SLICE, null, manySideResultField, null, null, functionCallArguments);
        }

        private final List<RsItem> rsItems;
        private final OperatorType operatorType;
        private final Expression lambdaExpression;
        private final String manySideResultField;
        // for lambda cte generation
        private final Expression unnestField;
        private final String aggregateOperator;
        private final List<Expression> functionCallArguments;

        private RelationshipOperation(
                List<RsItem> rsItems,
                OperatorType operatorType,
                Expression lambdaExpression,
                String manySideResultField,
                Expression unnestField,
                String aggregateOperator,
                List<Expression> functionCallArguments)
        {
            this.rsItems = requireNonNull(rsItems);
            this.operatorType = requireNonNull(operatorType);
            this.lambdaExpression = lambdaExpression;
            this.manySideResultField = manySideResultField;
            this.unnestField = unnestField;
            this.aggregateOperator = aggregateOperator;
            this.functionCallArguments = functionCallArguments == null ? List.of() : functionCallArguments;
        }

        public List<RsItem> getRsItems()
        {
            return rsItems;
        }

        public OperatorType getOperatorType()
        {
            return operatorType;
        }

        public Optional<Expression> getLambdaExpression()
        {
            return Optional.ofNullable(lambdaExpression);
        }

        public Optional<String> getManySideResultField()
        {
            return Optional.ofNullable(manySideResultField);
        }

        public Optional<Expression> getUnnestField()
        {
            return Optional.ofNullable(unnestField);
        }

        public Optional<String> getAggregateOperator()
        {
            return Optional.ofNullable(aggregateOperator);
        }

        public List<Expression> getFunctionCallArguments()
        {
            return functionCallArguments;
        }
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
        private final String baseModelName;

        public RelationshipCTEJoinInfo(String cteName, JoinCriteria condition, String baseModelName)
        {
            this.cteName = cteName;
            this.condition = condition;
            this.baseModelName = baseModelName;
        }

        public String getCteName()
        {
            return cteName;
        }

        public JoinCriteria getCondition()
        {
            return condition;
        }

        public String getBaseModelName()
        {
            return baseModelName;
        }
    }
}
