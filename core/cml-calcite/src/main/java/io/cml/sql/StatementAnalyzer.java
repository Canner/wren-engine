/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cml.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.cml.metadata.ColumnHandle;
import io.cml.metadata.ColumnSchema;
import io.cml.metadata.Metadata;
import io.cml.metadata.TableHandle;
import io.cml.metadata.TableSchema;
import io.cml.spi.type.DateType;
import io.cml.spi.type.PGType;
import io.cml.sql.analyzer.Analysis;
import io.cml.sql.analyzer.Analysis.GroupingSetAnalysis;
import io.cml.sql.analyzer.Analysis.SelectExpression;
import io.cml.sql.analyzer.ExpressionAnalysis;
import io.cml.sql.analyzer.ExpressionAnalyzer;
import io.cml.sql.analyzer.Field;
import io.cml.sql.analyzer.FieldId;
import io.cml.sql.analyzer.RelationId;
import io.cml.sql.analyzer.RelationType;
import io.cml.sql.analyzer.Scope;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Cube;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FieldReference;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GroupBy;
import io.trino.sql.tree.GroupingElement;
import io.trino.sql.tree.GroupingOperation;
import io.trino.sql.tree.GroupingSets;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Rollup;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SimpleGroupBy;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.DateString;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getLast;
import static io.cml.metadata.MetadataUtil.createQualifiedObjectName;
import static io.cml.spi.metadata.StandardErrorCode.INVALID_COLUMN_REFERENCE;
import static io.cml.spi.metadata.StandardErrorCode.MISSING_GROUP_BY;
import static io.cml.spi.metadata.StandardErrorCode.TOO_MANY_GROUPING_SETS;
import static io.cml.sql.analyzer.ExpressionTreeUtils.extractAggregateFunctions;
import static io.cml.sql.analyzer.ExpressionTreeUtils.extractExpressions;
import static io.cml.sql.analyzer.SemanticExceptions.semanticException;
import static java.lang.Math.toIntExact;
import static java.util.Collections.emptyList;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN_OR_EQUAL;

public final class StatementAnalyzer
{
    private Analysis analysis;
    private final Metadata metadata;

    public StatementAnalyzer(Metadata metadata)
    {
        this.metadata = metadata;
    }

    public Analysis analyze(Analysis analysis, RelOptCluster cluster, RelOptSchema relOptSchema, Statement statement)
    {
        this.analysis = analysis;
        Visitor visitor = new Visitor(cluster, relOptSchema);
        visitor.process(statement, Optional.empty());
        return analysis;
    }

    private final class Visitor
            extends AstVisitor<Scope, Optional<Scope>>
    {
        private final Optional<Scope> outerQueryScope = Optional.empty();
        private final RelBuilder relBuilder;

        Visitor(RelOptCluster relOptCluster, RelOptSchema relOptSchema)
        {
            this.relBuilder = RelFactories.LOGICAL_BUILDER.create(relOptCluster, relOptSchema);
        }

        public RelNode buildRelNode()
        {
            return relBuilder.build();
        }

        @Override
        public Scope process(Node node, Optional<Scope> scope)
        {
            Scope returnScope = super.process(node, scope);
            checkState(returnScope.getOuterQueryParent().equals(outerQueryScope), "result scope should have outer query scope equal with parameter outer query scope");
            if (scope.isPresent()) {
                checkState(hasScopeAsLocalParent(returnScope, scope.get()), "return scope should have context scope as one of its ancestors");
            }
            return returnScope;
        }

        private Scope process(Node node, Scope scope)
        {
            return process(node, Optional.of(scope));
        }

        @Override
        protected Scope visitNode(Node node, Optional<Scope> scope)
        {
            throw new IllegalStateException("Unsupported node type: " + node.getClass().getName());
        }

        @Override
        protected Scope visitTable(Table table, Optional<Scope> scope)
        {
            QualifiedObjectName name = createQualifiedObjectName(table, table.getName());

            Optional<TableHandle> tableHandle = metadata.getTableHandle(name);

            TableSchema tableSchema = metadata.getTableSchema(tableHandle.get());
            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(tableHandle.get());

            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            fields.addAll(analyzeTableOutputFields(table, name, tableSchema, columnHandles));

            List<Field> outputFields = fields.build();
            // analyzeFiltersAndMasks(table, name, tableHandle, outputFields, session.getIdentity().getUser());
            Scope tableScope = createAndAssignScope(table, scope, outputFields);
            relBuilder.scan(table.getName().getParts());
            return tableScope;
        }

        private List<Field> analyzeTableOutputFields(Table table, QualifiedObjectName tableName, TableSchema tableSchema, Map<String, ColumnHandle> columnHandles)
        {
            // Set<String> visibleColumnNames = getVisibleColumnNames(cannerTableName, tableSchema);
            // TODO: discover columns lazily based on where they are needed (to support connectors that can't enumerate all tables)
            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            for (ColumnSchema column : tableSchema.getColumns()) {
                Field field = Field.newQualified(
                        table.getName(),
                        Optional.of(column.getName()),
                        column.getType(),
                        column.isHidden(),
                        Optional.of(tableName),
                        Optional.of(column.getName()),
                        false);
                fields.add(field);
                ColumnHandle columnHandle = columnHandles.get(column.getName());
                checkArgument(columnHandle != null, "Unknown field %s", field);
                analysis.setColumn(field, columnHandle);
                analysis.addSourceColumns(field, ImmutableSet.of(new Analysis.SourceColumn(tableName, column.getName())));
            }
            return fields.build();
        }

        @Override
        protected Scope visitStatement(Statement node, Optional<Scope> scope)
        {
            analysis.setRoot(node);
            return null;
        }

        @Override
        protected Scope visitQuery(Query node, Optional<Scope> scope)
        {
            Scope withScope = analyzeWith(node, scope);
            Scope queryBodyScope = process(node.getQueryBody(), withScope);

            Scope queryScope = Scope.builder()
                    .withParent(withScope)
                    .withRelationType(RelationId.of(node), queryBodyScope.getRelationType())
                    .build();

            analysis.setScope(node, queryScope);
            return queryScope;
        }

        private Scope analyzeWith(Query node, Optional<Scope> scope)
        {
            if (node.getWith().isEmpty()) {
                return createScope(scope);
            }

            throw new UnsupportedOperationException("with clause");
        }

        @Override
        protected Scope visitQuerySpecification(QuerySpecification node, Optional<Scope> scope)
        {
            Scope sourceScope = analyzeFrom(node, scope);

            node.getWhere().ifPresent(where -> {
                analyzeWhere(node, sourceScope, where);
            });
            List<Expression> outputExpressions = analyzeSelect(node, sourceScope);
            GroupingSetAnalysis groupByAnalysis = analyzeGroupBy(node, sourceScope, outputExpressions);

            Scope outputScope = computeAndAssignOutputScope(node, scope, sourceScope);

            List<Expression> orderByExpressions = emptyList();
            Optional<Scope> orderByScope = Optional.empty();
            if (node.getOrderBy().isPresent()) {
                OrderBy orderBy = node.getOrderBy().get();
                orderByScope = Optional.of(computeAndAssignOrderByScope(orderBy, sourceScope, outputScope));

                orderByExpressions = analyzeOrderBy(node, orderBy.getSortItems(), orderByScope.get());
            }
            analysis.setOrderByExpressions(node, orderByExpressions);

            List<Expression> sourceExpressions = new ArrayList<>();
            analysis.getSelectExpressions(node).stream()
                    .map(SelectExpression::getExpression)
                    .forEach(sourceExpressions::add);

            analyzeGroupingOperations(node, sourceExpressions, orderByExpressions);
            analyzeAggregations(node, sourceScope, orderByScope, groupByAnalysis, sourceExpressions, orderByExpressions);

            if (analysis.isAggregation(node) && node.getOrderBy().isPresent()) {
                ImmutableList.Builder<Expression> aggregates = ImmutableList.<Expression>builder()
                        .addAll(groupByAnalysis.getOriginalExpressions())
                        .addAll(extractAggregateFunctions(orderByExpressions, metadata))
                        .addAll(extractExpressions(orderByExpressions, GroupingOperation.class));

                analysis.setOrderByAggregates(node.getOrderBy().get(), aggregates.build());
            }

            return outputScope;
        }

        private Scope computeAndAssignOutputScope(QuerySpecification node, Optional<Scope> scope, Scope sourceScope)
        {
            ImmutableList.Builder<Field> outputFields = ImmutableList.builder();

            for (SelectItem item : node.getSelect().getSelectItems()) {
                if (item instanceof SingleColumn) {
                    SingleColumn column = (SingleColumn) item;

                    Expression expression = column.getExpression();
                    Optional<Identifier> field = column.getAlias();

                    Optional<QualifiedObjectName> originTable = Optional.empty();
                    Optional<String> originColumn = Optional.empty();
                    QualifiedName name = null;

                    if (expression instanceof Identifier) {
                        name = QualifiedName.of(((Identifier) expression).getValue());
                    }
                    else if (expression instanceof DereferenceExpression) {
                        name = DereferenceExpression.getQualifiedName((DereferenceExpression) expression);
                    }

                    if (name != null) {
                        List<Field> matchingFields = sourceScope.getRelationType().resolveFields(name);
                        List<Field> visibleMatchingFields = matchingFields.stream()
                                .filter(input -> !input.isHidden())
                                .collect(toImmutableList());
                        if (matchingFields.size() > 1 && visibleMatchingFields.size() == 1) {
                            originTable = visibleMatchingFields.get(0).getOriginTable();
                            originColumn = visibleMatchingFields.get(0).getOriginColumnName();
                        }
                        else if (!matchingFields.isEmpty()) {
                            originTable = matchingFields.get(0).getOriginTable();
                            originColumn = matchingFields.get(0).getOriginColumnName();
                        }
                    }

                    if (field.isEmpty()) {
                        if (name != null) {
                            field = Optional.of(getLast(name.getOriginalParts()));
                        }
                    }

                    Field newField = Field.newUnqualified(field.map(Identifier::getValue), analysis.getType(expression), originTable, originColumn, column.getAlias().isPresent()); // TODO don't use analysis as a side-channel. Use outputExpressions to look up the type
                    if (originTable.isPresent()) {
                        analysis.addSourceColumns(newField, ImmutableSet.of(new Analysis.SourceColumn(originTable.get(), originColumn.orElseThrow())));
                    }
                    else {
                        analysis.addSourceColumns(newField, analysis.getExpressionSourceColumns(expression));
                    }
                    outputFields.add(newField);
                }
                else {
                    throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
                }
            }

            return createAndAssignScope(node, scope, outputFields.build());
        }

        private Scope analyzeFrom(QuerySpecification node, Optional<Scope> scope)
        {
            if (node.getFrom().isPresent()) {
                return process(node.getFrom().get(), scope);
            }

            Scope result = createScope(scope);
            analysis.setImplicitFromScope(node, result);
            return result;
        }

        private Scope analyzeWhere(Node node, Scope scope, Expression predicate)
        {
            // verifyNoAggregateWindowOrGroupingFunctions(metadata, predicate, "WHERE clause");

            ExpressionAnalysis expressionAnalysis = analyzeExpression(predicate, scope);

            analysis.setWhere(node, predicate);

            ComparisonExpression comparisonExpression = (ComparisonExpression) predicate;
            relBuilder.filter(relBuilder.call(expressionToRelOperator(comparisonExpression.getOperator()),
                    expressionToRexNode(comparisonExpression.getLeft(), scope),
                    expressionToRexNode(comparisonExpression.getRight(), scope)));
            return null;
        }

        private SqlBinaryOperator expressionToRelOperator(ComparisonExpression.Operator operator)
        {
            switch (operator) {
                case LESS_THAN_OR_EQUAL:
                    return LESS_THAN_OR_EQUAL;
            }
            throw new IllegalArgumentException();
        }

        private RexNode expressionToRexNode(Expression expression, Scope sourceScope)
        {
            if (expression instanceof Identifier) {
                Identifier identifier = (Identifier) expression;
                return relBuilder.field(1, getRelationNameFromScope(sourceScope).orElse(""), identifier.getValue());
            }
            else if (expression instanceof Literal) {
                Literal literal = (Literal) expression;
                PGType type = analysis.getType(literal);
                if (type.equals(DateType.DATE)) {
                    return relBuilder.getRexBuilder().makeDateLiteral(new DateString(literal.toString().split("'")[1]));
                }
                return relBuilder.literal(literal.toString());
            }
            throw new IllegalArgumentException();
        }

        private Optional<String> getRelationNameFromScope(Scope scope)
        {
            return scope.getRelationId().getSourceNode()
                    .filter(node -> node instanceof Table)
                    .map(table -> ((Table) table).getName().toString());
        }

        private List<Expression> analyzeSelect(QuerySpecification node, Scope scope)
        {
            ImmutableList.Builder<Expression> outputExpressionBuilder = ImmutableList.builder();
            ImmutableList.Builder<SelectExpression> selectExpressionBuilder = ImmutableList.builder();

            for (SelectItem item : node.getSelect().getSelectItems()) {
                if (item instanceof AllColumns) {
                    // TODO
                    // analyzeSelectAllColumns((AllColumns) item, node, scope, outputExpressionBuilder, selectExpressionBuilder);
                }
                else if (item instanceof SingleColumn) {
                    analyzeSelectSingleColumn((SingleColumn) item, node, scope, outputExpressionBuilder, selectExpressionBuilder);
                }
                else {
                    throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
                }
            }
            analysis.setSelectExpressions(node, selectExpressionBuilder.build());

            return outputExpressionBuilder.build();
        }

        private List<Expression> analyzeOrderBy(Node node, List<SortItem> sortItems, Scope orderByScope)
        {
            ImmutableList.Builder<Expression> orderByFieldsBuilder = ImmutableList.builder();

            for (SortItem item : sortItems) {
                Expression expression = item.getSortKey();

                if (expression instanceof LongLiteral) {
                    // this is an ordinal in the output tuple

                    long ordinal = ((LongLiteral) expression).getValue();
                    if (ordinal < 1 || ordinal > orderByScope.getRelationType().getVisibleFieldCount()) {
                        throw semanticException(INVALID_COLUMN_REFERENCE, expression, "ORDER BY position %s is not in select list", ordinal);
                    }

                    expression = new FieldReference(toIntExact(ordinal - 1));
                }

                ExpressionAnalysis expressionAnalysis = ExpressionAnalyzer.analyzeExpression(
                        metadata,
                        orderByScope,
                        analysis,
                        expression);
                orderByFieldsBuilder.add(expression);
            }

            return orderByFieldsBuilder.build();
        }

        private void analyzeSelectSingleColumn(
                SingleColumn singleColumn,
                QuerySpecification node,
                Scope scope,
                ImmutableList.Builder<Expression> outputExpressionBuilder,
                ImmutableList.Builder<SelectExpression> selectExpressionBuilder)
        {
            Expression expression = singleColumn.getExpression();
            analyzeExpression(expression, scope);
            outputExpressionBuilder.add(expression);
            selectExpressionBuilder.add(new SelectExpression(expression, Optional.empty()));
        }

        private GroupingSetAnalysis analyzeGroupBy(QuerySpecification node, Scope scope, List<Expression> outputExpressions)
        {
            if (node.getGroupBy().isPresent()) {
                ImmutableList.Builder<Set<FieldId>> cubes = ImmutableList.builder();
                ImmutableList.Builder<List<FieldId>> rollups = ImmutableList.builder();
                ImmutableList.Builder<List<Set<FieldId>>> sets = ImmutableList.builder();
                ImmutableList.Builder<Expression> complexExpressions = ImmutableList.builder();
                ImmutableList.Builder<Expression> groupingExpressions = ImmutableList.builder();

                checkGroupingSetsCount(node.getGroupBy().get());
                for (GroupingElement groupingElement : node.getGroupBy().get().getGroupingElements()) {
                    if (groupingElement instanceof SimpleGroupBy) {
                        for (Expression column : groupingElement.getExpressions()) {
                            // simple GROUP BY expressions allow ordinals or arbitrary expressions
                            if (column instanceof LongLiteral) {
                                long ordinal = ((LongLiteral) column).getValue();
                                if (ordinal < 1 || ordinal > outputExpressions.size()) {
                                    throw semanticException(INVALID_COLUMN_REFERENCE, column, "GROUP BY position %s is not in select list", ordinal);
                                }

                                column = outputExpressions.get(toIntExact(ordinal - 1));
                                // verifyNoAggregateWindowOrGroupingFunctions(metadata, column, "GROUP BY clause");
                            }
                            else {
                                // verifyNoAggregateWindowOrGroupingFunctions(metadata, column, "GROUP BY clause");
                                analyzeExpression(column, scope);
                            }

                            groupingExpressions.add(column);
                        }
                    }
                }

                List<Expression> expressions = groupingExpressions.build();

                GroupingSetAnalysis groupingSets = new GroupingSetAnalysis(expressions, cubes.build(), rollups.build(), sets.build(), complexExpressions.build());
                analysis.setGroupingSets(node, groupingSets);

                return groupingSets;
            }

            GroupingSetAnalysis result = new GroupingSetAnalysis(ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), ImmutableList.of());

            return result;
        }

        private void analyzeAggregations(
                QuerySpecification node,
                Scope sourceScope,
                Optional<Scope> orderByScope,
                GroupingSetAnalysis groupByAnalysis,
                List<Expression> outputExpressions,
                List<Expression> orderByExpressions)
        {
            checkState(orderByExpressions.isEmpty() || orderByScope.isPresent(), "non-empty orderByExpressions list without orderByScope provided");

            List<FunctionCall> aggregates = extractAggregateFunctions(Iterables.concat(outputExpressions, orderByExpressions), metadata);
            analysis.setAggregates(node, aggregates);
        }

        private void checkGroupingSetsCount(GroupBy node)
        {
            // If groupBy is distinct then crossProduct will be overestimated if there are duplicate grouping sets.
            int crossProduct = 1;
            for (GroupingElement element : node.getGroupingElements()) {
                try {
                    int product;
                    if (element instanceof SimpleGroupBy) {
                        product = 1;
                    }
                    else if (element instanceof Cube) {
                        int exponent = element.getExpressions().size();
                        if (exponent > 30) {
                            throw new ArithmeticException();
                        }
                        product = 1 << exponent;
                    }
                    else if (element instanceof Rollup) {
                        product = element.getExpressions().size() + 1;
                    }
                    else if (element instanceof GroupingSets) {
                        product = ((GroupingSets) element).getSets().size();
                    }
                    else {
                        throw new UnsupportedOperationException("Unsupported grouping element type: " + element.getClass().getName());
                    }
                    crossProduct = Math.multiplyExact(crossProduct, product);
                }
                catch (ArithmeticException e) {
                    // TODO: get from getMaxGroupingSets
                    throw semanticException(TOO_MANY_GROUPING_SETS, node,
                            "GROUP BY has more than %s grouping sets but can contain at most %s", Integer.MAX_VALUE, "0");
                }
            }
        }

        private void analyzeGroupingOperations(QuerySpecification node, List<Expression> outputExpressions, List<Expression> orderByExpressions)
        {
            List<GroupingOperation> groupingOperations = extractExpressions(Iterables.concat(outputExpressions, orderByExpressions), GroupingOperation.class);
            boolean isGroupingOperationPresent = !groupingOperations.isEmpty();

            if (isGroupingOperationPresent && node.getGroupBy().isEmpty()) {
                throw semanticException(
                        MISSING_GROUP_BY,
                        node,
                        "A GROUPING() operation can only be used with a corresponding GROUPING SET/CUBE/ROLLUP/GROUP BY clause");
            }

            analysis.setGroupingOperations(node, groupingOperations);
        }

        private ExpressionAnalysis analyzeExpression(Expression expression, Scope scope)
        {
            return ExpressionAnalyzer.analyzeExpression(
                    metadata,
                    scope,
                    analysis,
                    expression);
        }

        private Scope computeAndAssignOrderByScope(OrderBy node, Scope sourceScope, Scope outputScope)
        {
            // ORDER BY should "see" both output and FROM fields during initial analysis and non-aggregation query planning
            Scope orderByScope = Scope.builder()
                    .withParent(sourceScope)
                    .withRelationType(outputScope.getRelationId(), outputScope.getRelationType())
                    .build();
            analysis.setScope(node, orderByScope);
            return orderByScope;
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope)
        {
            return createAndAssignScope(node, parentScope, emptyList());
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, Field... fields)
        {
            return createAndAssignScope(node, parentScope, new RelationType(fields));
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, List<Field> fields)
        {
            return createAndAssignScope(node, parentScope, new RelationType(fields));
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, RelationType relationType)
        {
            Scope scope = scopeBuilder(parentScope)
                    .withRelationType(RelationId.of(node), relationType)
                    .build();

            analysis.setScope(node, scope);
            return scope;
        }

        private Scope createScope(Optional<Scope> parentScope)
        {
            return scopeBuilder(parentScope).build();
        }

        private Scope.Builder scopeBuilder(Optional<Scope> parentScope)
        {
            Scope.Builder scopeBuilder = Scope.builder();

            if (parentScope.isPresent()) {
                // parent scope represents local query scope hierarchy. Local query scope
                // hierarchy should have outer query scope as ancestor already.
                scopeBuilder.withParent(parentScope.get());
            }
            else {
                outerQueryScope.ifPresent(scopeBuilder::withOuterQueryParent);
            }

            return scopeBuilder;
        }
    }

    private static boolean hasScopeAsLocalParent(Scope root, Scope parent)
    {
        Scope scope = root;
        while (scope.getLocalParent().isPresent()) {
            scope = scope.getLocalParent().get();
            if (scope.equals(parent)) {
                return true;
            }
        }

        return false;
    }
}
