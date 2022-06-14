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

package io.cml.sql;

import com.google.common.collect.ImmutableList;
import io.cml.metadata.Metadata;
import io.cml.spi.type.DateType;
import io.cml.spi.type.IntegerType;
import io.cml.spi.type.PGType;
import io.cml.sql.analyzer.Analysis;
import io.cml.sql.analyzer.Field;
import io.cml.sql.analyzer.RelationType;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Relation;
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

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTIPLY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS;

public class LogicalPlanner
{
    private final Analysis analysis;
    private final RelOptCluster cluster;
    private final RelOptSchema relOptSchema;

    private int sumCount = 1;
    private int avgCount = 1;
    private int countCount = 1;

    private final Metadata metadata;

    public LogicalPlanner(Analysis analysis, RelOptCluster cluster, RelOptSchema relOptSchema, Metadata metadata)
    {
        this.analysis = analysis;
        this.cluster = cluster;
        this.relOptSchema = relOptSchema;
        this.metadata = metadata;
    }

    public RelNode plan(Statement statement)
    {
        LogicalPlanner.Visitor visitor = new LogicalPlanner.Visitor(cluster, relOptSchema);
        return visitor.process(statement);
    }

    private final class Visitor
            extends AstVisitor<RelNode, Void>
    {
        private final RelBuilder relBuilder;

        Visitor(RelOptCluster relOptCluster, RelOptSchema relOptSchema)
        {
            this.relBuilder = RelFactories.LOGICAL_BUILDER.create(relOptCluster, relOptSchema);
        }

        @Override
        protected RelNode visitNode(Node node, Void context)
        {
            throw new IllegalStateException("Unsupported node type: " + node.getClass().getName());
        }

        @Override
        protected RelNode visitStatement(Statement node, Void context)
        {
            analysis.setRoot(node);
            return null;
        }

        @Override
        protected RelNode visitQuery(Query node, Void context)
        {
            return process(node.getQueryBody());
        }

        @Override
        protected RelNode visitQuerySpecification(QuerySpecification node, Void context)
        {
            Relation sourceNode = node.getFrom().get();
            node.getFrom().ifPresent(from -> planFrom(from, node));
            node.getWhere().ifPresent(where -> {
                planWhere(where, sourceNode);
            });
            planOrderByAggregate(node);
            relBuilder.rename(createOutput());
            return relBuilder.build();
        }

        private void planFrom(Relation from, Node source)
        {
            if (from instanceof Table) {
                relBuilder.scan(((Table) from).getName().getParts());
                // TODO: need to distinct required field
                projectRequiredField(from);
                return;
            }
            throw new IllegalArgumentException();
        }

        private void projectRequiredField(Node source)
        {
            List<RexNode> nodes = analysis.getTypes().keySet().stream().filter(pgType -> pgType.getNode() instanceof Identifier)
                    .map(NodeRef::getNode).distinct().map(expression -> expressionToRexNode(expression, source))
                    .collect(toImmutableList());
            relBuilder.project(nodes);
        }

        private void planWhere(Expression predicate, Node source)
        {
            ComparisonExpression comparisonExpression = (ComparisonExpression) predicate;
            relBuilder.filter(relBuilder.call(expressionToRelOperator(comparisonExpression.getOperator()),
                    expressionToRexNode(comparisonExpression.getLeft(), source),
                    expressionToRexNode(comparisonExpression.getRight(), source)));
        }

        private void planOrderByAggregate(QuerySpecification node)
        {
            List<RexNode> groupKey = analysis.getGroupingSet(node).getOriginalExpressions().stream().map(expression -> expressionToRexNode(expression, node.getFrom().get()))
                    .collect(toImmutableList());

            List<RelBuilder.AggCall> functionCalls = analysis
                    .getAggregates(node).stream().map(functionCall -> functionCallToAggCall(functionCall, node))
                    .collect(toImmutableList());
            relBuilder.aggregate(relBuilder.groupKey(groupKey), functionCalls);

            if (node.getOrderBy().isPresent()) {
                List<RexNode> orderBy = analysis.getOrderByAggregates(node.getOrderBy().get()).stream()
                        .map(expression -> expressionToRexNode(expression, node.getFrom().get())).collect(toImmutableList());
                relBuilder.sort(orderBy);
            }
        }

        private List<String> createOutput()
        {
            ImmutableList.Builder<String> names = ImmutableList.builder();

            int columnNumber = 0;
            RelationType outputDescriptor = analysis.getOutputDescriptor();
            for (Field field : outputDescriptor.getVisibleFields()) {
                String name = field.getName().orElse("_col" + columnNumber);
                names.add(name);
                columnNumber++;
            }

            return names.build();
        }

        private SqlBinaryOperator expressionToRelOperator(ComparisonExpression.Operator operator)
        {
            switch (operator) {
                case LESS_THAN_OR_EQUAL:
                    return LESS_THAN_OR_EQUAL;
            }
            throw new IllegalArgumentException();
        }

        private SqlBinaryOperator trinoArithmeticBinaryOperatorToRelOperator(ArithmeticBinaryExpression.Operator operator)
        {
            switch (operator) {
                case MULTIPLY:
                    return MULTIPLY;
                case SUBTRACT:
                    return MINUS;
                case ADD:
                    return PLUS;
            }
            throw new UnsupportedOperationException("Unsupported operator: " + operator);
        }

        private RexNode expressionToRexNode(Expression expression, Node source)
        {
            if (expression instanceof Identifier) {
                Identifier identifier = (Identifier) expression;
                return relBuilder.field(identifier.getValue());
            }
            else if (expression instanceof Literal) {
                Literal literal = (Literal) expression;
                PGType type = analysis.getType(literal);
                if (type.equals(DateType.DATE)) {
                    return relBuilder.getRexBuilder().makeDateLiteral(new DateString(literal.toString().split("'")[1]));
                }
                else if (type.equals(IntegerType.INTEGER)) {
                    return relBuilder.literal(Integer.valueOf(literal.toString()));
                }
                return relBuilder.literal(literal.toString());
            }
            else if (expression instanceof ArithmeticBinaryExpression) {
                ArithmeticBinaryExpression arithmeticBinaryExpression = (ArithmeticBinaryExpression) expression;
                return relBuilder.call(
                        trinoArithmeticBinaryOperatorToRelOperator(arithmeticBinaryExpression.getOperator()),
                        expressionToRexNode(arithmeticBinaryExpression.getLeft(), source),
                        expressionToRexNode(arithmeticBinaryExpression.getRight(), source));
            }
            throw new IllegalArgumentException();
        }

        private Optional<String> getRelationNameFromScope(Node source)
        {
            return analysis.tryGetScope(source).map(scope ->
                    scope.getRelationId().getSourceNode()
                            .filter(node -> node instanceof Table)
                            .map(table -> ((Table) table).getName().toString()).orElse(""));
        }

        private RelBuilder.AggCall functionCallToAggCall(FunctionCall functionCall, Node source)
        {
            Node from = ((QuerySpecification) source).getFrom().get();
            switch (functionCall.getName().toString()) {
                case "sum":
                    return relBuilder.sum(false, "sum_" + sumCount++, expressionToRexNode(functionCall.getArguments().get(0), from));
                case "avg":
                    return relBuilder.avg(false, "avg_" + avgCount++, expressionToRexNode(functionCall.getArguments().get(0), from));
                case "count":
                    return relBuilder.count(false, "count_" + countCount++);
            }
            throw new IllegalArgumentException();
        }
    }
}
