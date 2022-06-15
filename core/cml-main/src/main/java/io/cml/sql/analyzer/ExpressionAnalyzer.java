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

package io.cml.sql.analyzer;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.cml.metadata.Metadata;
import io.cml.metadata.ResolvedFunction;
import io.cml.spi.CmlException;
import io.cml.spi.function.OperatorType;
import io.cml.spi.type.PGType;
import io.cml.sql.QualifiedObjectName;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.StackableAstVisitor;
import io.trino.sql.tree.StringLiteral;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static io.cml.spi.metadata.StandardErrorCode.TYPE_NOT_FOUND;
import static io.cml.spi.type.BigIntType.BIGINT;
import static io.cml.spi.type.IntegerType.INTEGER;
import static io.cml.spi.type.VarcharType.VARCHAR;
import static io.cml.sql.analyzer.ExpressionTreeUtils.extractLocation;
import static io.cml.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class ExpressionAnalyzer
{
    private final Map<NodeRef<Expression>, PGType<?>> expressionTypes = new LinkedHashMap<>();
    private final Multimap<QualifiedObjectName, String> tableColumnReferences = HashMultimap.create();
    private final List<Field> sourceFields = new ArrayList<>();
    private final Multimap<NodeRef<Node>, Field> referencedFields = HashMultimap.create();

    private final Map<NodeRef<Expression>, ResolvedField> columnReferences = new LinkedHashMap<>();

    private final Map<NodeRef<FunctionCall>, ResolvedFunction> resolvedFunctions = new LinkedHashMap<>();

    private final Function<Expression, PGType<?>> getPreanalyzedType;

    private final Metadata metadata;

    public ExpressionAnalyzer(Function<Expression, PGType<?>> getPreanalyzedType, Metadata metadata)
    {
        this.getPreanalyzedType = requireNonNull(getPreanalyzedType, "getPreanalyzedType is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public PGType<?> setExpressionType(Expression expression, PGType<?> type)
    {
        requireNonNull(expression, "expression cannot be null");
        requireNonNull(type, "type cannot be null");

        expressionTypes.put(NodeRef.of(expression), type);

        return type;
    }

    private PGType<?> getExpressionType(Expression expression)
    {
        requireNonNull(expression, "expression cannot be null");

        PGType<?> type = expressionTypes.get(NodeRef.of(expression));
        checkState(type != null, "Expression not yet analyzed: %s", expression);
        return type;
    }

    public Map<NodeRef<Expression>, PGType<?>> getExpressionTypes()
    {
        return unmodifiableMap(expressionTypes);
    }

    public PGType<?> analyze(Expression expression, Scope scope)
    {
        Visitor visitor = new Visitor(scope);
        return visitor.process(expression, new StackableAstVisitor.StackableAstVisitorContext<>(new Context(scope)));
    }

    private class Visitor
            extends StackableAstVisitor<PGType<?>, Context>
    {
        // Used to resolve FieldReferences (e.g. during local execution planning)
        private final Scope baseScope;

        public Visitor(Scope baseScope)
        {
            this.baseScope = baseScope;
        }

        @Override
        public PGType<?> process(Node node, @Nullable StackableAstVisitorContext<Context> context)
        {
            if (node instanceof Expression) {
                // don't double process a node
                PGType<?> type = expressionTypes.get(NodeRef.of(((Expression) node)));
                if (type != null) {
                    return type;
                }
            }
            return super.process(node, context);
        }

        @Override
        protected PGType<?> visitIdentifier(Identifier node, StackableAstVisitorContext<Context> context)
        {
            ResolvedField resolvedField = context.getContext().getScope().resolveField(node, QualifiedName.of(node.getValue()));
            return handleResolvedField(node, resolvedField, context);
        }

        private PGType<?> handleResolvedField(Expression node, ResolvedField resolvedField, StackableAstVisitorContext<Context> context)
        {
            FieldId fieldId = FieldId.from(resolvedField);
            Field field = resolvedField.getField();
            // if (context.getContext().isInLambda()) {
            //     LambdaArgumentDeclaration lambdaArgumentDeclaration = context.getContext().getFieldToLambdaArgumentDeclaration().get(fieldId);
            //     if (lambdaArgumentDeclaration != null) {
            //         // Lambda argument reference is not a column reference
            //         lambdaArgumentReferences.put(NodeRef.of((Identifier) node), lambdaArgumentDeclaration);
            //         return setExpressionType(node, field.getType());
            //     ㄋss=f}
            // }

            if (field.getOriginTable().isPresent() && field.getOriginColumnName().isPresent()) {
                // tableColumnReferences stores info about column links to which table in sql.
                // e.g. select name from nation. in tableColumnReferences will store <K,V> = <"nation", "name">
                // this map structure will be added to Analysis.tableColumnReferences which is used in
                // checkCanSelectFromColumns access control.
                tableColumnReferences.put(field.getOriginTable().get(), field.getOriginColumnName().get());
            }

            sourceFields.add(field);

            fieldId.getRelationId()
                    .getSourceNode()
                    .ifPresent(source -> referencedFields.put(NodeRef.of(source), field));

            ResolvedField previous = columnReferences.put(NodeRef.of(node), resolvedField);
            checkState(previous == null, "%s already known to refer to %s", node, previous);

            return setExpressionType(node, field.getType());
        }

        @Override
        protected PGType<?> visitFunctionCall(FunctionCall node, StackableAstVisitorContext<Context> context)
        {
            List<PGType<?>> argumentTypes = getCallArgumentTypes(node.getArguments(), context);

            ResolvedFunction function;
            try {
                function = metadata.resolveFunction(node.getName(), argumentTypes);
            }
            catch (CmlException e) {
                if (e.getLocation().isPresent()) {
                    // If analysis of any of the argument types (which is done lazily to deal with lambda
                    // expressions) fails, we want to report the original reason for the failure
                    throw e;
                }

                // otherwise, it must have failed due to a missing function or other reason, so we report an error at the
                // current location

                throw new CmlException(e::getErrorCode, extractLocation(node), e.getMessage(), e);
            }

            resolvedFunctions.put(NodeRef.of(node), function);

            PGType<?> type = function.getReturnType();
            return setExpressionType(node, type);
        }

        public List<PGType<?>> getCallArgumentTypes(List<Expression> arguments, StackableAstVisitorContext<Context> context)
        {
            ImmutableList.Builder<PGType<?>> argumentTypesBuilder = ImmutableList.builder();
            for (Expression argument : arguments) {
                argumentTypesBuilder.add(process(argument, context));
            }
            return argumentTypesBuilder.build();
        }

        @Override
        protected PGType<?> visitArithmeticBinary(ArithmeticBinaryExpression node, StackableAstVisitorContext<Context> context)
        {
            return getOperator(context, node, OperatorType.valueOf(node.getOperator().name()), node.getLeft(), node.getRight());
        }

        private PGType<?> getOperator(StackableAstVisitorContext<Context> context, Expression node, OperatorType operatorType, Expression... arguments)
        {
            ImmutableList.Builder<PGType<?>> argumentTypes = ImmutableList.builder();
            for (Expression expression : arguments) {
                argumentTypes.add(process(expression, context));
            }

            ResolvedFunction operation;
            operation = metadata.resolveOperator(operatorType, argumentTypes.build());

            PGType<?> type = operation.getReturnType();
            return setExpressionType(node, type);
        }

        @Override
        protected PGType<?> visitLongLiteral(LongLiteral node, StackableAstVisitorContext<Context> context)
        {
            if (node.getValue() >= Integer.MIN_VALUE && node.getValue() <= Integer.MAX_VALUE) {
                return setExpressionType(node, INTEGER);
            }

            return setExpressionType(node, BIGINT);
        }

        @Override
        protected PGType<?> visitComparisonExpression(ComparisonExpression node, StackableAstVisitorContext<Context> context)
        {
            OperatorType operatorType;
            switch (node.getOperator()) {
                case EQUAL:
                case NOT_EQUAL:
                    operatorType = OperatorType.EQUAL;
                    break;
                case LESS_THAN:
                case GREATER_THAN:
                    operatorType = OperatorType.LESS_THAN;
                    break;
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN_OR_EQUAL:
                    operatorType = OperatorType.LESS_THAN_OR_EQUAL;
                    break;
                case IS_DISTINCT_FROM:
                    operatorType = OperatorType.IS_DISTINCT_FROM;
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported comparison operator: " + node.getOperator());
            }
            return getOperator(context, node, operatorType, node.getLeft(), node.getRight());
        }

        @Override
        protected PGType<?> visitStringLiteral(StringLiteral node, StackableAstVisitorContext<Context> context)
        {
            return setExpressionType(node, VARCHAR);
        }

        @Override
        protected PGType<?> visitGenericLiteral(GenericLiteral node, StackableAstVisitorContext<Context> context)
        {
            PGType<?> type;
            try {
                type = metadata.fromSqlType(node.getType());
            }
            catch (IllegalArgumentException e) {
                throw semanticException(TYPE_NOT_FOUND, node, "Unknown type: %s", node.getType());
            }

            return setExpressionType(node, type);
        }
    }

    private static class Context
    {
        private final Scope scope;

        public Context(Scope scope)
        {
            this.scope = scope;
        }

        Scope getScope()
        {
            return scope;
        }
    }

    private static void updateAnalysis(Analysis analysis, ExpressionAnalyzer analyzer)
    {
        analysis.addTypes(analyzer.getExpressionTypes());
    }

    public static ExpressionAnalysis analyzeExpression(
            Metadata metadata,
            Scope scope,
            Analysis analysis,
            Expression expression)
    {
        ExpressionAnalyzer analyzer = create(analysis, metadata);
        analyzer.analyze(expression, scope);

        updateAnalysis(analysis, analyzer);
        // analysis.addExpressionFields(expression, analyzer.getSourceFields());

        return new ExpressionAnalysis(
                analyzer.getExpressionTypes(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of());
    }

    public static ExpressionAnalyzer create(Analysis analysis, Metadata metadata)
    {
        return new ExpressionAnalyzer(analysis::getType, metadata);
    }
}
