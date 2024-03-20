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

import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.ArrayConstructor;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BindExpression;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CharLiteral;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.DataTypeParameter;
import io.trino.sql.tree.DateTimeDataType;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.ExistsPredicate;
import io.trino.sql.tree.FieldReference;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.IntervalDayTimeDataType;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.NumericParameter;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.QuantifiedComparisonExpression;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.RowDataType;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.TimeLiteral;
import io.trino.sql.tree.TimestampLiteral;
import io.trino.sql.tree.TryExpression;
import io.trino.sql.tree.TypeParameter;
import io.trino.sql.tree.WhenClause;
import io.trino.sql.tree.Window;
import io.trino.sql.tree.WindowReference;
import io.trino.sql.tree.WindowSpecification;

import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

public class BaseRewriter<T>
        extends BaseTreeRewriter<T>
{
    @Override
    protected Node visitCoalesceExpression(CoalesceExpression node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new CoalesceExpression(
                    node.getLocation().get(),
                    visitNodes(node.getOperands(), context));
        }
        return new CoalesceExpression(visitNodes(node.getOperands(), context));
    }

    @Override
    protected Node visitLiteral(Literal node, T context)
    {
        return super.visitLiteral(node, context);
    }

    @Override
    protected Node visitDoubleLiteral(DoubleLiteral node, T context)
    {
        return super.visitDoubleLiteral(node, context);
    }

    @Override
    protected Node visitDecimalLiteral(DecimalLiteral node, T context)
    {
        return super.visitDecimalLiteral(node, context);
    }

    @Override
    protected Node visitGenericLiteral(GenericLiteral node, T context)
    {
        return super.visitGenericLiteral(node, context);
    }

    @Override
    protected Node visitTimeLiteral(TimeLiteral node, T context)
    {
        return super.visitTimeLiteral(node, context);
    }

    @Override
    protected Node visitTimestampLiteral(TimestampLiteral node, T context)
    {
        return super.visitTimestampLiteral(node, context);
    }

    @Override
    protected Node visitWhenClause(WhenClause node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new WhenClause(
                    node.getLocation().get(),
                    visitAndCast(node.getOperand(), context),
                    visitAndCast(node.getResult(), context));
        }
        return new WhenClause(
                visitAndCast(node.getOperand(), context),
                visitAndCast(node.getResult(), context));
    }

    @Override
    protected Node visitIntervalLiteral(IntervalLiteral node, T context)
    {
        return super.visitIntervalLiteral(node, context);
    }

    @Override
    protected Node visitLambdaExpression(LambdaExpression node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new LambdaExpression(
                    node.getLocation().get(),
                    node.getArguments(),
                    visitAndCast(node.getBody(), context));
        }
        return new LambdaExpression(
                node.getArguments(),
                visitAndCast(node.getBody(), context));
    }

    @Override
    protected Node visitSimpleCaseExpression(SimpleCaseExpression node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new SimpleCaseExpression(
                    node.getLocation().get(),
                    visitAndCast(node.getOperand(), context),
                    visitNodes(node.getWhenClauses(), context),
                    node.getDefaultValue().map(expression -> visitAndCast(expression, context)));
        }
        return new SimpleCaseExpression(
                visitAndCast(node.getOperand(), context),
                visitNodes(node.getWhenClauses(), context),
                node.getDefaultValue().map(expression -> visitAndCast(expression, context)));
    }

    @Override
    protected Node visitStringLiteral(StringLiteral node, T context)
    {
        return super.visitStringLiteral(node, context);
    }

    @Override
    protected Node visitCharLiteral(CharLiteral node, T context)
    {
        return super.visitCharLiteral(node, context);
    }

    @Override
    protected Node visitBinaryLiteral(BinaryLiteral node, T context)
    {
        return super.visitBinaryLiteral(node, context);
    }

    @Override
    protected Node visitBooleanLiteral(BooleanLiteral node, T context)
    {
        return super.visitBooleanLiteral(node, context);
    }

    @Override
    protected Node visitInListExpression(InListExpression node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new InListExpression(
                    node.getLocation().get(),
                    visitNodes(node.getValues(), context));
        }
        return new InListExpression(visitNodes(node.getValues(), context));
    }

    @Override
    protected Node visitIdentifier(Identifier node, T context)
    {
        return super.visitIdentifier(node, context);
    }

    @Override
    protected Node visitNullIfExpression(NullIfExpression node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new NullIfExpression(
                    node.getLocation().get(),
                    visitAndCast(node.getFirst(), context),
                    visitAndCast(node.getSecond(), context));
        }
        return new NullIfExpression(
                visitAndCast(node.getFirst(), context),
                visitAndCast(node.getSecond(), context));
    }

    @Override
    protected Node visitIfExpression(IfExpression node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new IfExpression(
                    node.getLocation().get(),
                    visitAndCast(node.getCondition(), context),
                    visitAndCast(node.getTrueValue(), context),
                    node.getFalseValue().map(expression -> visitAndCast(expression, context)).orElse(null));
        }
        return new IfExpression(
                visitAndCast(node.getCondition(), context),
                visitAndCast(node.getTrueValue(), context),
                node.getFalseValue().map(expression -> visitAndCast(expression, context)).orElse(null));
    }

    @Override
    protected Node visitNullLiteral(NullLiteral node, T context)
    {
        return super.visitNullLiteral(node, context);
    }

    @Override
    protected Node visitArithmeticUnary(ArithmeticUnaryExpression node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new ArithmeticUnaryExpression(
                    node.getLocation().get(),
                    node.getSign(),
                    visitAndCast(node.getValue(), context));
        }
        return new ArithmeticUnaryExpression(node.getSign(), visitAndCast(node.getValue(), context));
    }

    @Override
    protected Node visitSearchedCaseExpression(SearchedCaseExpression node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new SearchedCaseExpression(
                    node.getLocation().get(),
                    visitNodes(node.getWhenClauses(), context),
                    node.getDefaultValue().map(expression -> visitAndCast(expression, context)));
        }
        return new SearchedCaseExpression(
                visitNodes(node.getWhenClauses(), context),
                node.getDefaultValue().map(expression -> visitAndCast(expression, context)));
    }

    @Override
    protected Node visitLikePredicate(LikePredicate node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new LikePredicate(
                    node.getLocation().get(),
                    visitAndCast(node.getValue(), context),
                    visitAndCast(node.getPattern(), context),
                    node.getEscape().map(expression -> visitAndCast(expression, context)));
        }
        return new LikePredicate(
                visitAndCast(node.getValue(), context),
                visitAndCast(node.getPattern(), context),
                node.getEscape().map(expression -> visitAndCast(expression, context)));
    }

    @Override
    protected Node visitIsNotNullPredicate(IsNotNullPredicate node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new IsNotNullPredicate(
                    node.getLocation().get(),
                    visitAndCast(node.getValue(), context));
        }
        return new IsNotNullPredicate(visitAndCast(node.getValue(), context));
    }

    @Override
    protected Node visitIsNullPredicate(IsNullPredicate node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new IsNullPredicate(
                    node.getLocation().get(),
                    visitAndCast(node.getValue(), context));
        }
        return new IsNullPredicate(visitAndCast(node.getValue(), context));
    }

    @Override
    protected Node visitArrayConstructor(ArrayConstructor node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new ArrayConstructor(
                    node.getLocation().get(),
                    visitNodes(node.getValues(), context));
        }
        return new ArrayConstructor(visitNodes(node.getValues(), context));
    }

    @Override
    protected Node visitLogicalExpression(LogicalExpression node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new LogicalExpression(
                    node.getLocation().get(),
                    node.getOperator(),
                    visitNodes(node.getTerms(), context));
        }
        return new LogicalExpression(
                node.getOperator(),
                visitNodes(node.getTerms(), context));
    }

    @Override
    protected Node visitSubscriptExpression(SubscriptExpression node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new SubscriptExpression(
                    node.getLocation().get(),
                    visitAndCast(node.getBase(), context),
                    visitAndCast(node.getIndex(), context));
        }
        return new SubscriptExpression(visitAndCast(node.getBase(), context), visitAndCast(node.getIndex(), context));
    }

    @Override
    protected Node visitLongLiteral(LongLiteral node, T context)
    {
        return super.visitLongLiteral(node, context);
    }

    @Override
    protected Node visitParameter(Parameter node, T context)
    {
        return super.visitParameter(node, context);
    }

    @Override
    protected Node visitRow(Row node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new Row(
                    node.getLocation().get(),
                    visitNodes(node.getItems(), context));
        }
        return new Row(visitNodes(node.getItems(), context));
    }

    @Override
    protected Node visitTryExpression(TryExpression node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new TryExpression(
                    node.getLocation().get(),
                    visitAndCast(node.getInnerExpression(), context));
        }
        return new TryExpression(visitAndCast(node.getInnerExpression(), context));
    }

    @Override
    protected Node visitCast(Cast node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new Cast(
                    node.getLocation().get(),
                    visitAndCast(node.getExpression(), context),
                    visitAndCast(node.getType(), context),
                    node.isSafe(),
                    node.isTypeOnly());
        }
        return new Cast(
                visitAndCast(node.getExpression(), context),
                visitAndCast(node.getType(), context),
                node.isSafe(),
                node.isTypeOnly());
    }

    @Override
    protected Node visitFieldReference(FieldReference node, T context)
    {
        return super.visitFieldReference(node, context);
    }

    @Override
    protected Node visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new QuantifiedComparisonExpression(
                    node.getLocation().get(),
                    node.getOperator(),
                    node.getQuantifier(),
                    visitAndCast(node.getValue(), context),
                    visitAndCast(node.getSubquery(), context));
        }
        return new QuantifiedComparisonExpression(
                node.getOperator(),
                node.getQuantifier(),
                visitAndCast(node.getValue(), context),
                visitAndCast(node.getSubquery(), context));
    }

    @Override
    protected Node visitLambdaArgumentDeclaration(LambdaArgumentDeclaration node, T context)
    {
        return super.visitLambdaArgumentDeclaration(node, context);
    }

    @Override
    protected Node visitBindExpression(BindExpression node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new BindExpression(
                    node.getLocation().get(),
                    visitNodes(node.getValues(), context),
                    visitAndCast(node.getFunction(), context));
        }
        return new BindExpression(visitNodes(node.getValues(), context), visitAndCast(node.getFunction(), context));
    }

    @Override
    protected Node visitDataType(DataType node, T context)
    {
        return super.visitDataType(node, context);
    }

    @Override
    protected Node visitRowDataType(RowDataType node, T context)
    {
        return super.visitRowDataType(node, context);
    }

    @Override
    protected Node visitGenericDataType(GenericDataType node, T context)
    {
        return super.visitGenericDataType(node, context);
    }

    @Override
    protected Node visitRowField(RowDataType.Field node, T context)
    {
        return super.visitRowField(node, context);
    }

    @Override
    protected Node visitDataTypeParameter(DataTypeParameter node, T context)
    {
        return super.visitDataTypeParameter(node, context);
    }

    @Override
    protected Node visitNumericTypeParameter(NumericParameter node, T context)
    {
        return super.visitNumericTypeParameter(node, context);
    }

    @Override
    protected Node visitTypeParameter(TypeParameter node, T context)
    {
        return super.visitTypeParameter(node, context);
    }

    @Override
    protected Node visitIntervalDataType(IntervalDayTimeDataType node, T context)
    {
        return super.visitIntervalDataType(node, context);
    }

    @Override
    protected Node visitDateTimeType(DateTimeDataType node, T context)
    {
        return super.visitDateTimeType(node, context);
    }

    @Override
    protected Node visitInPredicate(InPredicate node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new InPredicate(
                    node.getLocation().get(),
                    visitAndCast(node.getValue(), context),
                    visitAndCast(node.getValueList(), context));
        }
        return new InPredicate(
                visitAndCast(node.getValue(), context),
                visitAndCast(node.getValueList(), context));
    }

    @Override
    protected Node visitComparisonExpression(ComparisonExpression node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new ComparisonExpression(
                    node.getLocation().get(),
                    node.getOperator(),
                    visitAndCast(node.getLeft(), context),
                    visitAndCast(node.getRight(), context));
        }
        return new ComparisonExpression(
                node.getOperator(),
                visitAndCast(node.getLeft(), context),
                visitAndCast(node.getRight(), context));
    }

    @Override
    protected Node visitExists(ExistsPredicate node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new ExistsPredicate(
                    node.getLocation().get(),
                    visitAndCast(node.getSubquery(), context));
        }
        return new ExistsPredicate(visitAndCast(node.getSubquery(), context));
    }

    @Override
    protected Node visitNotExpression(NotExpression node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new NotExpression(
                    node.getLocation().get(),
                    visitAndCast(node.getValue(), context));
        }
        return new NotExpression(visitAndCast(node.getValue(), context));
    }

    @Override
    protected Node visitArithmeticBinary(ArithmeticBinaryExpression node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new ArithmeticBinaryExpression(
                    node.getLocation().get(),
                    node.getOperator(),
                    visitAndCast(node.getLeft(), context),
                    visitAndCast(node.getRight(), context));
        }
        return new ArithmeticBinaryExpression(
                node.getOperator(),
                visitAndCast(node.getLeft(), context),
                visitAndCast(node.getRight(), context));
    }

    @Override
    protected Node visitBetweenPredicate(BetweenPredicate node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new BetweenPredicate(
                    node.getLocation().get(),
                    visitAndCast(node.getValue(), context),
                    visitAndCast(node.getMin(), context),
                    visitAndCast(node.getMax(), context));
        }
        return new BetweenPredicate(
                visitAndCast(node.getValue(), context),
                visitAndCast(node.getMin(), context),
                visitAndCast(node.getMax(), context));
    }

    @Override
    protected Node visitFunctionCall(FunctionCall node, T context)
    {
        return new FunctionCall(
                node.getLocation(),
                node.getName(),
                node.getWindow().map(expression -> visitAndCast(expression, context)),
                node.getFilter().map(expression -> visitAndCast(expression, context)),
                node.getOrderBy().map(expression -> visitAndCast(expression, context)),
                node.isDistinct(),
                node.getNullTreatment(),
                Optional.empty(),
                visitNodes(node.getArguments(), context));
    }

    @Override
    protected Node visitSubqueryExpression(SubqueryExpression node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new SubqueryExpression(
                    node.getLocation().get(),
                    visitAndCast(node.getQuery(), context));
        }
        return new SubqueryExpression(visitAndCast(node.getQuery(), context));
    }

    @Override
    protected Node visitDereferenceExpression(DereferenceExpression node, T context)
    {
        return new DereferenceExpression(
                node.getLocation(),
                visitAndCast(node.getBase(), context),
                node.getField());
    }

    protected <S extends Node> S visitAndCast(S node, T context)
    {
        return (S) process(node, context);
    }

    protected <S extends Window> S visitAndCast(S window, T context)
    {
        Node node = null;
        if (window instanceof WindowSpecification) {
            node = (WindowSpecification) window;
        }
        else if (window instanceof WindowReference) {
            node = (WindowReference) window;
        }
        return (S) process(node, context);
    }

    @SuppressWarnings("unchecked")
    protected <S extends Node> List<S> visitNodes(List<S> nodes, T context)
    {
        return nodes.stream()
                .map(node -> (S) process(node, context))
                .collect(toList());
    }
}
