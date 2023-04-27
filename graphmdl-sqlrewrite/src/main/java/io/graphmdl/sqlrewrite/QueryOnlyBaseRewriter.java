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

import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.ArrayConstructor;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BindExpression;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Cube;
import io.trino.sql.tree.CurrentTime;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Except;
import io.trino.sql.tree.ExistsPredicate;
import io.trino.sql.tree.Extract;
import io.trino.sql.tree.Format;
import io.trino.sql.tree.FrameBound;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.FunctionRelation;
import io.trino.sql.tree.GroupBy;
import io.trino.sql.tree.GroupingSets;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.Intersect;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.JoinCriteria;
import io.trino.sql.tree.JoinOn;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.Lateral;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.LogicalBinaryExpression;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.QuantifiedComparisonExpression;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.Rollup;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SampledRelation;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.SimpleGroupBy;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.TryExpression;
import io.trino.sql.tree.Union;
import io.trino.sql.tree.Unnest;
import io.trino.sql.tree.Values;
import io.trino.sql.tree.WhenClause;
import io.trino.sql.tree.Window;
import io.trino.sql.tree.WindowFrame;
import io.trino.sql.tree.WindowReference;
import io.trino.sql.tree.WindowSpecification;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;

import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

public class QueryOnlyBaseRewriter<T>
        extends AstVisitor<Node, T>
{
    @Override
    protected Node visitNode(Node node, T context)
    {
        return node;
    }

    @Override
    protected Node visitQuery(Query node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new Query(
                    node.getLocation().get(),
                    node.getWith().map(expression -> visitAndCast(expression, context)),
                    visitAndCast(node.getQueryBody(), context),
                    node.getOrderBy().map(expression -> visitAndCast(expression, context)),
                    node.getOffset(),
                    node.getLimit());
        }
        return new Query(
                node.getWith().map(expression -> visitAndCast(expression, context)),
                visitAndCast(node.getQueryBody(), context),
                node.getOrderBy().map(expression -> visitAndCast(expression, context)),
                node.getOffset(),
                node.getLimit());
    }

    @Override
    protected Node visitCurrentTime(CurrentTime node, T context)
    {
        return super.visitCurrentTime(node, context);
    }

    @Override
    protected Node visitExtract(Extract node, T context)
    {
        if (node.getLocation().isPresent()) {
            new Extract(
                    node.getLocation().get(),
                    visitAndCast(node.getExpression(), context),
                    node.getField());
        }
        return new Extract(visitAndCast(node.getExpression(), context), node.getField());
    }

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
    protected Node visitIntersect(Intersect node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new Intersect(
                    node.getLocation().get(),
                    visitNodes(node.getRelations(), context),
                    node.isDistinct());
        }
        return new Intersect(visitNodes(node.getRelations(), context), node.isDistinct());
    }

    @Override
    protected Node visitExcept(Except node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new Except(
                    node.getLocation().get(),
                    visitAndCast(node.getLeft(), context),
                    visitAndCast(node.getRight(), context),
                    node.isDistinct());
        }
        return new Except(
                visitAndCast(node.getLeft(), context),
                visitAndCast(node.getRight(), context),
                node.isDistinct());
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
    protected Node visitUnnest(Unnest node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new Unnest(
                    node.getLocation().get(),
                    visitNodes(node.getExpressions(), context),
                    node.isWithOrdinality());
        }
        return new Unnest(visitNodes(node.getExpressions(), context), node.isWithOrdinality());
    }

    @Override
    protected Node visitFunctionRelation(FunctionRelation node, T context)
    {
        return new FunctionRelation(
                node.getLocation().orElse(null),
                node.getName(),
                node.getArguments());
    }

    @Override
    protected Node visitLateral(Lateral node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new Lateral(
                    node.getLocation().get(),
                    visitAndCast(node.getQuery(), context));
        }
        return new Lateral(visitAndCast(node.getQuery(), context));
    }

    @Override
    protected Node visitValues(Values node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new Values(
                    node.getLocation().get(),
                    visitNodes(node.getRows(), context));
        }
        return new Values(visitNodes(node.getRows(), context));
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
    protected Node visitSampledRelation(SampledRelation node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new SampledRelation(
                    node.getLocation().get(),
                    visitAndCast(node.getRelation(), context),
                    node.getType(),
                    visitAndCast(node.getSamplePercentage(), context));
        }
        return new SampledRelation(
                visitAndCast(node.getRelation(), context),
                node.getType(),
                visitAndCast(node.getSamplePercentage(), context));
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
                    node.getType(),
                    node.isSafe(),
                    node.isTypeOnly());
        }
        return new Cast(
                visitAndCast(node.getExpression(), context),
                node.getType(),
                node.isSafe(),
                node.isTypeOnly());
    }

    @Override
    protected Node visitWindowSpecification(WindowSpecification node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new WindowSpecification(
                    node.getLocation().get(),
                    node.getExistingWindowName(),
                    visitNodes(node.getPartitionBy(), context),
                    node.getOrderBy(),
                    node.getFrame().map(expression -> visitAndCast(expression, context)));
        }
        return new WindowSpecification(
                node.getExistingWindowName(),
                visitNodes(node.getPartitionBy(), context),
                node.getOrderBy(),
                node.getFrame().map(expression -> visitAndCast(expression, context)));
    }

    @Override
    protected Node visitWindowFrame(WindowFrame node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new WindowFrame(
                    node.getLocation().get(),
                    node.getType(),
                    visitAndCast(node.getStart(), context),
                    node.getEnd().map(expression -> visitAndCast(expression, context)),
                    node.getMeasures(),
                    node.getAfterMatchSkipTo(),
                    node.getPatternSearchMode(),
                    node.getPattern(),
                    node.getSubsets(),
                    node.getVariableDefinitions());
        }
        return new WindowFrame(
                node.getType(),
                visitAndCast(node.getStart(), context),
                node.getEnd().map(expression -> visitAndCast(expression, context)),
                node.getMeasures(),
                node.getAfterMatchSkipTo(),
                node.getPatternSearchMode(),
                node.getPattern(),
                node.getSubsets(),
                node.getVariableDefinitions());
    }

    @Override
    protected Node visitFrameBound(FrameBound node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new FrameBound(
                    node.getLocation().get(),
                    node.getType(),
                    node.getValue().map(expression -> visitAndCast(expression, context)).orElse(null));
        }
        return new FrameBound(
                node.getType(),
                node.getValue().map(expression -> visitAndCast(expression, context)).orElse(null));
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
    protected Node visitFormat(Format node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new Format(
                    node.getLocation().get(),
                    visitNodes(node.getArguments(), context));
        }
        return new Format(visitNodes(node.getArguments(), context));
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
    protected Node visitLogicalBinaryExpression(LogicalBinaryExpression node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new LogicalBinaryExpression(
                    node.getLocation().get(),
                    node.getOperator(),
                    visitAndCast(node.getLeft(), context),
                    visitAndCast(node.getRight(), context));
        }
        return new LogicalBinaryExpression(
                node.getOperator(),
                visitAndCast(node.getLeft(), context),
                visitAndCast(node.getRight(), context));
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
    protected Node visitQuerySpecification(QuerySpecification node, T context)
    {
        // Relations should be visited first for alias.
        Optional<Relation> from = node.getFrom().map(expression -> visitAndCast(expression, context));

        if (node.getLocation().isPresent()) {
            return new QuerySpecification(
                    node.getLocation().get(),
                    visitAndCast(node.getSelect(), context),
                    from,
                    node.getWhere().map(expression -> visitAndCast(expression, context)),
                    node.getGroupBy().map(expression -> visitAndCast(expression, context)),
                    node.getHaving().map(expression -> visitAndCast(expression, context)),
                    visitNodes(node.getWindows(), context),
                    node.getOrderBy().map(expression -> visitAndCast(expression, context)),
                    node.getOffset(),
                    node.getLimit());
        }
        return new QuerySpecification(
                visitAndCast(node.getSelect(), context),
                from,
                node.getWhere().map(expression -> visitAndCast(expression, context)),
                node.getGroupBy().map(expression -> visitAndCast(expression, context)),
                node.getHaving().map(expression -> visitAndCast(expression, context)),
                visitNodes(node.getWindows(), context),
                node.getOrderBy().map(expression -> visitAndCast(expression, context)),
                node.getOffset(),
                node.getLimit());
    }

    @Override
    protected Node visitJoin(Join node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new Join(
                    node.getLocation().get(),
                    node.getType(),
                    visitAndCast(node.getLeft(), context),
                    visitAndCast(node.getRight(), context),
                    node.getCriteria().map(joinCriteria -> visitJoinCriteria(joinCriteria, context)));
        }
        return new Join(
                node.getType(),
                visitAndCast(node.getLeft(), context),
                visitAndCast(node.getRight(), context),
                node.getCriteria().map(joinCriteria -> visitJoinCriteria(joinCriteria, context)));
    }

    protected JoinCriteria visitJoinCriteria(JoinCriteria joinCriteria, T context)
    {
        if (joinCriteria instanceof JoinOn) {
            JoinOn joinOn = (JoinOn) joinCriteria;
            return new JoinOn(visitAndCast(joinOn.getExpression(), context));
        }

        return joinCriteria;
    }

    @Override
    protected Node visitAliasedRelation(AliasedRelation node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new AliasedRelation(
                    node.getLocation().get(),
                    visitAndCast(node.getRelation(), context),
                    node.getAlias(),
                    node.getColumnNames());
        }
        return new AliasedRelation(
                visitAndCast(node.getRelation(), context),
                node.getAlias(),
                node.getColumnNames());
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
    protected Node visitTableSubquery(TableSubquery node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new TableSubquery(
                    node.getLocation().get(),
                    visitAndCast(node.getQuery(), context));
        }
        return new TableSubquery(visitAndCast(node.getQuery(), context));
    }

    @Override
    protected Node visitWith(With node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new With(
                    node.getLocation().get(),
                    node.isRecursive(),
                    visitNodes(node.getQueries(), context));
        }
        return new With(
                node.isRecursive(),
                visitNodes(node.getQueries(), context));
    }

    @Override
    protected Node visitWithQuery(WithQuery node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new WithQuery(
                    node.getLocation().get(),
                    node.getName(),
                    visitAndCast(node.getQuery(), context),
                    node.getColumnNames());
        }
        return new WithQuery(
                node.getName(),
                visitAndCast(node.getQuery(), context),
                node.getColumnNames());
    }

    @Override
    protected Node visitUnion(Union node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new Union(
                    node.getLocation().get(),
                    visitNodes(node.getRelations(), context),
                    node.isDistinct());
        }
        return new Union(
                visitNodes(node.getRelations(), context),
                node.isDistinct());
    }

    @Override
    protected Node visitSelect(Select node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new Select(
                    node.getLocation().get(),
                    node.isDistinct(),
                    visitNodes(node.getSelectItems(), context));
        }
        return new Select(
                node.isDistinct(),
                visitNodes(node.getSelectItems(), context));
    }

    @Override
    protected Node visitGroupBy(GroupBy node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new GroupBy(
                    node.getLocation().get(),
                    node.isDistinct(),
                    visitNodes(node.getGroupingElements(), context));
        }
        return new GroupBy(node.isDistinct(), visitNodes(node.getGroupingElements(), context));
    }

    @Override
    protected Node visitCube(Cube node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new Cube(
                    node.getLocation().get(),
                    visitNodes(node.getExpressions(), context));
        }
        return new Cube(visitNodes(node.getExpressions(), context));
    }

    @Override
    protected Node visitGroupingSets(GroupingSets node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new GroupingSets(
                    node.getLocation().get(),
                    node.getSets().stream()
                            .map(expressions -> visitNodes(expressions, context))
                            .collect(toList()));
        }
        return new GroupingSets(
                node.getSets().stream()
                        .map(expressions -> visitNodes(expressions, context))
                        .collect(toList()));
    }

    @Override
    protected Node visitSimpleGroupBy(SimpleGroupBy node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new SimpleGroupBy(
                    node.getLocation().get(),
                    visitNodes(node.getExpressions(), context));
        }
        return new SimpleGroupBy(visitNodes(node.getExpressions(), context));
    }

    @Override
    protected Node visitRollup(Rollup node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new Rollup(
                    node.getLocation().get(),
                    visitNodes(node.getExpressions(), context));
        }
        return new Rollup(visitNodes(node.getExpressions(), context));
    }

    @Override
    protected Node visitOrderBy(OrderBy node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new OrderBy(
                    node.getLocation().get(),
                    visitNodes(node.getSortItems(), context));
        }
        return new OrderBy(visitNodes(node.getSortItems(), context));
    }

    @Override
    protected Node visitSortItem(SortItem node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new SortItem(
                    node.getLocation().get(),
                    visitAndCast(node.getSortKey(), context),
                    node.getOrdering(),
                    node.getNullOrdering());
        }
        return new SortItem(
                visitAndCast(node.getSortKey(), context),
                node.getOrdering(),
                node.getNullOrdering());
    }

    @Override
    protected Node visitSingleColumn(SingleColumn node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new SingleColumn(
                    node.getLocation().get(),
                    visitAndCast(node.getExpression(), context),
                    node.getAlias());
        }
        return new SingleColumn(
                visitAndCast(node.getExpression(), context),
                node.getAlias());
    }

    @Override
    protected Node visitDereferenceExpression(DereferenceExpression node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new DereferenceExpression(
                    node.getLocation().get(),
                    node.getBase(),
                    node.getField());
        }
        return new DereferenceExpression(
                node.getBase(),
                node.getField());
    }

    @Override
    protected Node visitTable(Table node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new Table(
                    node.getLocation().get(),
                    node.getName());
        }
        return new Table(node.getName());
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
