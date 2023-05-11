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

package io.graphmdl.main.wireprotocol;

import io.trino.sql.tree.AddColumn;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.Analyze;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.ArrayConstructor;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.AtTimeZone;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BindExpression;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Call;
import io.trino.sql.tree.CallArgument;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CharLiteral;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.Comment;
import io.trino.sql.tree.Commit;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.CreateRole;
import io.trino.sql.tree.CreateSchema;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.CreateTableAsSelect;
import io.trino.sql.tree.CreateView;
import io.trino.sql.tree.Cube;
import io.trino.sql.tree.CurrentPath;
import io.trino.sql.tree.CurrentTime;
import io.trino.sql.tree.CurrentUser;
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.DataTypeParameter;
import io.trino.sql.tree.DateTimeDataType;
import io.trino.sql.tree.Deallocate;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.Delete;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.DescribeInput;
import io.trino.sql.tree.DescribeOutput;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.DropColumn;
import io.trino.sql.tree.DropRole;
import io.trino.sql.tree.DropSchema;
import io.trino.sql.tree.DropTable;
import io.trino.sql.tree.DropView;
import io.trino.sql.tree.Except;
import io.trino.sql.tree.Execute;
import io.trino.sql.tree.ExistsPredicate;
import io.trino.sql.tree.Explain;
import io.trino.sql.tree.ExplainOption;
import io.trino.sql.tree.Extract;
import io.trino.sql.tree.FetchFirst;
import io.trino.sql.tree.FieldReference;
import io.trino.sql.tree.Format;
import io.trino.sql.tree.FrameBound;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Grant;
import io.trino.sql.tree.GrantRoles;
import io.trino.sql.tree.GroupBy;
import io.trino.sql.tree.GroupingElement;
import io.trino.sql.tree.GroupingOperation;
import io.trino.sql.tree.GroupingSets;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.Insert;
import io.trino.sql.tree.Intersect;
import io.trino.sql.tree.IntervalDayTimeDataType;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.Isolation;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.JoinCriteria;
import io.trino.sql.tree.JoinOn;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.Lateral;
import io.trino.sql.tree.LikeClause;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.Limit;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.NumericParameter;
import io.trino.sql.tree.Offset;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.PathElement;
import io.trino.sql.tree.PathSpecification;
import io.trino.sql.tree.Prepare;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.QuantifiedComparisonExpression;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QueryBody;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.RenameColumn;
import io.trino.sql.tree.RenameSchema;
import io.trino.sql.tree.RenameTable;
import io.trino.sql.tree.ResetSession;
import io.trino.sql.tree.Revoke;
import io.trino.sql.tree.RevokeRoles;
import io.trino.sql.tree.Rollback;
import io.trino.sql.tree.Rollup;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.RowDataType;
import io.trino.sql.tree.SampledRelation;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SetOperation;
import io.trino.sql.tree.SetPath;
import io.trino.sql.tree.SetRole;
import io.trino.sql.tree.SetSession;
import io.trino.sql.tree.ShowCatalogs;
import io.trino.sql.tree.ShowColumns;
import io.trino.sql.tree.ShowCreate;
import io.trino.sql.tree.ShowFunctions;
import io.trino.sql.tree.ShowGrants;
import io.trino.sql.tree.ShowRoleGrants;
import io.trino.sql.tree.ShowRoles;
import io.trino.sql.tree.ShowSchemas;
import io.trino.sql.tree.ShowSession;
import io.trino.sql.tree.ShowStats;
import io.trino.sql.tree.ShowTables;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.SimpleGroupBy;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.StartTransaction;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableElement;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.TimeLiteral;
import io.trino.sql.tree.TimestampLiteral;
import io.trino.sql.tree.TransactionAccessMode;
import io.trino.sql.tree.TransactionMode;
import io.trino.sql.tree.TryExpression;
import io.trino.sql.tree.TypeParameter;
import io.trino.sql.tree.Union;
import io.trino.sql.tree.Unnest;
import io.trino.sql.tree.Use;
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

public class BaseRewriteVisitor<C>
        extends AstVisitor<Node, C>
{
    @Override
    protected Node visitNode(Node node, C context)
    {
        return node;
    }

    @Override
    protected Node visitCreateTableAsSelect(CreateTableAsSelect node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new CreateTableAsSelect(
                    node.getLocation().get(),
                    node.getName(),
                    visitAndCast(node.getQuery()),
                    node.isNotExists(),
                    node.getProperties(),
                    node.isWithData(),
                    node.getColumnAliases(),
                    node.getComment());
        }
        return new CreateTableAsSelect(
                node.getName(),
                visitAndCast(node.getQuery()),
                node.isNotExists(),
                node.getProperties(),
                node.isWithData(),
                node.getColumnAliases(),
                node.getComment());
    }

    @Override
    protected Node visitQuery(Query node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new Query(
                    node.getLocation().get(),
                    node.getWith().map(this::visitAndCast),
                    visitAndCast(node.getQueryBody()),
                    node.getOrderBy().map(this::visitAndCast),
                    node.getOffset(),
                    node.getLimit());
        }
        return new Query(
                node.getWith().map(this::visitAndCast),
                visitAndCast(node.getQueryBody()),
                node.getOrderBy().map(this::visitAndCast),
                node.getOffset(),
                node.getLimit());
    }

    @Override
    protected Node visitCurrentTime(CurrentTime node, C context)
    {
        return super.visitCurrentTime(node, context);
    }

    @Override
    protected Node visitExtract(Extract node, C context)
    {
        if (node.getLocation().isPresent()) {
            new Extract(
                    node.getLocation().get(),
                    visitAndCast(node.getExpression()),
                    node.getField());
        }
        return new Extract(visitAndCast(node.getExpression()), node.getField());
    }

    @Override
    protected Node visitCoalesceExpression(CoalesceExpression node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new CoalesceExpression(
                    node.getLocation().get(),
                    visitNodes(node.getOperands()));
        }
        return new CoalesceExpression(visitNodes(node.getOperands()));
    }

    @Override
    protected Node visitLiteral(Literal node, C context)
    {
        return super.visitLiteral(node, context);
    }

    @Override
    protected Node visitDoubleLiteral(DoubleLiteral node, C context)
    {
        return super.visitDoubleLiteral(node, context);
    }

    @Override
    protected Node visitDecimalLiteral(DecimalLiteral node, C context)
    {
        return super.visitDecimalLiteral(node, context);
    }

    @Override
    protected Node visitStatement(Statement node, C context)
    {
        return super.visitStatement(node, context);
    }

    @Override
    protected Node visitPrepare(Prepare node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new Prepare(
                    node.getLocation().get(),
                    node.getName(),
                    visitAndCast(node.getStatement()));
        }
        return new Prepare(
                node.getName(),
                visitAndCast(node.getStatement()));
    }

    @Override
    protected Node visitDeallocate(Deallocate node, C context)
    {
        return super.visitDeallocate(node, context);
    }

    @Override
    protected Node visitExecute(Execute node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new Execute(
                    node.getLocation().get(),
                    node.getName(),
                    visitNodes(node.getParameters()));
        }
        return new Execute(node.getName(), visitNodes(node.getParameters()));
    }

    @Override
    protected Node visitDescribeOutput(DescribeOutput node, C context)
    {
        return super.visitDescribeOutput(node, context);
    }

    @Override
    protected Node visitDescribeInput(DescribeInput node, C context)
    {
        return super.visitDescribeInput(node, context);
    }

    @Override
    protected Node visitExplain(Explain node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new Explain(
                    node.getLocation().get(),
                    visitAndCast(node.getStatement()),
                    node.getOptions());
        }
        return new Explain(
                visitAndCast(node.getStatement()),
                node.getOptions());
    }

    @Override
    protected Node visitShowTables(ShowTables node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new ShowTables(
                    node.getLocation().get(),
                    node.getSchema(),
                    node.getLikePattern(),
                    node.getEscape());
        }
        return new ShowTables(
                node.getSchema(),
                node.getLikePattern(),
                node.getEscape());
    }

    @Override
    protected Node visitShowSchemas(ShowSchemas node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new ShowSchemas(
                    node.getLocation().get(),
                    node.getCatalog(),
                    node.getLikePattern(),
                    node.getEscape());
        }
        return new ShowSchemas(
                node.getCatalog(),
                node.getLikePattern(),
                node.getEscape());
    }

    @Override
    protected Node visitShowCatalogs(ShowCatalogs node, C context)
    {
        return super.visitShowCatalogs(node, context);
    }

    @Override
    protected Node visitShowStats(ShowStats node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new ShowStats(
                    node.getLocation(),
                    visitAndCast(node.getRelation()));
        }
        return new ShowStats(visitAndCast(node.getRelation()));
    }

    @Override
    protected Node visitShowCreate(ShowCreate node, C context)
    {
        if (node.getType() == ShowCreate.Type.TABLE) {
            Table table = (Table) visitTable(new Table(node.getName()), context);
            if (node.getLocation().isPresent()) {
                return new ShowCreate(
                        node.getLocation().get(),
                        node.getType(),
                        table.getName());
            }
            return new ShowCreate(
                    node.getType(),
                    table.getName());
        }
        return super.visitShowCreate(node, context);
    }

    @Override
    protected Node visitShowFunctions(ShowFunctions node, C context)
    {
        return super.visitShowFunctions(node, context);
    }

    @Override
    protected Node visitUse(Use node, C context)
    {
        return super.visitUse(node, context);
    }

    @Override
    protected Node visitShowSession(ShowSession node, C context)
    {
        return super.visitShowSession(node, context);
    }

    @Override
    protected Node visitSetSession(SetSession node, C context)
    {
        return super.visitSetSession(node, context);
    }

    @Override
    protected Node visitResetSession(ResetSession node, C context)
    {
        return super.visitResetSession(node, context);
    }

    @Override
    protected Node visitGenericLiteral(GenericLiteral node, C context)
    {
        return super.visitGenericLiteral(node, context);
    }

    @Override
    protected Node visitTimeLiteral(TimeLiteral node, C context)
    {
        return super.visitTimeLiteral(node, context);
    }

    @Override
    protected Node visitExplainOption(ExplainOption node, C context)
    {
        return super.visitExplainOption(node, context);
    }

    @Override
    protected Node visitRelation(Relation node, C context)
    {
        return super.visitRelation(node, context);
    }

    @Override
    protected Node visitQueryBody(QueryBody node, C context)
    {
        return super.visitQueryBody(node, context);
    }

    @Override
    protected Node visitOffset(Offset node, C context)
    {
        return super.visitOffset(node, context);
    }

    @Override
    protected Node visitFetchFirst(FetchFirst node, C context)
    {
        return super.visitFetchFirst(node, context);
    }

    @Override
    protected Node visitLimit(Limit node, C context)
    {
        return super.visitLimit(node, context);
    }

    @Override
    protected Node visitSetOperation(SetOperation node, C context)
    {
        return super.visitSetOperation(node, context);
    }

    @Override
    protected Node visitIntersect(Intersect node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new Intersect(
                    node.getLocation().get(),
                    visitNodes(node.getRelations()),
                    node.isDistinct());
        }
        return new Intersect(visitNodes(node.getRelations()), node.isDistinct());
    }

    @Override
    protected Node visitExcept(Except node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new Except(
                    node.getLocation().get(),
                    visitAndCast(node.getLeft()),
                    visitAndCast(node.getRight()),
                    node.isDistinct());
        }
        return new Except(
                visitAndCast(node.getLeft()),
                visitAndCast(node.getRight()),
                node.isDistinct());
    }

    @Override
    protected Node visitTimestampLiteral(TimestampLiteral node, C context)
    {
        return super.visitTimestampLiteral(node, context);
    }

    @Override
    protected Node visitWhenClause(WhenClause node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new WhenClause(
                    node.getLocation().get(),
                    visitAndCast(node.getOperand()),
                    visitAndCast(node.getResult()));
        }
        return new WhenClause(
                visitAndCast(node.getOperand()),
                visitAndCast(node.getResult()));
    }

    @Override
    protected Node visitIntervalLiteral(IntervalLiteral node, C context)
    {
        return super.visitIntervalLiteral(node, context);
    }

    @Override
    protected Node visitLambdaExpression(LambdaExpression node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new LambdaExpression(
                    node.getLocation().get(),
                    node.getArguments(),
                    visitAndCast(node.getBody()));
        }
        return new LambdaExpression(
                node.getArguments(),
                visitAndCast(node.getBody()));
    }

    @Override
    protected Node visitSimpleCaseExpression(SimpleCaseExpression node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new SimpleCaseExpression(
                    node.getLocation().get(),
                    visitAndCast(node.getOperand()),
                    visitNodes(node.getWhenClauses()),
                    node.getDefaultValue().map(this::visitAndCast));
        }
        return new SimpleCaseExpression(
                visitAndCast(node.getOperand()),
                visitNodes(node.getWhenClauses()),
                node.getDefaultValue().map(this::visitAndCast));
    }

    @Override
    protected Node visitStringLiteral(StringLiteral node, C context)
    {
        return super.visitStringLiteral(node, context);
    }

    @Override
    protected Node visitCharLiteral(CharLiteral node, C context)
    {
        return super.visitCharLiteral(node, context);
    }

    @Override
    protected Node visitBinaryLiteral(BinaryLiteral node, C context)
    {
        return super.visitBinaryLiteral(node, context);
    }

    @Override
    protected Node visitBooleanLiteral(BooleanLiteral node, C context)
    {
        return super.visitBooleanLiteral(node, context);
    }

    @Override
    protected Node visitInListExpression(InListExpression node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new InListExpression(
                    node.getLocation().get(),
                    visitNodes(node.getValues()));
        }
        return new InListExpression(visitNodes(node.getValues()));
    }

    @Override
    protected Node visitIdentifier(Identifier node, C context)
    {
        return super.visitIdentifier(node, context);
    }

    @Override
    protected Node visitNullIfExpression(NullIfExpression node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new NullIfExpression(
                    node.getLocation().get(),
                    visitAndCast(node.getFirst()),
                    visitAndCast(node.getSecond()));
        }
        return new NullIfExpression(
                visitAndCast(node.getFirst()),
                visitAndCast(node.getSecond()));
    }

    @Override
    protected Node visitIfExpression(IfExpression node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new IfExpression(
                    node.getLocation().get(),
                    visitAndCast(node.getCondition()),
                    visitAndCast(node.getTrueValue()),
                    node.getFalseValue().map(this::visitAndCast).orElse(null));
        }
        return new IfExpression(
                visitAndCast(node.getCondition()),
                visitAndCast(node.getTrueValue()),
                node.getFalseValue().map(this::visitAndCast).orElse(null));
    }

    @Override
    protected Node visitNullLiteral(NullLiteral node, C context)
    {
        return super.visitNullLiteral(node, context);
    }

    @Override
    protected Node visitArithmeticUnary(ArithmeticUnaryExpression node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new ArithmeticUnaryExpression(
                    node.getLocation().get(),
                    node.getSign(),
                    visitAndCast(node.getValue()));
        }
        return new ArithmeticUnaryExpression(node.getSign(), visitAndCast(node.getValue()));
    }

    @Override
    protected Node visitSelectItem(SelectItem node, C context)
    {
        return super.visitSelectItem(node, context);
    }

    @Override
    protected Node visitAllColumns(AllColumns node, C context)
    {
        return super.visitAllColumns(node, context);
    }

    @Override
    protected Node visitSearchedCaseExpression(SearchedCaseExpression node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new SearchedCaseExpression(
                    node.getLocation().get(),
                    visitNodes(node.getWhenClauses()),
                    node.getDefaultValue().map(this::visitAndCast));
        }
        return new SearchedCaseExpression(
                visitNodes(node.getWhenClauses()),
                node.getDefaultValue().map(this::visitAndCast));
    }

    @Override
    protected Node visitLikePredicate(LikePredicate node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new LikePredicate(
                    node.getLocation().get(),
                    visitAndCast(node.getValue()),
                    visitAndCast(node.getPattern()),
                    node.getEscape().map(this::visitAndCast));
        }
        return new LikePredicate(
                visitAndCast(node.getValue()),
                visitAndCast(node.getPattern()),
                node.getEscape().map(this::visitAndCast));
    }

    @Override
    protected Node visitIsNotNullPredicate(IsNotNullPredicate node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new IsNotNullPredicate(
                    node.getLocation().get(),
                    visitAndCast(node.getValue()));
        }
        return new IsNotNullPredicate(visitAndCast(node.getValue()));
    }

    @Override
    protected Node visitIsNullPredicate(IsNullPredicate node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new IsNullPredicate(
                    node.getLocation().get(),
                    visitAndCast(node.getValue()));
        }
        return new IsNullPredicate(visitAndCast(node.getValue()));
    }

    @Override
    protected Node visitArrayConstructor(ArrayConstructor node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new ArrayConstructor(
                    node.getLocation().get(),
                    visitNodes(node.getValues()));
        }
        return new ArrayConstructor(visitNodes(node.getValues()));
    }

    @Override
    protected Node visitLogicalExpression(LogicalExpression node, C context)
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
    protected Node visitSubscriptExpression(SubscriptExpression node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new SubscriptExpression(
                    node.getLocation().get(),
                    visitAndCast(node.getBase()),
                    visitAndCast(node.getIndex()));
        }
        return new SubscriptExpression(visitAndCast(node.getBase()), visitAndCast(node.getIndex()));
    }

    @Override
    protected Node visitLongLiteral(LongLiteral node, C context)
    {
        return super.visitLongLiteral(node, context);
    }

    @Override
    protected Node visitParameter(Parameter node, C context)
    {
        return super.visitParameter(node, context);
    }

    @Override
    protected Node visitUnnest(Unnest node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new Unnest(
                    node.getLocation().get(),
                    visitNodes(node.getExpressions()),
                    node.isWithOrdinality());
        }
        return new Unnest(visitNodes(node.getExpressions()), node.isWithOrdinality());
    }

    @Override
    protected Node visitLateral(Lateral node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new Lateral(
                    node.getLocation().get(),
                    visitAndCast(node.getQuery()));
        }
        return new Lateral(visitAndCast(node.getQuery()));
    }

    @Override
    protected Node visitValues(Values node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new Values(
                    node.getLocation().get(),
                    visitNodes(node.getRows()));
        }
        return new Values(visitNodes(node.getRows()));
    }

    @Override
    protected Node visitRow(Row node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new Row(
                    node.getLocation().get(),
                    visitNodes(node.getItems()));
        }
        return new Row(visitNodes(node.getItems()));
    }

    @Override
    protected Node visitSampledRelation(SampledRelation node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new SampledRelation(
                    node.getLocation().get(),
                    visitAndCast(node.getRelation()),
                    node.getType(),
                    visitAndCast(node.getSamplePercentage()));
        }
        return new SampledRelation(
                visitAndCast(node.getRelation()),
                node.getType(),
                visitAndCast(node.getSamplePercentage()));
    }

    @Override
    protected Node visitTryExpression(TryExpression node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new TryExpression(
                    node.getLocation().get(),
                    visitAndCast(node.getInnerExpression()));
        }
        return new TryExpression(visitAndCast(node.getInnerExpression()));
    }

    @Override
    protected Node visitCast(Cast node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new Cast(
                    node.getLocation().get(),
                    visitAndCast(node.getExpression()),
                    node.getType(),
                    node.isSafe(),
                    node.isTypeOnly());
        }
        return new Cast(
                visitAndCast(node.getExpression()),
                node.getType(),
                node.isSafe(),
                node.isTypeOnly());
    }

    @Override
    protected Node visitFieldReference(FieldReference node, C context)
    {
        return super.visitFieldReference(node, context);
    }

    @Override
    protected Node visitWindowSpecification(WindowSpecification node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new WindowSpecification(
                    node.getLocation().get(),
                    node.getExistingWindowName(),
                    visitNodes(node.getPartitionBy()),
                    node.getOrderBy(),
                    node.getFrame().map(this::visitAndCast));
        }
        return new WindowSpecification(
                node.getExistingWindowName(),
                visitNodes(node.getPartitionBy()),
                node.getOrderBy(),
                node.getFrame().map(this::visitAndCast));
    }

    @Override
    protected Node visitWindowFrame(WindowFrame node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new WindowFrame(
                    node.getLocation().get(),
                    node.getType(),
                    visitAndCast(node.getStart()),
                    node.getEnd().map(this::visitAndCast),
                    node.getMeasures(),
                    node.getAfterMatchSkipTo(),
                    node.getPatternSearchMode(),
                    node.getPattern(),
                    node.getSubsets(),
                    node.getVariableDefinitions());
        }
        return new WindowFrame(
                node.getType(),
                visitAndCast(node.getStart()),
                node.getEnd().map(this::visitAndCast),
                node.getMeasures(),
                node.getAfterMatchSkipTo(),
                node.getPatternSearchMode(),
                node.getPattern(),
                node.getSubsets(),
                node.getVariableDefinitions());
    }

    @Override
    protected Node visitFrameBound(FrameBound node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new FrameBound(
                    node.getLocation().get(),
                    node.getType(),
                    node.getValue().map(this::visitAndCast).orElse(null));
        }
        return new FrameBound(
                node.getType(),
                node.getValue().map(this::visitAndCast).orElse(null));
    }

    @Override
    protected Node visitCallArgument(CallArgument node, C context)
    {
        return new CallArgument(
                node.getLocation(),
                node.getName(),
                visitAndCast(node.getValue()));
    }

    @Override
    protected Node visitTableElement(TableElement node, C context)
    {
        return super.visitTableElement(node, context);
    }

    @Override
    protected Node visitColumnDefinition(ColumnDefinition node, C context)
    {
        return super.visitColumnDefinition(node, context);
    }

    @Override
    protected Node visitLikeClause(LikeClause node, C context)
    {
        return super.visitLikeClause(node, context);
    }

    @Override
    protected Node visitCreateSchema(CreateSchema node, C context)
    {
        return super.visitCreateSchema(node, context);
    }

    @Override
    protected Node visitDropSchema(DropSchema node, C context)
    {
        return super.visitDropSchema(node, context);
    }

    @Override
    protected Node visitRenameSchema(RenameSchema node, C context)
    {
        return super.visitRenameSchema(node, context);
    }

    @Override
    protected Node visitCreateTable(CreateTable node, C context)
    {
        return super.visitCreateTable(node, context);
    }

    @Override
    protected Node visitProperty(Property node, C context)
    {
        return super.visitProperty(node, context);
    }

    @Override
    protected Node visitDropTable(DropTable node, C context)
    {
        return super.visitDropTable(node, context);
    }

    @Override
    protected Node visitRenameTable(RenameTable node, C context)
    {
        return super.visitRenameTable(node, context);
    }

    @Override
    protected Node visitComment(Comment node, C context)
    {
        return super.visitComment(node, context);
    }

    @Override
    protected Node visitRenameColumn(RenameColumn node, C context)
    {
        return super.visitRenameColumn(node, context);
    }

    @Override
    protected Node visitDropColumn(DropColumn node, C context)
    {
        return super.visitDropColumn(node, context);
    }

    @Override
    protected Node visitAddColumn(AddColumn node, C context)
    {
        return super.visitAddColumn(node, context);
    }

    @Override
    protected Node visitAnalyze(Analyze node, C context)
    {
        return super.visitAnalyze(node, context);
    }

    @Override
    protected Node visitCreateView(CreateView node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new CreateView(
                    node.getLocation().get(),
                    node.getName(),
                    visitAndCast(node.getQuery()),
                    node.isReplace(),
                    node.getComment(),
                    node.getSecurity());
        }
        return new CreateView(
                node.getName(),
                visitAndCast(node.getQuery()),
                node.isReplace(),
                node.getComment(),
                node.getSecurity());
    }

    @Override
    protected Node visitDropView(DropView node, C context)
    {
        return super.visitDropView(node, context);
    }

    @Override
    protected Node visitInsert(Insert node, C context)
    {
        return super.visitInsert(node, context);
    }

    @Override
    protected Node visitCall(Call node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new Call(
                    node.getLocation().get(),
                    node.getName(),
                    visitNodes(node.getArguments()));
        }
        return new Call(node.getName(), visitNodes(node.getArguments()));
    }

    @Override
    protected Node visitDelete(Delete node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new Delete(
                    node.getLocation().get(),
                    visitAndCast(node.getTable()),
                    node.getWhere().map(this::visitAndCast));
        }
        return new Delete(
                visitAndCast(node.getTable()),
                node.getWhere().map(this::visitAndCast));
    }

    @Override
    protected Node visitStartTransaction(StartTransaction node, C context)
    {
        return super.visitStartTransaction(node, context);
    }

    @Override
    protected Node visitCreateRole(CreateRole node, C context)
    {
        return super.visitCreateRole(node, context);
    }

    @Override
    protected Node visitDropRole(DropRole node, C context)
    {
        return super.visitDropRole(node, context);
    }

    @Override
    protected Node visitGrantRoles(GrantRoles node, C context)
    {
        return super.visitGrantRoles(node, context);
    }

    @Override
    protected Node visitRevokeRoles(RevokeRoles node, C context)
    {
        return super.visitRevokeRoles(node, context);
    }

    @Override
    protected Node visitSetRole(SetRole node, C context)
    {
        return super.visitSetRole(node, context);
    }

    @Override
    protected Node visitGrant(Grant node, C context)
    {
        return super.visitGrant(node, context);
    }

    @Override
    protected Node visitRevoke(Revoke node, C context)
    {
        return super.visitRevoke(node, context);
    }

    @Override
    protected Node visitShowGrants(ShowGrants node, C context)
    {
        return super.visitShowGrants(node, context);
    }

    @Override
    protected Node visitShowRoles(ShowRoles node, C context)
    {
        return super.visitShowRoles(node, context);
    }

    @Override
    protected Node visitShowRoleGrants(ShowRoleGrants node, C context)
    {
        return super.visitShowRoleGrants(node, context);
    }

    @Override
    protected Node visitSetPath(SetPath node, C context)
    {
        return super.visitSetPath(node, context);
    }

    @Override
    protected Node visitPathSpecification(PathSpecification node, C context)
    {
        return super.visitPathSpecification(node, context);
    }

    @Override
    protected Node visitPathElement(PathElement node, C context)
    {
        return super.visitPathElement(node, context);
    }

    @Override
    protected Node visitTransactionMode(TransactionMode node, C context)
    {
        return super.visitTransactionMode(node, context);
    }

    @Override
    protected Node visitIsolationLevel(Isolation node, C context)
    {
        return super.visitIsolationLevel(node, context);
    }

    @Override
    protected Node visitTransactionAccessMode(TransactionAccessMode node, C context)
    {
        return super.visitTransactionAccessMode(node, context);
    }

    @Override
    protected Node visitCommit(Commit node, C context)
    {
        return super.visitCommit(node, context);
    }

    @Override
    protected Node visitRollback(Rollback node, C context)
    {
        return super.visitRollback(node, context);
    }

    @Override
    protected Node visitAtTimeZone(AtTimeZone node, C context)
    {
        return super.visitAtTimeZone(node, context);
    }

    @Override
    protected Node visitGroupingElement(GroupingElement node, C context)
    {
        return super.visitGroupingElement(node, context);
    }

    @Override
    protected Node visitSymbolReference(SymbolReference node, C context)
    {
        return super.visitSymbolReference(node, context);
    }

    @Override
    protected Node visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new QuantifiedComparisonExpression(
                    node.getLocation().get(),
                    node.getOperator(),
                    node.getQuantifier(),
                    visitAndCast(node.getValue()),
                    visitAndCast(node.getSubquery()));
        }
        return new QuantifiedComparisonExpression(
                node.getOperator(),
                node.getQuantifier(),
                visitAndCast(node.getValue()),
                visitAndCast(node.getSubquery()));
    }

    @Override
    protected Node visitLambdaArgumentDeclaration(LambdaArgumentDeclaration node, C context)
    {
        return super.visitLambdaArgumentDeclaration(node, context);
    }

    @Override
    protected Node visitBindExpression(BindExpression node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new BindExpression(
                    node.getLocation().get(),
                    visitNodes(node.getValues()),
                    visitAndCast(node.getFunction()));
        }
        return new BindExpression(visitNodes(node.getValues()), visitAndCast(node.getFunction()));
    }

    @Override
    protected Node visitGroupingOperation(GroupingOperation node, C context)
    {
        return super.visitGroupingOperation(node, context);
    }

    @Override
    protected Node visitCurrentUser(CurrentUser node, C context)
    {
        return super.visitCurrentUser(node, context);
    }

    @Override
    protected Node visitCurrentPath(CurrentPath node, C context)
    {
        return super.visitCurrentPath(node, context);
    }

    @Override
    protected Node visitFormat(Format node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new Format(
                    node.getLocation().get(),
                    visitNodes(node.getArguments()));
        }
        return new Format(visitNodes(node.getArguments()));
    }

    @Override
    protected Node visitDataType(DataType node, C context)
    {
        return super.visitDataType(node, context);
    }

    @Override
    protected Node visitRowDataType(RowDataType node, C context)
    {
        return super.visitRowDataType(node, context);
    }

    @Override
    protected Node visitGenericDataType(GenericDataType node, C context)
    {
        return super.visitGenericDataType(node, context);
    }

    @Override
    protected Node visitRowField(RowDataType.Field node, C context)
    {
        return super.visitRowField(node, context);
    }

    @Override
    protected Node visitDataTypeParameter(DataTypeParameter node, C context)
    {
        return super.visitDataTypeParameter(node, context);
    }

    @Override
    protected Node visitNumericTypeParameter(NumericParameter node, C context)
    {
        return super.visitNumericTypeParameter(node, context);
    }

    @Override
    protected Node visitTypeParameter(TypeParameter node, C context)
    {
        return super.visitTypeParameter(node, context);
    }

    @Override
    protected Node visitIntervalDataType(IntervalDayTimeDataType node, C context)
    {
        return super.visitIntervalDataType(node, context);
    }

    @Override
    protected Node visitDateTimeType(DateTimeDataType node, C context)
    {
        return super.visitDateTimeType(node, context);
    }

    @Override
    protected Node visitInPredicate(InPredicate node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new InPredicate(
                    node.getLocation().get(),
                    visitAndCast(node.getValue()),
                    visitAndCast(node.getValueList()));
        }
        return new InPredicate(
                visitAndCast(node.getValue()),
                visitAndCast(node.getValueList()));
    }

    @Override
    protected Node visitComparisonExpression(ComparisonExpression node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new ComparisonExpression(
                    node.getLocation().get(),
                    node.getOperator(),
                    visitAndCast(node.getLeft()),
                    visitAndCast(node.getRight()));
        }
        return new ComparisonExpression(
                node.getOperator(),
                visitAndCast(node.getLeft()),
                visitAndCast(node.getRight()));
    }

    @Override
    protected Node visitExists(ExistsPredicate node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new ExistsPredicate(
                    node.getLocation().get(),
                    visitAndCast(node.getSubquery()));
        }
        return new ExistsPredicate(visitAndCast(node.getSubquery()));
    }

    @Override
    protected Node visitNotExpression(NotExpression node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new NotExpression(
                    node.getLocation().get(),
                    visitAndCast(node.getValue()));
        }
        return new NotExpression(visitAndCast(node.getValue()));
    }

    @Override
    protected Node visitArithmeticBinary(ArithmeticBinaryExpression node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new ArithmeticBinaryExpression(
                    node.getLocation().get(),
                    node.getOperator(),
                    visitAndCast(node.getLeft()),
                    visitAndCast(node.getRight()));
        }
        return new ArithmeticBinaryExpression(
                node.getOperator(),
                visitAndCast(node.getLeft()),
                visitAndCast(node.getRight()));
    }

    @Override
    protected Node visitBetweenPredicate(BetweenPredicate node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new BetweenPredicate(
                    node.getLocation().get(),
                    visitAndCast(node.getValue()),
                    visitAndCast(node.getMin()),
                    visitAndCast(node.getMax()));
        }
        return new BetweenPredicate(
                visitAndCast(node.getValue()),
                visitAndCast(node.getMin()),
                visitAndCast(node.getMax()));
    }

    @Override
    protected Node visitFunctionCall(FunctionCall node, C context)
    {
        return new FunctionCall(
                node.getLocation(),
                node.getName(),
                node.getWindow().map(this::visitAndCast),
                node.getFilter().map(this::visitAndCast),
                node.getOrderBy().map(this::visitAndCast),
                node.isDistinct(),
                node.getNullTreatment(),
                Optional.empty(),
                visitNodes(node.getArguments()));
    }

    @Override
    protected Node visitQuerySpecification(QuerySpecification node, C context)
    {
        // Relations should be visited first for alias.
        Optional<Relation> from = node.getFrom().map(this::visitAndCast);

        if (node.getLocation().isPresent()) {
            return new QuerySpecification(
                    node.getLocation().get(),
                    visitAndCast(node.getSelect()),
                    from,
                    node.getWhere().map(this::visitAndCast),
                    node.getGroupBy().map(this::visitAndCast),
                    node.getHaving().map(this::visitAndCast),
                    visitNodes(node.getWindows()),
                    node.getOrderBy().map(this::visitAndCast),
                    node.getOffset(),
                    node.getLimit());
        }
        return new QuerySpecification(
                visitAndCast(node.getSelect()),
                from,
                node.getWhere().map(this::visitAndCast),
                node.getGroupBy().map(this::visitAndCast),
                node.getHaving().map(this::visitAndCast),
                visitNodes(node.getWindows()),
                node.getOrderBy().map(this::visitAndCast),
                node.getOffset(),
                node.getLimit());
    }

    @Override
    protected Node visitShowColumns(ShowColumns node, C context)
    {
        Table table = (Table) visitTable(new Table(node.getTable()), context);
        if (node.getLocation().isPresent()) {
            return new ShowColumns(
                    node.getLocation().get(),
                    table.getName(),
                    Optional.empty(),
                    Optional.empty());
        }
        return new ShowColumns(table.getName(), Optional.empty(), Optional.empty());
    }

    @Override
    protected Node visitJoin(Join node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new Join(
                    node.getLocation().get(),
                    node.getType(),
                    visitAndCast(node.getLeft()),
                    visitAndCast(node.getRight()),
                    node.getCriteria().map(this::visitJoinCriteria));
        }
        return new Join(
                node.getType(),
                visitAndCast(node.getLeft()),
                visitAndCast(node.getRight()),
                node.getCriteria().map(this::visitJoinCriteria));
    }

    protected JoinCriteria visitJoinCriteria(JoinCriteria joinCriteria)
    {
        if (joinCriteria instanceof JoinOn) {
            JoinOn joinOn = (JoinOn) joinCriteria;
            return new JoinOn(visitAndCast(joinOn.getExpression()));
        }

        return joinCriteria;
    }

    @Override
    protected Node visitAliasedRelation(AliasedRelation node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new AliasedRelation(
                    node.getLocation().get(),
                    visitAndCast(node.getRelation()),
                    node.getAlias(),
                    node.getColumnNames());
        }
        return new AliasedRelation(
                visitAndCast(node.getRelation()),
                node.getAlias(),
                node.getColumnNames());
    }

    @Override
    protected Node visitSubqueryExpression(SubqueryExpression node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new SubqueryExpression(
                    node.getLocation().get(),
                    visitAndCast(node.getQuery()));
        }
        return new SubqueryExpression(visitAndCast(node.getQuery()));
    }

    @Override
    protected Node visitTableSubquery(TableSubquery node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new TableSubquery(
                    node.getLocation().get(),
                    visitAndCast(node.getQuery()));
        }
        return new TableSubquery(visitAndCast(node.getQuery()));
    }

    @Override
    protected Node visitWith(With node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new With(
                    node.getLocation().get(),
                    node.isRecursive(),
                    visitNodes(node.getQueries()));
        }
        return new With(
                node.isRecursive(),
                visitNodes(node.getQueries()));
    }

    @Override
    protected Node visitWithQuery(WithQuery node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new WithQuery(
                    node.getLocation().get(),
                    node.getName(),
                    visitAndCast(node.getQuery()),
                    node.getColumnNames());
        }
        return new WithQuery(
                node.getName(),
                visitAndCast(node.getQuery()),
                node.getColumnNames());
    }

    @Override
    protected Node visitUnion(Union node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new Union(
                    node.getLocation().get(),
                    visitNodes(node.getRelations()),
                    node.isDistinct());
        }
        return new Union(
                visitNodes(node.getRelations()),
                node.isDistinct());
    }

    @Override
    protected Node visitSelect(Select node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new Select(
                    node.getLocation().get(),
                    node.isDistinct(),
                    visitNodes(node.getSelectItems()));
        }
        return new Select(
                node.isDistinct(),
                visitNodes(node.getSelectItems()));
    }

    @Override
    protected Node visitGroupBy(GroupBy node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new GroupBy(
                    node.getLocation().get(),
                    node.isDistinct(),
                    visitNodes(node.getGroupingElements(), context));
        }
        return new GroupBy(node.isDistinct(), visitNodes(node.getGroupingElements()));
    }

    @Override
    protected Node visitCube(Cube node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new Cube(
                    node.getLocation().get(),
                    visitNodes(node.getExpressions()));
        }
        return new Cube(visitNodes(node.getExpressions()));
    }

    @Override
    protected Node visitGroupingSets(GroupingSets node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new GroupingSets(
                    node.getLocation().get(),
                    node.getSets().stream()
                            .map(this::visitNodes)
                            .collect(toList()));
        }
        return new GroupingSets(
                node.getSets().stream()
                        .map(this::visitNodes)
                        .collect(toList()));
    }

    @Override
    protected Node visitSimpleGroupBy(SimpleGroupBy node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new SimpleGroupBy(
                    node.getLocation().get(),
                    visitNodes(node.getExpressions()));
        }
        return new SimpleGroupBy(visitNodes(node.getExpressions()));
    }

    @Override
    protected Node visitRollup(Rollup node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new Rollup(
                    node.getLocation().get(),
                    visitNodes(node.getExpressions()));
        }
        return new Rollup(visitNodes(node.getExpressions()));
    }

    @Override
    protected Node visitOrderBy(OrderBy node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new OrderBy(
                    node.getLocation().get(),
                    visitNodes(node.getSortItems()));
        }
        return new OrderBy(visitNodes(node.getSortItems()));
    }

    @Override
    protected Node visitSortItem(SortItem node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new SortItem(
                    node.getLocation().get(),
                    visitAndCast(node.getSortKey()),
                    node.getOrdering(),
                    node.getNullOrdering());
        }
        return new SortItem(
                visitAndCast(node.getSortKey()),
                node.getOrdering(),
                node.getNullOrdering());
    }

    @Override
    protected Node visitSingleColumn(SingleColumn node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new SingleColumn(
                    node.getLocation().get(),
                    visitAndCast(node.getExpression()),
                    node.getAlias());
        }
        return new SingleColumn(
                visitAndCast(node.getExpression()),
                node.getAlias());
    }

    @Override
    protected Node visitDereferenceExpression(DereferenceExpression node, C context)
    {
        return new DereferenceExpression(
                node.getLocation(),
                node.getBase(),
                node.getField());
    }

    @Override
    protected Node visitTable(Table node, C context)
    {
        if (node.getLocation().isPresent()) {
            return new Table(
                    node.getLocation().get(),
                    node.getName());
        }
        return new Table(node.getName());
    }

    @SuppressWarnings("unchecked")
    protected <T extends Node> T visitAndCast(T node)
    {
        return (T) process(node);
    }

    @SuppressWarnings("unchecked")
    protected <T extends Window> T visitAndCast(T window)
    {
        Node node = null;
        if (window instanceof WindowSpecification) {
            node = (WindowSpecification) window;
        }
        else if (window instanceof WindowReference) {
            node = (WindowReference) window;
        }
        return (T) process(node);
    }

    @SuppressWarnings("unchecked")
    protected <T extends Node> List<T> visitNodes(List<T> nodes)
    {
        return nodes.stream()
                .map(node -> (T) process(node))
                .collect(toList());
    }

    protected <T extends Node> List<T> visitNodes(List<T> nodes, C context)
    {
        return nodes.stream()
                .map(node -> (T) process(node, context))
                .collect(toList());
    }
}
