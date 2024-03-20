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

import io.trino.sql.tree.AddColumn;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.Analyze;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.AtTimeZone;
import io.trino.sql.tree.Call;
import io.trino.sql.tree.CallArgument;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.Comment;
import io.trino.sql.tree.Commit;
import io.trino.sql.tree.CreateRole;
import io.trino.sql.tree.CreateSchema;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.CreateTableAsSelect;
import io.trino.sql.tree.CreateView;
import io.trino.sql.tree.Cube;
import io.trino.sql.tree.CurrentPath;
import io.trino.sql.tree.CurrentTime;
import io.trino.sql.tree.CurrentUser;
import io.trino.sql.tree.Deallocate;
import io.trino.sql.tree.Delete;
import io.trino.sql.tree.DescribeInput;
import io.trino.sql.tree.DescribeOutput;
import io.trino.sql.tree.DropColumn;
import io.trino.sql.tree.DropRole;
import io.trino.sql.tree.DropSchema;
import io.trino.sql.tree.DropTable;
import io.trino.sql.tree.DropView;
import io.trino.sql.tree.Except;
import io.trino.sql.tree.Execute;
import io.trino.sql.tree.Explain;
import io.trino.sql.tree.ExplainOption;
import io.trino.sql.tree.Extract;
import io.trino.sql.tree.FetchFirst;
import io.trino.sql.tree.Format;
import io.trino.sql.tree.FrameBound;
import io.trino.sql.tree.FunctionRelation;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Grant;
import io.trino.sql.tree.GrantRoles;
import io.trino.sql.tree.GroupBy;
import io.trino.sql.tree.GroupingElement;
import io.trino.sql.tree.GroupingOperation;
import io.trino.sql.tree.GroupingSets;
import io.trino.sql.tree.Insert;
import io.trino.sql.tree.Intersect;
import io.trino.sql.tree.Isolation;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.JoinCriteria;
import io.trino.sql.tree.JoinOn;
import io.trino.sql.tree.Lateral;
import io.trino.sql.tree.LikeClause;
import io.trino.sql.tree.Limit;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Offset;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.PathElement;
import io.trino.sql.tree.PathSpecification;
import io.trino.sql.tree.Prepare;
import io.trino.sql.tree.Property;
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
import io.trino.sql.tree.SampledRelation;
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
import io.trino.sql.tree.SimpleGroupBy;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.StartTransaction;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableElement;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.TimeLiteral;
import io.trino.sql.tree.TransactionAccessMode;
import io.trino.sql.tree.TransactionMode;
import io.trino.sql.tree.Union;
import io.trino.sql.tree.Unnest;
import io.trino.sql.tree.Use;
import io.trino.sql.tree.Values;
import io.trino.sql.tree.Window;
import io.trino.sql.tree.WindowFrame;
import io.trino.sql.tree.WindowReference;
import io.trino.sql.tree.WindowSpecification;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;

import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

/**
 * Base class for tree rewriter. It will traverse all query nodes excludes all expressions, literals and data type parameters.
 */
public class BaseTreeRewriter<T>
        extends AstVisitor<Node, T>
{
    @Override
    protected Node visitNode(Node node, T context)
    {
        return node;
    }

    @Override
    protected Node visitCreateTableAsSelect(CreateTableAsSelect node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new CreateTableAsSelect(
                    node.getLocation().get(),
                    node.getName(),
                    visitAndCast(node.getQuery(), context),
                    node.isNotExists(),
                    node.getProperties(),
                    node.isWithData(),
                    node.getColumnAliases(),
                    node.getComment());
        }
        return new CreateTableAsSelect(
                node.getName(),
                visitAndCast(node.getQuery(), context),
                node.isNotExists(),
                node.getProperties(),
                node.isWithData(),
                node.getColumnAliases(),
                node.getComment());
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
    protected Node visitStatement(Statement node, T context)
    {
        return super.visitStatement(node, context);
    }

    @Override
    protected Node visitPrepare(Prepare node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new Prepare(
                    node.getLocation().get(),
                    node.getName(),
                    visitAndCast(node.getStatement(), context));
        }
        return new Prepare(
                node.getName(),
                visitAndCast(node.getStatement(), context));
    }

    @Override
    protected Node visitDeallocate(Deallocate node, T context)
    {
        return super.visitDeallocate(node, context);
    }

    @Override
    protected Node visitExecute(Execute node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new Execute(
                    node.getLocation().get(),
                    node.getName(),
                    visitNodes(node.getParameters(), context));
        }
        return new Execute(node.getName(), visitNodes(node.getParameters(), context));
    }

    @Override
    protected Node visitDescribeOutput(DescribeOutput node, T context)
    {
        return super.visitDescribeOutput(node, context);
    }

    @Override
    protected Node visitDescribeInput(DescribeInput node, T context)
    {
        return super.visitDescribeInput(node, context);
    }

    @Override
    protected Node visitExplain(Explain node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new Explain(
                    node.getLocation().get(),
                    visitAndCast(node.getStatement(), context),
                    node.getOptions());
        }
        return new Explain(
                visitAndCast(node.getStatement(), context),
                node.getOptions());
    }

    @Override
    protected Node visitShowTables(ShowTables node, T context)
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
    protected Node visitShowSchemas(ShowSchemas node, T context)
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
    protected Node visitShowCatalogs(ShowCatalogs node, T context)
    {
        return super.visitShowCatalogs(node, context);
    }

    @Override
    protected Node visitShowStats(ShowStats node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new ShowStats(
                    node.getLocation(),
                    visitAndCast(node.getRelation(), context));
        }
        return new ShowStats(visitAndCast(node.getRelation(), context));
    }

    @Override
    protected Node visitShowCreate(ShowCreate node, T context)
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
    protected Node visitShowFunctions(ShowFunctions node, T context)
    {
        return super.visitShowFunctions(node, context);
    }

    @Override
    protected Node visitUse(Use node, T context)
    {
        return super.visitUse(node, context);
    }

    @Override
    protected Node visitShowSession(ShowSession node, T context)
    {
        return super.visitShowSession(node, context);
    }

    @Override
    protected Node visitSetSession(SetSession node, T context)
    {
        return super.visitSetSession(node, context);
    }

    @Override
    protected Node visitResetSession(ResetSession node, T context)
    {
        return super.visitResetSession(node, context);
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
    protected Node visitExplainOption(ExplainOption node, T context)
    {
        return super.visitExplainOption(node, context);
    }

    @Override
    protected Node visitRelation(Relation node, T context)
    {
        return super.visitRelation(node, context);
    }

    @Override
    protected Node visitQueryBody(QueryBody node, T context)
    {
        return super.visitQueryBody(node, context);
    }

    @Override
    protected Node visitOffset(Offset node, T context)
    {
        return super.visitOffset(node, context);
    }

    @Override
    protected Node visitFetchFirst(FetchFirst node, T context)
    {
        return super.visitFetchFirst(node, context);
    }

    @Override
    protected Node visitLimit(Limit node, T context)
    {
        return super.visitLimit(node, context);
    }

    @Override
    protected Node visitSetOperation(SetOperation node, T context)
    {
        return super.visitSetOperation(node, context);
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
    protected Node visitSelectItem(SelectItem node, T context)
    {
        return super.visitSelectItem(node, context);
    }

    @Override
    protected Node visitAllColumns(AllColumns node, T context)
    {
        return super.visitAllColumns(node, context);
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
    protected Node visitCallArgument(CallArgument node, T context)
    {
        return new CallArgument(
                node.getLocation(),
                node.getName(),
                visitAndCast(node.getValue(), context));
    }

    @Override
    protected Node visitTableElement(TableElement node, T context)
    {
        return super.visitTableElement(node, context);
    }

    @Override
    protected Node visitColumnDefinition(ColumnDefinition node, T context)
    {
        return super.visitColumnDefinition(node, context);
    }

    @Override
    protected Node visitLikeClause(LikeClause node, T context)
    {
        return super.visitLikeClause(node, context);
    }

    @Override
    protected Node visitCreateSchema(CreateSchema node, T context)
    {
        return super.visitCreateSchema(node, context);
    }

    @Override
    protected Node visitDropSchema(DropSchema node, T context)
    {
        return super.visitDropSchema(node, context);
    }

    @Override
    protected Node visitRenameSchema(RenameSchema node, T context)
    {
        return super.visitRenameSchema(node, context);
    }

    @Override
    protected Node visitCreateTable(CreateTable node, T context)
    {
        return super.visitCreateTable(node, context);
    }

    @Override
    protected Node visitProperty(Property node, T context)
    {
        return super.visitProperty(node, context);
    }

    @Override
    protected Node visitDropTable(DropTable node, T context)
    {
        return super.visitDropTable(node, context);
    }

    @Override
    protected Node visitRenameTable(RenameTable node, T context)
    {
        return super.visitRenameTable(node, context);
    }

    @Override
    protected Node visitComment(Comment node, T context)
    {
        return super.visitComment(node, context);
    }

    @Override
    protected Node visitRenameColumn(RenameColumn node, T context)
    {
        return super.visitRenameColumn(node, context);
    }

    @Override
    protected Node visitDropColumn(DropColumn node, T context)
    {
        return super.visitDropColumn(node, context);
    }

    @Override
    protected Node visitAddColumn(AddColumn node, T context)
    {
        return super.visitAddColumn(node, context);
    }

    @Override
    protected Node visitAnalyze(Analyze node, T context)
    {
        return super.visitAnalyze(node, context);
    }

    @Override
    protected Node visitCreateView(CreateView node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new CreateView(
                    node.getLocation().get(),
                    node.getName(),
                    visitAndCast(node.getQuery(), context),
                    node.isReplace(),
                    node.getComment(),
                    node.getSecurity());
        }
        return new CreateView(
                node.getName(),
                visitAndCast(node.getQuery(), context),
                node.isReplace(),
                node.getComment(),
                node.getSecurity());
    }

    @Override
    protected Node visitDropView(DropView node, T context)
    {
        return super.visitDropView(node, context);
    }

    @Override
    protected Node visitInsert(Insert node, T context)
    {
        return super.visitInsert(node, context);
    }

    @Override
    protected Node visitCall(Call node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new Call(
                    node.getLocation().get(),
                    node.getName(),
                    visitNodes(node.getArguments(), context));
        }
        return new Call(node.getName(), visitNodes(node.getArguments(), context));
    }

    @Override
    protected Node visitDelete(Delete node, T context)
    {
        if (node.getLocation().isPresent()) {
            return new Delete(
                    node.getLocation().get(),
                    visitAndCast(node.getTable(), context),
                    node.getWhere().map(expression -> visitAndCast(expression, context)));
        }
        return new Delete(
                visitAndCast(node.getTable(), context),
                node.getWhere().map(expression -> visitAndCast(expression, context)));
    }

    @Override
    protected Node visitStartTransaction(StartTransaction node, T context)
    {
        return super.visitStartTransaction(node, context);
    }

    @Override
    protected Node visitCreateRole(CreateRole node, T context)
    {
        return super.visitCreateRole(node, context);
    }

    @Override
    protected Node visitDropRole(DropRole node, T context)
    {
        return super.visitDropRole(node, context);
    }

    @Override
    protected Node visitGrantRoles(GrantRoles node, T context)
    {
        return super.visitGrantRoles(node, context);
    }

    @Override
    protected Node visitRevokeRoles(RevokeRoles node, T context)
    {
        return super.visitRevokeRoles(node, context);
    }

    @Override
    protected Node visitSetRole(SetRole node, T context)
    {
        return super.visitSetRole(node, context);
    }

    @Override
    protected Node visitGrant(Grant node, T context)
    {
        return super.visitGrant(node, context);
    }

    @Override
    protected Node visitRevoke(Revoke node, T context)
    {
        return super.visitRevoke(node, context);
    }

    @Override
    protected Node visitShowGrants(ShowGrants node, T context)
    {
        return super.visitShowGrants(node, context);
    }

    @Override
    protected Node visitShowRoles(ShowRoles node, T context)
    {
        return super.visitShowRoles(node, context);
    }

    @Override
    protected Node visitShowRoleGrants(ShowRoleGrants node, T context)
    {
        return super.visitShowRoleGrants(node, context);
    }

    @Override
    protected Node visitSetPath(SetPath node, T context)
    {
        return super.visitSetPath(node, context);
    }

    @Override
    protected Node visitPathSpecification(PathSpecification node, T context)
    {
        return super.visitPathSpecification(node, context);
    }

    @Override
    protected Node visitPathElement(PathElement node, T context)
    {
        return super.visitPathElement(node, context);
    }

    @Override
    protected Node visitTransactionMode(TransactionMode node, T context)
    {
        return super.visitTransactionMode(node, context);
    }

    @Override
    protected Node visitIsolationLevel(Isolation node, T context)
    {
        return super.visitIsolationLevel(node, context);
    }

    @Override
    protected Node visitTransactionAccessMode(TransactionAccessMode node, T context)
    {
        return super.visitTransactionAccessMode(node, context);
    }

    @Override
    protected Node visitCommit(Commit node, T context)
    {
        return super.visitCommit(node, context);
    }

    @Override
    protected Node visitRollback(Rollback node, T context)
    {
        return super.visitRollback(node, context);
    }

    @Override
    protected Node visitAtTimeZone(AtTimeZone node, T context)
    {
        return super.visitAtTimeZone(node, context);
    }

    @Override
    protected Node visitGroupingElement(GroupingElement node, T context)
    {
        return super.visitGroupingElement(node, context);
    }

    @Override
    protected Node visitSymbolReference(SymbolReference node, T context)
    {
        return super.visitSymbolReference(node, context);
    }

    @Override
    protected Node visitGroupingOperation(GroupingOperation node, T context)
    {
        return super.visitGroupingOperation(node, context);
    }

    @Override
    protected Node visitCurrentUser(CurrentUser node, T context)
    {
        return super.visitCurrentUser(node, context);
    }

    @Override
    protected Node visitCurrentPath(CurrentPath node, T context)
    {
        return super.visitCurrentPath(node, context);
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
    protected Node visitShowColumns(ShowColumns node, T context)
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
