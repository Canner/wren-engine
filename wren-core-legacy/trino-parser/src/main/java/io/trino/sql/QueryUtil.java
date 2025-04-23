package io.trino.sql;

import com.google.common.collect.ImmutableList;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GroupBy;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.JoinCriteria;
import io.trino.sql.tree.JoinOn;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Offset;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QueryBody;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.Unnest;
import io.trino.sql.tree.Values;
import io.trino.sql.tree.WhenClause;
import io.trino.sql.tree.WindowDefinition;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static io.trino.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.Arrays.asList;

public final class QueryUtil
{
    private QueryUtil() {}

    public static Identifier identifier(String name)
    {
        return new Identifier(name);
    }

    public static Identifier quotedIdentifier(String name)
    {
        return new Identifier(name, true);
    }

    public static Expression nameReference(String first, String... rest)
    {
        return DereferenceExpression.from(QualifiedName.of(first, rest));
    }

    public static SelectItem unaliasedName(String name)
    {
        return new SingleColumn(identifier(name));
    }

    public static SelectItem aliasedName(String name, String alias)
    {
        return new SingleColumn(identifier(name), identifier(alias));
    }

    public static SubscriptExpression subscriptExpression(Expression name, String index)
    {
        return new SubscriptExpression(name, new LongLiteral(index));
    }

    public static Select selectList(Expression... expressions)
    {
        return selectList(asList(expressions));
    }

    public static Select selectList(List<Expression> expressions)
    {
        ImmutableList.Builder<SelectItem> items = ImmutableList.builder();
        for (Expression expression : expressions) {
            items.add(new SingleColumn(expression));
        }
        return new Select(false, items.build());
    }

    public static Select selectListDistinct(List<Expression> expressions)
    {
        ImmutableList.Builder<SelectItem> items = ImmutableList.builder();
        for (Expression expression : expressions) {
            items.add(new SingleColumn(expression));
        }
        return new Select(true, items.build());
    }

    public static Select selectList(List<Expression> expressions, List<String> aliases)
    {
        ImmutableList.Builder<SelectItem> items = ImmutableList.builder();
        for (int i = 0; i < expressions.size(); i++) {
            items.add(new SingleColumn(expressions.get(i), identifier(aliases.get(i))));
        }
        return new Select(false, items.build());
    }

    public static Select selectList(SelectItem... items)
    {
        return new Select(false, ImmutableList.copyOf(items));
    }

    public static Select selectAll(List<SelectItem> items)
    {
        return new Select(false, items);
    }

    public static Table table(QualifiedName name)
    {
        return new Table(name);
    }

    public static Unnest unnest(Expression... expressions)
    {
        return new Unnest(asList(expressions), false);
    }

    public static Join leftJoin(Relation left, Relation right, JoinCriteria joinCriteria)
    {
        return new Join(Join.Type.LEFT, left, right, Optional.ofNullable(joinCriteria));
    }

    public static Join crossJoin(Relation left, Relation right)
    {
        return new Join(Join.Type.CROSS, left, right, Optional.empty());
    }

    public static Join implicitJoin(Relation left, Relation right)
    {
        return new Join(Join.Type.IMPLICIT, left, right, Optional.empty());
    }

    public static JoinOn joinOn(Expression conditionSql)
    {
        return new JoinOn(conditionSql);
    }

    public static ComparisonExpression getConditionNode(String condition)
    {
        SqlParser sqlParser = new SqlParser();
        Query statement = (Query) sqlParser.createStatement("SELECT " + condition, new ParsingOptions(AS_DOUBLE));
        return (ComparisonExpression)
                ((SingleColumn) ((QuerySpecification) statement.getQueryBody()).getSelect().getSelectItems().get(0)).getExpression();
    }

    public static Relation subquery(Query query)
    {
        return new TableSubquery(query);
    }

    public static SortItem ascending(String name)
    {
        return new SortItem(identifier(name), SortItem.Ordering.ASCENDING, SortItem.NullOrdering.UNDEFINED);
    }

    public static Expression logicalAnd(Expression left, Expression right)
    {
        return LogicalExpression.and(left, right);
    }

    public static Expression equal(Expression left, Expression right)
    {
        return new ComparisonExpression(ComparisonExpression.Operator.EQUAL, left, right);
    }

    public static Expression caseWhen(Expression operand, Expression result)
    {
        return new SearchedCaseExpression(ImmutableList.of(new WhenClause(operand, result)), Optional.empty());
    }

    public static Expression functionCall(String name, List<Expression> arguments)
    {
        return new FunctionCall(QualifiedName.of(name), ImmutableList.copyOf(arguments));
    }

    public static Expression functionCall(String name, Expression... arguments)
    {
        return new FunctionCall(QualifiedName.of(name), ImmutableList.copyOf(arguments));
    }

    public static Values values(Row... row)
    {
        return new Values(ImmutableList.copyOf(row));
    }

    public static Row row(Expression... values)
    {
        return new Row(ImmutableList.copyOf(values));
    }

    public static Relation aliased(Relation relation, String alias)
    {
        return new AliasedRelation(relation, identifier(alias), null);
    }

    public static Relation aliased(Relation relation, String alias, List<String> columnAliases)
    {
        return new AliasedRelation(
                relation,
                identifier(alias),
                columnAliases.stream()
                        .map(QueryUtil::identifier)
                        .collect(Collectors.toList()));
    }

    public static SelectItem aliasedNullToEmpty(String column, String alias)
    {
        return new SingleColumn(new CoalesceExpression(identifier(column), new StringLiteral("")), identifier(alias));
    }

    public static OrderBy ordering(SortItem... items)
    {
        return new OrderBy(ImmutableList.copyOf(items));
    }

    public static Query simpleQuery(Select select)
    {
        return query(new QuerySpecification(
                select,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));
    }

    public static Query simpleQuery(Select select, Relation from)
    {
        return simpleQuery(select, from, Optional.empty(), Optional.empty());
    }

    public static Query simpleQuery(Select select, Relation from, OrderBy orderBy)
    {
        return simpleQuery(select, from, Optional.empty(), Optional.of(orderBy));
    }

    public static Query simpleQuery(Select select, Relation from, Expression where)
    {
        return simpleQuery(select, from, Optional.of(where), Optional.empty());
    }

    public static Query simpleQuery(Select select, Relation from, Expression where, OrderBy orderBy)
    {
        return simpleQuery(select, from, Optional.of(where), Optional.of(orderBy));
    }

    public static Query simpleQuery(Select select, Relation from, Optional<Expression> where, Optional<OrderBy> orderBy)
    {
        return simpleQuery(select, from, where, Optional.empty(), Optional.empty(), orderBy, Optional.empty(), Optional.empty());
    }

    public static Query simpleQuery(
            Select select,
            Relation from,
            Optional<Expression> where,
            Optional<GroupBy> groupBy,
            Optional<Expression> having,
            Optional<OrderBy> orderBy,
            Optional<Offset> offset,
            Optional<Node> limit)
    {
        return simpleQuery(select, from, where, groupBy, having, ImmutableList.of(), orderBy, offset, limit);
    }

    public static Query simpleQuery(
            Select select,
            Relation from,
            Optional<Expression> where,
            Optional<GroupBy> groupBy,
            Optional<Expression> having,
            List<WindowDefinition> windows,
            Optional<OrderBy> orderBy,
            Optional<Offset> offset,
            Optional<Node> limit)
    {
        return query(new QuerySpecification(
                select,
                Optional.of(from),
                where,
                groupBy,
                having,
                windows,
                orderBy,
                offset,
                limit));
    }

    public static Query singleValueQuery(String columnName, String value)
    {
        Relation values = values(row(new StringLiteral((value))));
        return simpleQuery(
                selectList(new AllColumns()),
                aliased(values, "t", ImmutableList.of(columnName)));
    }

    public static Query singleValueQuery(String columnName, boolean value)
    {
        Relation values = values(row(value ? TRUE_LITERAL : FALSE_LITERAL));
        return simpleQuery(
                selectList(new AllColumns()),
                aliased(values, "t", ImmutableList.of(columnName)));
    }

    // TODO pass column types
    public static Query emptyQuery(List<String> columns)
    {
        Select select = selectList(columns.stream()
                .map(column -> new SingleColumn(new NullLiteral(), QueryUtil.identifier(column)))
                .toArray(SelectItem[]::new));
        Optional<Expression> where = Optional.of(FALSE_LITERAL);
        return query(new QuerySpecification(
                select,
                Optional.empty(),
                where,
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));
    }

    public static Query query(QueryBody body)
    {
        return new Query(
                Optional.empty(),
                body,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    public static QualifiedName getQualifiedName(Expression expression)
    {
        if (expression instanceof DereferenceExpression) {
            return DereferenceExpression.getQualifiedName((DereferenceExpression) expression);
        }
        if (expression instanceof Identifier) {
            return QualifiedName.of(ImmutableList.of((Identifier) expression));
        }
        return null;
    }
}
