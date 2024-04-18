package io.trino.sql;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.trino.sql.SqlFormatter.Dialect;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.AllRows;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.ArrayConstructor;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.AtTimeZone;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BindExpression;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CharLiteral;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Cube;
import io.trino.sql.tree.CurrentCatalog;
import io.trino.sql.tree.CurrentPath;
import io.trino.sql.tree.CurrentSchema;
import io.trino.sql.tree.CurrentTime;
import io.trino.sql.tree.CurrentUser;
import io.trino.sql.tree.DateTimeDataType;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.ExistsPredicate;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Extract;
import io.trino.sql.tree.FieldReference;
import io.trino.sql.tree.Format;
import io.trino.sql.tree.FrameBound;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.GroupingElement;
import io.trino.sql.tree.GroupingOperation;
import io.trino.sql.tree.GroupingSets;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.IntervalDayTimeDataType;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LabelDereference;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.NumericParameter;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.QuantifiedComparisonExpression;
import io.trino.sql.tree.Rollup;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.RowDataType;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.SimpleGroupBy;
import io.trino.sql.tree.SkipTo;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.TimeLiteral;
import io.trino.sql.tree.TimestampLiteral;
import io.trino.sql.tree.TryExpression;
import io.trino.sql.tree.TypeParameter;
import io.trino.sql.tree.WhenClause;
import io.trino.sql.tree.Window;
import io.trino.sql.tree.WindowFrame;
import io.trino.sql.tree.WindowOperation;
import io.trino.sql.tree.WindowReference;
import io.trino.sql.tree.WindowSpecification;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.PrimitiveIterator;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.RowPatternFormatter.formatPattern;
import static io.trino.sql.SqlFormatter.Dialect.BIGQUERY;
import static io.trino.sql.SqlFormatter.Dialect.DEFAULT;
import static io.trino.sql.SqlFormatter.Dialect.DUCKDB;
import static io.trino.sql.SqlFormatter.Dialect.POSTGRES;
import static io.trino.sql.SqlFormatter.formatName;
import static io.trino.sql.SqlFormatter.formatSql;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.QuantifiedComparisonExpression.Quantifier.ANY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public final class ExpressionFormatter
{
    private static final ThreadLocal<DecimalFormat> doubleFormatter = ThreadLocal.withInitial(
            () -> new DecimalFormat("0.###################E0###", new DecimalFormatSymbols(Locale.US)));

    private ExpressionFormatter() {}

    public static String formatExpression(Expression expression, Dialect dialect)
    {
        return new Formatter(dialect).process(expression, null);
    }

    private static String formatIdentifier(String s, Dialect dialect)
    {
        if (dialect == DEFAULT || dialect == DUCKDB || dialect == POSTGRES) {
            return '"' + s.replace("\"", "\"\"") + '"';
        }
        else if (dialect == BIGQUERY) {
            return '`' + s + '`';
        }
        throw new IllegalArgumentException("Unsupported identifier quote: " + dialect);
    }

    public static class Formatter
            extends AstVisitor<String, Void>
    {
        private final Dialect dialect;

        private Formatter(Dialect dialect)
        {
            this.dialect = requireNonNull(dialect, "dialect is null");
        }

        @Override
        protected String visitNode(Node node, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected String visitRow(Row node, Void context)
        {
            String rowPrefix = (dialect == DEFAULT || dialect == BIGQUERY) ? "ROW" : "";
            return rowPrefix + " (" + Joiner.on(", ").join(node.getItems().stream()
                    .map(child -> process(child, context))
                    .collect(toList())) + ")";
        }

        @Override
        protected String visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException(format("not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        protected String visitAtTimeZone(AtTimeZone node, Void context)
        {
            return new StringBuilder()
                    .append(process(node.getValue(), context))
                    .append(" AT TIME ZONE ")
                    .append(process(node.getTimeZone(), context)).toString();
        }

        @Override
        protected String visitCurrentCatalog(CurrentCatalog node, Void context)
        {
            return "CURRENT_CATALOG";
        }

        @Override
        protected String visitCurrentSchema(CurrentSchema node, Void context)
        {
            return "CURRENT_SCHEMA";
        }

        @Override
        protected String visitCurrentUser(CurrentUser node, Void context)
        {
            return "CURRENT_USER";
        }

        @Override
        protected String visitCurrentPath(CurrentPath node, Void context)
        {
            return "CURRENT_PATH";
        }

        @Override
        protected String visitFormat(Format node, Void context)
        {
            return "format(" + joinExpressions(node.getArguments()) + ")";
        }

        @Override
        protected String visitCurrentTime(CurrentTime node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append(node.getFunction().getName());

            if (node.getPrecision() != null) {
                builder.append('(')
                        .append(node.getPrecision())
                        .append(')');
            }

            return builder.toString();
        }

        @Override
        protected String visitExtract(Extract node, Void context)
        {
            return "EXTRACT(" + node.getField() + " FROM " + process(node.getExpression(), context) + ")";
        }

        @Override
        protected String visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            return String.valueOf(node.getValue());
        }

        @Override
        protected String visitStringLiteral(StringLiteral node, Void context)
        {
            return formatStringLiteral(node.getValue(), dialect);
        }

        @Override
        protected String visitCharLiteral(CharLiteral node, Void context)
        {
            return "CHAR " + formatStringLiteral(node.getValue(), dialect);
        }

        @Override
        protected String visitBinaryLiteral(BinaryLiteral node, Void context)
        {
            return "X'" + node.toHexString() + "'";
        }

        @Override
        protected String visitParameter(Parameter node, Void context)
        {
            return "?";
        }

        @Override
        protected String visitAllRows(AllRows node, Void context)
        {
            return "ALL";
        }

        @Override
        protected String visitArrayConstructor(ArrayConstructor node, Void context)
        {
            ImmutableList.Builder<String> valueStrings = ImmutableList.builder();
            for (Expression value : node.getValues()) {
                valueStrings.add(formatSql(value, dialect));
            }
            return "ARRAY[" + Joiner.on(",").join(valueStrings.build()) + "]";
        }

        @Override
        protected String visitSubscriptExpression(SubscriptExpression node, Void context)
        {
            String subscript;
            if (dialect == DEFAULT || dialect == DUCKDB || dialect == POSTGRES) {
                subscript = formatSql(node.getIndex(), dialect);
            }
            else if (dialect == BIGQUERY) {
                // BigQuery doesn't support the normal way to access array element like `ARRAY[1,2,3][1]`.
                // It should use `ORDINAL` or `OFFSET` operator to handle index value.
                // Since pg is 1-based array index, that's why we use `ORDINAL` here.
                // https://cloud.google.com/bigquery/docs/reference/standard-sql/arrays#accessing_array_elements
                // use `SAFE_ORDINAL` to avoid array out of bounds error since in sql rewrite one-to-many relationship
                // could use array index in join condition, sometimes the array column could be null
                subscript = "SAFE_ORDINAL(" + formatSql(node.getIndex(), dialect) + ")";
            }
            else {
                throw new IllegalArgumentException("Unsupported dialect: " + dialect);
            }
            if (dialect == POSTGRES) {
                return "(" + formatSql(node.getBase(), dialect) + ")[" + subscript + "]";
            }
            return formatSql(node.getBase(), dialect) + "[" + subscript + "]";
        }

        @Override
        protected String visitLongLiteral(LongLiteral node, Void context)
        {
            return Long.toString(node.getValue());
        }

        @Override
        protected String visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            if (dialect == DUCKDB) {
                return String.valueOf(node.getValue());
            }
            return doubleFormatter.get().format(node.getValue());
        }

        @Override
        protected String visitDecimalLiteral(DecimalLiteral node, Void context)
        {
            if (dialect == DUCKDB) {
                return node.getValue();
            }
            // TODO return node value without "DECIMAL '..'" when FeaturesConfig#parseDecimalLiteralsAsDouble switch is removed
            return "DECIMAL '" + node.getValue() + "'";
        }

        @Override
        protected String visitGenericLiteral(GenericLiteral node, Void context)
        {
            return node.getType() + " " + formatStringLiteral(node.getValue(), dialect);
        }

        @Override
        protected String visitTimeLiteral(TimeLiteral node, Void context)
        {
            return "TIME '" + node.getValue() + "'";
        }

        @Override
        protected String visitTimestampLiteral(TimestampLiteral node, Void context)
        {
            if (dialect == DUCKDB) {
                return "'" + node.getValue() + "'";
            }
            return "TIMESTAMP '" + node.getValue() + "'";
        }

        @Override
        protected String visitNullLiteral(NullLiteral node, Void context)
        {
            return "null";
        }

        @Override
        protected String visitIntervalLiteral(IntervalLiteral node, Void context)
        {
            String sign = (node.getSign() == IntervalLiteral.Sign.NEGATIVE) ? "-" : "";
            StringBuilder builder = new StringBuilder();
            if (dialect.equals(BIGQUERY) || dialect.equals(DUCKDB)) {
                builder.append("INTERVAL ")
                        .append("'").append(sign).append(node.getValue()).append("' ")
                        .append(node.getStartField());
            }
            else {
                builder.append("INTERVAL ")
                        .append(sign)
                        .append(" '").append(node.getValue()).append("' ")
                        .append(node.getStartField());
            }

            if (node.getEndField().isPresent()) {
                builder.append(" TO ").append(node.getEndField().get());
            }
            return builder.toString();
        }

        @Override
        protected String visitSubqueryExpression(SubqueryExpression node, Void context)
        {
            return "(" + formatSql(node.getQuery(), dialect) + ")";
        }

        @Override
        protected String visitExists(ExistsPredicate node, Void context)
        {
            return "(EXISTS " + formatSql(node.getSubquery(), dialect) + ")";
        }

        @Override
        protected String visitIdentifier(Identifier node, Void context)
        {
            if (!node.isDelimited()) {
                return node.getValue();
            }
            else {
                return formatIdentifier(node.getValue(), dialect);
            }
        }

        @Override
        protected String visitLambdaArgumentDeclaration(LambdaArgumentDeclaration node, Void context)
        {
            return formatExpression(node.getName(), dialect);
        }

        @Override
        protected String visitSymbolReference(SymbolReference node, Void context)
        {
            return formatIdentifier(node.getName(), dialect);
        }

        @Override
        protected String visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            String baseString = process(node.getBase(), context);
            return baseString + "." + node.getField().map(this::process).orElse("*");
        }

        @Override
        public String visitFieldReference(FieldReference node, Void context)
        {
            // add colon so this won't parse
            return ":input(" + node.getFieldIndex() + ")";
        }

        @Override
        protected String visitFunctionCall(FunctionCall node, Void context)
        {
            if ("LISTAGG".equalsIgnoreCase(node.getName().toString())) {
                return visitListagg(node);
            }

            // TODO: support slice function in duckdb & postgresql dialect
            // https://github.com/Canner/canner-metric-layer/issues/289
            if ("SLICE".equalsIgnoreCase(node.getName().toString()) && dialect.equals(BIGQUERY)) {
                return processSliceInBigQuery(node);
            }

            // TODO: `GENERATE_TIMESTAMP_ARRAY` is a bigquery function. We may need to consider the SQL dialect when Wren planning.
            if ("GENERATE_TIMESTAMP_ARRAY".equalsIgnoreCase(node.getName().toString()) && dialect.equals(DUCKDB)) {
                return processGenerateTimestampArrayInDuckDB(node);
            }

            if (dialect.equals(BIGQUERY) && "COUNT_IF".equalsIgnoreCase(node.getName().toString())) {
                return visitFunctionCall(new FunctionCall(
                        node.getLocation(),
                        QualifiedName.of("COUNTIF"),
                        node.getWindow(),
                        node.getFilter(),
                        node.getOrderBy(),
                        node.isDistinct(),
                        node.getNullTreatment(),
                        node.getProcessingMode(),
                        node.getArguments()), context);
            }

            if (dialect.equals(BIGQUERY) && "DATE_DIFF".equalsIgnoreCase(node.getName().toString())) {
                return processDateDiffInBigQuery(node, context);
            }

            StringBuilder builder = new StringBuilder();

            if (node.getProcessingMode().isPresent()) {
                builder.append(node.getProcessingMode().get().getMode())
                        .append(" ");
            }

            String arguments = joinExpressions(node.getArguments());
            if (node.getArguments().isEmpty() && "count".equalsIgnoreCase(node.getName().getSuffix())) {
                arguments = "*";
            }
            if (node.isDistinct()) {
                arguments = "DISTINCT " + arguments;
            }

            builder.append(formatName(node.getName(), dialect))
                    .append('(').append(arguments);

            // BigQuery doesn't allow null element in an array. Add IGNORE NULLS to ignore null element always.
            if (dialect.equals(BIGQUERY) &&
                    "ARRAY_AGG".equalsIgnoreCase(node.getName().toString())) {
                builder.append(" IGNORE NULLS");
            }

            if (node.getOrderBy().isPresent()) {
                builder.append(' ').append(formatOrderBy(node.getOrderBy().get(), dialect));
            }

            builder.append(')');

            node.getNullTreatment().ifPresent(nullTreatment -> {
                switch (nullTreatment) {
                    case IGNORE:
                        builder.append(" IGNORE NULLS");
                        break;
                    case RESPECT:
                        builder.append(" RESPECT NULLS");
                        break;
                }
            });

            if (!dialect.equals(BIGQUERY) && node.getFilter().isPresent()) {
                builder.append(" FILTER ").append(visitFilter(node.getFilter().get(), context));
            }

            if (node.getWindow().isPresent()) {
                builder.append(" OVER ").append(formatWindow(node.getWindow().get(), dialect));
            }

            return builder.toString();
        }

        @Override
        protected String visitWindowOperation(WindowOperation node, Void context)
        {
            return process(node.getName(), context) + " OVER " + formatWindow(node.getWindow(), dialect);
        }

        @Override
        protected String visitLambdaExpression(LambdaExpression node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append('(');
            Joiner.on(", ").appendTo(builder, node.getArguments());
            builder.append(") -> ");
            builder.append(process(node.getBody(), context));
            return builder.toString();
        }

        @Override
        protected String visitBindExpression(BindExpression node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append("\"$INTERNAL$BIND\"(");
            for (Expression value : node.getValues()) {
                builder.append(process(value, context))
                        .append(", ");
            }
            builder.append(process(node.getFunction(), context))
                    .append(")");
            return builder.toString();
        }

        @Override
        protected String visitLogicalExpression(LogicalExpression node, Void context)
        {
            return "(" +
                    node.getTerms().stream()
                            .map(term -> process(term, context))
                            .collect(Collectors.joining(" " + node.getOperator().toString() + " ")) +
                    ")";
        }

        @Override
        protected String visitNotExpression(NotExpression node, Void context)
        {
            return "(NOT " + process(node.getValue(), context) + ")";
        }

        @Override
        protected String visitComparisonExpression(ComparisonExpression node, Void context)
        {
            return formatBinaryExpression(node.getOperator().getValue(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            return "(" + process(node.getValue(), context) + " IS NULL)";
        }

        @Override
        protected String visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            return "(" + process(node.getValue(), context) + " IS NOT NULL)";
        }

        @Override
        protected String visitNullIfExpression(NullIfExpression node, Void context)
        {
            return "NULLIF(" + process(node.getFirst(), context) + ", " + process(node.getSecond(), context) + ')';
        }

        @Override
        protected String visitIfExpression(IfExpression node, Void context)
        {
            StringBuilder builder = new StringBuilder();
            builder.append("IF(")
                    .append(process(node.getCondition(), context))
                    .append(", ")
                    .append(process(node.getTrueValue(), context));
            if (node.getFalseValue().isPresent()) {
                builder.append(", ")
                        .append(process(node.getFalseValue().get(), context));
            }
            builder.append(")");
            return builder.toString();
        }

        @Override
        protected String visitTryExpression(TryExpression node, Void context)
        {
            return "TRY(" + process(node.getInnerExpression(), context) + ")";
        }

        @Override
        protected String visitCoalesceExpression(CoalesceExpression node, Void context)
        {
            return "COALESCE(" + joinExpressions(node.getOperands()) + ")";
        }

        @Override
        protected String visitArithmeticUnary(ArithmeticUnaryExpression node, Void context)
        {
            String value = process(node.getValue(), context);

            switch (node.getSign()) {
                case MINUS:
                    // Unary is ambiguous with respect to negative numbers. "-1" parses as a number, but "-(1)" parses as "unaryMinus(number)"
                    // The parentheses are needed to ensure the parsing roundtrips properly.
                    return "-(" + value + ")";
                case PLUS:
                    return "+" + value;
            }
            throw new UnsupportedOperationException("Unsupported sign: " + node.getSign());
        }

        @Override
        protected String visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            return formatBinaryExpression(node.getOperator().getValue(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitLikePredicate(LikePredicate node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append('(')
                    .append(process(node.getValue(), context))
                    .append(" LIKE ")
                    .append(process(node.getPattern(), context));

            if (dialect == DEFAULT || dialect == DUCKDB) {
                node.getEscape().ifPresent(escape -> builder.append(" ESCAPE ")
                        .append(process(escape, context)));
            }

            builder.append(')');

            return builder.toString();
        }

        @Override
        protected String visitAllColumns(AllColumns node, Void context)
        {
            StringBuilder builder = new StringBuilder();
            if (node.getTarget().isPresent()) {
                builder.append(process(node.getTarget().get(), context));
                builder.append(".*");
            }
            else {
                builder.append("*");
            }

            if (!node.getAliases().isEmpty()) {
                builder.append(" AS (");
                Joiner.on(", ").appendTo(builder, node.getAliases().stream()
                        .map(alias -> process(alias, context))
                        .collect(toList()));
                builder.append(")");
            }

            return builder.toString();
        }

        @Override
        public String visitCast(Cast node, Void context)
        {
            return (node.isSafe() ? "TRY_CAST" : "CAST") +
                    "(" + process(node.getExpression(), context) + " AS " + process(node.getType(), context) + ")";
        }

        @Override
        protected String visitSearchedCaseExpression(SearchedCaseExpression node, Void context)
        {
            ImmutableList.Builder<String> parts = ImmutableList.builder();
            parts.add("CASE");
            for (WhenClause whenClause : node.getWhenClauses()) {
                parts.add(process(whenClause, context));
            }

            node.getDefaultValue()
                    .ifPresent(value -> parts.add("ELSE").add(process(value, context)));

            parts.add("END");

            return "(" + Joiner.on(' ').join(parts.build()) + ")";
        }

        @Override
        protected String visitSimpleCaseExpression(SimpleCaseExpression node, Void context)
        {
            ImmutableList.Builder<String> parts = ImmutableList.builder();

            parts.add("CASE")
                    .add(process(node.getOperand(), context));

            for (WhenClause whenClause : node.getWhenClauses()) {
                parts.add(process(whenClause, context));
            }

            node.getDefaultValue()
                    .ifPresent(value -> parts.add("ELSE").add(process(value, context)));

            parts.add("END");

            return "(" + Joiner.on(' ').join(parts.build()) + ")";
        }

        @Override
        protected String visitWhenClause(WhenClause node, Void context)
        {
            return "WHEN " + process(node.getOperand(), context) + " THEN " + process(node.getResult(), context);
        }

        @Override
        protected String visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            return "(" + process(node.getValue(), context) + " BETWEEN " +
                    process(node.getMin(), context) + " AND " + process(node.getMax(), context) + ")";
        }

        @Override
        protected String visitInPredicate(InPredicate node, Void context)
        {
            return "(" + process(node.getValue(), context) + " IN " + process(node.getValueList(), context) + ")";
        }

        @Override
        protected String visitInListExpression(InListExpression node, Void context)
        {
            return "(" + joinExpressions(node.getValues()) + ")";
        }

        private String visitFilter(Expression node, Void context)
        {
            return "(WHERE " + process(node, context) + ')';
        }

        @Override
        protected String visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, Void context)
        {
            if (dialect.equals(BIGQUERY)) {
                // Convert PostgreSQL `= ANY` to BigQuery `IN`
                if (node.getQuantifier().equals(ANY) && node.getOperator().equals(EQUAL)) {
                    return "(" +
                            process(node.getValue(), context) +
                            " IN " +
                            process(node.getSubquery(), context) +
                            ")";
                }
            }

            return new StringBuilder()
                    .append("(")
                    .append(process(node.getValue(), context))
                    .append(' ')
                    .append(node.getOperator().getValue())
                    .append(' ')
                    .append(node.getQuantifier().toString())
                    .append(' ')
                    .append(process(node.getSubquery(), context))
                    .append(")")
                    .toString();
        }

        @Override
        protected String visitGroupingOperation(GroupingOperation node, Void context)
        {
            return "GROUPING (" + joinExpressions(node.getGroupingColumns()) + ")";
        }

        @Override
        protected String visitRowDataType(RowDataType node, Void context)
        {
            return node.getFields().stream()
                    .map(this::process)
                    .collect(joining(", ", "ROW(", ")"));
        }

        @Override
        protected String visitRowField(RowDataType.Field node, Void context)
        {
            StringBuilder result = new StringBuilder();

            if (node.getName().isPresent()) {
                result.append(process(node.getName().get(), context));
                result.append(" ");
            }

            result.append(process(node.getType(), context));

            return result.toString();
        }

        @Override
        protected String visitGenericDataType(GenericDataType node, Void context)
        {
            StringBuilder result = new StringBuilder();
            result.append(node.getName());

            if (node.getName().getCanonicalValue().equals("ARRAY")
                    && dialect == BIGQUERY) {
                result.append("<");
                result.append(process(node.getArguments().get(0), context));
                result.append(">");
                return result.toString();
            }

            // Format ARRAY<BOOLEAN> To BOOLEAN[] for DuckDB
            if (node.getName().getCanonicalValue().equals("ARRAY")
                    && dialect == DUCKDB) {
                result.delete(0, result.length());
                result.append(process(node.getArguments().get(0), context));
                result.append("[]");
                return result.toString();
            }

            if (!node.getArguments().isEmpty()) {
                result.append(node.getArguments().stream()
                        .map(this::process)
                        .collect(joining(", ", "(", ")")));
            }

            return result.toString();
        }

        @Override
        protected String visitTypeParameter(TypeParameter node, Void context)
        {
            return process(node.getValue(), context);
        }

        @Override
        protected String visitNumericTypeParameter(NumericParameter node, Void context)
        {
            return node.getValue();
        }

        @Override
        protected String visitIntervalDataType(IntervalDayTimeDataType node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append("INTERVAL ");
            builder.append(node.getFrom());
            if (node.getFrom() != node.getTo()) {
                builder.append(" TO ")
                        .append(node.getTo());
            }

            return builder.toString();
        }

        @Override
        protected String visitDateTimeType(DateTimeDataType node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append(node.getType().toString().toLowerCase(Locale.ENGLISH)); // TODO: normalize to upper case according to standard SQL semantics
            if (node.getPrecision().isPresent()) {
                builder.append("(")
                        .append(node.getPrecision().get())
                        .append(")");
            }

            if (node.isWithTimeZone()) {
                builder.append(" with time zone"); // TODO: normalize to upper case according to standard SQL semantics
            }

            return builder.toString();
        }

        @Override
        protected String visitLabelDereference(LabelDereference node, Void context)
        {
            // format LabelDereference L.x as "LABEL_DEREFERENCE("L", "x")"
            // LabelDereference, like SymbolReference, is an IR-type expression. It is never a result of the parser.
            // After being formatted this way for serialization, it will be parsed as functionCall
            // and swapped back for LabelDereference.
            return "LABEL_DEREFERENCE(" + formatIdentifier(node.getLabel(), dialect) + ", " + node.getReference().map(this::process).orElse("*") + ")";
        }

        private String formatBinaryExpression(String operator, Expression left, Expression right)
        {
            return '(' + process(left, null) + ' ' + operator + ' ' + process(right, null) + ')';
        }

        private String joinExpressions(List<Expression> expressions)
        {
            return Joiner.on(", ").join(expressions.stream()
                    .map((e) -> process(e, null))
                    .iterator());
        }

        /**
         * Returns the formatted `LISTAGG` function call corresponding to the specified node.
         * <p>
         * During the parsing of the syntax tree, the `LISTAGG` expression is synthetically converted
         * to a function call. This method formats the specified {@link FunctionCall} node to correspond
         * to the standardised syntax of the `LISTAGG` expression.
         *
         * @param node the `LISTAGG` function call
         */
        private String visitListagg(FunctionCall node)
        {
            StringBuilder builder = new StringBuilder();

            List<Expression> arguments = node.getArguments();
            Expression expression = arguments.get(0);
            Expression separator = arguments.get(1);
            BooleanLiteral overflowError = (BooleanLiteral) arguments.get(2);
            Expression overflowFiller = arguments.get(3);
            BooleanLiteral showOverflowEntryCount = (BooleanLiteral) arguments.get(4);

            String innerArguments = joinExpressions(ImmutableList.of(expression, separator));
            if (node.isDistinct()) {
                innerArguments = "DISTINCT " + innerArguments;
            }

            builder.append("LISTAGG")
                    .append('(').append(innerArguments);

            builder.append(" ON OVERFLOW ");
            if (overflowError.getValue()) {
                builder.append(" ERROR");
            }
            else {
                builder.append(" TRUNCATE")
                        .append(' ')
                        .append(process(overflowFiller, null));
                if (showOverflowEntryCount.getValue()) {
                    builder.append(" WITH COUNT");
                }
                else {
                    builder.append(" WITHOUT COUNT");
                }
            }

            builder.append(')');

            if (node.getOrderBy().isPresent()) {
                builder.append(" WITHIN GROUP ")
                        .append('(')
                        .append(formatOrderBy(node.getOrderBy().get(), dialect))
                        .append(')');
            }

            return builder.toString();
        }

        private String processSliceInBigQuery(FunctionCall node)
        {
            List<Expression> arguments = node.getArguments();
            Expression expression = arguments.get(0);
            LongLiteral start = (LongLiteral) arguments.get(1);
            LongLiteral length = (LongLiteral) arguments.get(2);
            return format("ARRAY(SELECT p FROM UNNEST(%s) p WITH OFFSET index WHERE index BETWEEN %s AND %s ORDER BY index)",
                    process(expression),
                    // bigquery use zero-based indexes
                    start.getValue() - 1,
                    start.getValue() - 1 + length.getValue());
        }

        private String processGenerateTimestampArrayInDuckDB(FunctionCall node)
        {
            List<Expression> arguments = node.getArguments();
            Expression start = arguments.get(0);
            Expression end = arguments.get(1);
            return format("GENERATE_SERIES(%s, %s, INTERVAL 1 DAY)",
                    start,
                    end);
        }

        private String processDateDiffInBigQuery(FunctionCall node, Void context)
        {
            checkArgument(node.getArguments().size() == 3, "DATE_DIFF function should have 3 arguments");
            List<Expression> arguments = node.getArguments();
            StringLiteral datePart = (StringLiteral) arguments.get(0);
            Expression start = arguments.get(1);
            Expression end = arguments.get(2);
            // In BigQuery, the formula is `start - end` but it's `end - start` in trino.
            return format("TIMESTAMP_DIFF(%s, %s, %s)",
                    process(end, context),
                    process(start, context),
                    datePart.getValue());
        }
    }

    static String formatStringLiteral(String s, Dialect dialect)
    {
        if (dialect == BIGQUERY) {
            s = s.replace("'", "\\'");
        }
        else {
            s = s.replace("'", "''");
        }
        if (CharMatcher.inRange((char) 0x20, (char) 0x7E).matchesAllOf(s)) {
            return "'" + s + "'";
        }

        StringBuilder builder = new StringBuilder();
        if (dialect == DEFAULT || dialect == DUCKDB || dialect == POSTGRES) {
            builder.append("U&");
        }
        builder.append("'");
        PrimitiveIterator.OfInt iterator = s.codePoints().iterator();
        while (iterator.hasNext()) {
            int codePoint = iterator.nextInt();
            checkArgument(codePoint >= 0, "Invalid UTF-8 encoding in characters: %s", s);
            if (isAsciiPrintable(codePoint)) {
                char ch = (char) codePoint;
                if (ch == '\\') {
                    builder.append(ch);
                }
                builder.append(ch);
            }
            else {
                if (dialect == DEFAULT || dialect == DUCKDB || dialect == POSTGRES) {
                    if (codePoint <= 0xFFFF) {
                        builder.append('\\');
                        builder.append(format("%04X", codePoint));
                    }
                    else {
                        builder.append("\\+");
                        builder.append(format("%06X", codePoint));
                    }
                }
                else if (dialect == BIGQUERY) {
                    if (codePoint <= 0xFFFF) {
                        builder.append('\\');
                        builder.append('u');
                        builder.append(format("%04X", codePoint));
                    }
                    else {
                        builder.append("\\");
                        builder.append('U');
                        builder.append(format("%08X", codePoint));
                    }
                }
            }
        }
        builder.append("'");
        return builder.toString();
    }

    public static String formatOrderBy(OrderBy orderBy, Dialect dialect)
    {
        return "ORDER BY " + formatSortItems(orderBy.getSortItems(), dialect);
    }

    private static String formatSortItems(List<SortItem> sortItems, Dialect dialect)
    {
        return Joiner.on(", ").join(sortItems.stream()
                .map(sortItemFormatterFunction(dialect))
                .iterator());
    }

    private static String formatWindow(Window window, Dialect dialect)
    {
        if (window instanceof WindowReference) {
            return formatExpression(((WindowReference) window).getName(), dialect);
        }

        return formatWindowSpecification((WindowSpecification) window, dialect);
    }

    static String formatWindowSpecification(WindowSpecification windowSpecification, Dialect dialect)
    {
        List<String> parts = new ArrayList<>();

        if (windowSpecification.getExistingWindowName().isPresent()) {
            parts.add(formatExpression(windowSpecification.getExistingWindowName().get(), dialect));
        }
        if (!windowSpecification.getPartitionBy().isEmpty()) {
            parts.add("PARTITION BY " + windowSpecification.getPartitionBy().stream()
                    .map(expression -> formatExpression(expression, dialect))
                    .collect(joining(", ")));
        }
        if (windowSpecification.getOrderBy().isPresent()) {
            parts.add(formatOrderBy(windowSpecification.getOrderBy().get(), dialect));
        }
        if (windowSpecification.getFrame().isPresent()) {
            parts.add(formatFrame(windowSpecification.getFrame().get(), dialect));
        }

        return '(' + Joiner.on(' ').join(parts) + ')';
    }

    private static String formatFrame(WindowFrame windowFrame, Dialect dialect)
    {
        StringBuilder builder = new StringBuilder();

        if (!windowFrame.getMeasures().isEmpty()) {
            builder.append("MEASURES ")
                    .append(windowFrame.getMeasures().stream()
                            .map(measure -> formatExpression(measure.getExpression(), dialect) + " AS " + formatExpression(measure.getName(), dialect))
                            .collect(joining(", ")))
                    .append(" ");
        }

        builder.append(windowFrame.getType().toString())
                .append(' ');

        if (windowFrame.getEnd().isPresent()) {
            builder.append("BETWEEN ")
                    .append(formatFrameBound(windowFrame.getStart(), dialect))
                    .append(" AND ")
                    .append(formatFrameBound(windowFrame.getEnd().get(), dialect));
        }
        else {
            builder.append(formatFrameBound(windowFrame.getStart(), dialect));
        }

        windowFrame.getAfterMatchSkipTo().ifPresent(skipTo ->
                builder.append(" ")
                        .append(formatSkipTo(skipTo, dialect)));
        windowFrame.getPatternSearchMode().ifPresent(searchMode ->
                builder.append(" ")
                        .append(searchMode.getMode().name()));
        windowFrame.getPattern().ifPresent(pattern ->
                builder.append(" PATTERN(")
                        .append(formatPattern(pattern, dialect))
                        .append(")"));
        if (!windowFrame.getSubsets().isEmpty()) {
            builder.append(" SUBSET ");
            builder.append(windowFrame.getSubsets().stream()
                    .map(subset -> formatExpression(subset.getName(), dialect) + " = " + subset.getIdentifiers().stream()
                            .map(expression -> formatExpression(expression, dialect)).collect(joining(", ", "(", ")")))
                    .collect(joining(", ")));
        }
        if (!windowFrame.getVariableDefinitions().isEmpty()) {
            builder.append(" DEFINE ");
            builder.append(windowFrame.getVariableDefinitions().stream()
                    .map(variable -> formatExpression(variable.getName(), dialect) + " AS " + formatExpression(variable.getExpression(), dialect))
                    .collect(joining(", ")));
        }

        return builder.toString();
    }

    private static String formatFrameBound(FrameBound frameBound, Dialect dialect)
    {
        switch (frameBound.getType()) {
            case UNBOUNDED_PRECEDING:
                return "UNBOUNDED PRECEDING";
            case PRECEDING:
                return formatExpression(frameBound.getValue().get(), dialect) + " PRECEDING";
            case CURRENT_ROW:
                return "CURRENT ROW";
            case FOLLOWING:
                return formatExpression(frameBound.getValue().get(), dialect) + " FOLLOWING";
            case UNBOUNDED_FOLLOWING:
                return "UNBOUNDED FOLLOWING";
        }
        throw new IllegalArgumentException("unhandled type: " + frameBound.getType());
    }

    public static String formatSkipTo(SkipTo skipTo, Dialect dialect)
    {
        switch (skipTo.getPosition()) {
            case PAST_LAST:
                return "AFTER MATCH SKIP PAST LAST ROW";
            case NEXT:
                return "AFTER MATCH SKIP TO NEXT ROW";
            case LAST:
                checkState(skipTo.getIdentifier().isPresent(), "missing identifier in AFTER MATCH SKIP TO LAST");
                return "AFTER MATCH SKIP TO LAST " + formatExpression(skipTo.getIdentifier().get(), dialect);
            case FIRST:
                checkState(skipTo.getIdentifier().isPresent(), "missing identifier in AFTER MATCH SKIP TO FIRST");
                return "AFTER MATCH SKIP TO FIRST " + formatExpression(skipTo.getIdentifier().get(), dialect);
            default:
                throw new IllegalStateException("unexpected skipTo: " + skipTo);
        }
    }

    static String formatGroupBy(List<GroupingElement> groupingElements, Dialect dialect)
    {
        ImmutableList.Builder<String> resultStrings = ImmutableList.builder();

        for (GroupingElement groupingElement : groupingElements) {
            String result = "";
            if (groupingElement instanceof SimpleGroupBy) {
                List<Expression> columns = groupingElement.getExpressions();
                if (columns.size() == 1) {
                    result = formatExpression(getOnlyElement(columns), dialect);
                }
                else {
                    result = formatGroupingSet(columns, dialect);
                }
            }
            else if (groupingElement instanceof GroupingSets) {
                result = format("GROUPING SETS (%s)", Joiner.on(", ").join(
                        ((GroupingSets) groupingElement).getSets().stream()
                                .map(expression -> formatGroupingSet(expression, dialect))
                                .iterator()));
            }
            else if (groupingElement instanceof Cube) {
                result = format("CUBE %s", formatGroupingSet(groupingElement.getExpressions(), dialect));
            }
            else if (groupingElement instanceof Rollup) {
                result = format("ROLLUP %s", formatGroupingSet(groupingElement.getExpressions(), dialect));
            }
            resultStrings.add(result);
        }
        return Joiner.on(", ").join(resultStrings.build());
    }

    private static boolean isAsciiPrintable(int codePoint)
    {
        return codePoint >= 0x20 && codePoint < 0x7F;
    }

    private static String formatGroupingSet(List<Expression> groupingSet, Dialect dialect)
    {
        return format("(%s)", Joiner.on(", ").join(groupingSet.stream()
                .map(expression -> formatExpression(expression, dialect))
                .iterator()));
    }

    private static Function<SortItem, String> sortItemFormatterFunction(Dialect dialect)
    {
        return input -> {
            StringBuilder builder = new StringBuilder();

            builder.append(formatExpression(input.getSortKey(), dialect));

            switch (input.getOrdering()) {
                case ASCENDING:
                    builder.append(" ASC");
                    break;
                case DESCENDING:
                    builder.append(" DESC");
                    break;
                default:
                    throw new UnsupportedOperationException("unknown ordering: " + input.getOrdering());
            }

            switch (input.getNullOrdering()) {
                case FIRST:
                    builder.append(" NULLS FIRST");
                    break;
                case LAST:
                    builder.append(" NULLS LAST");
                    break;
                case UNDEFINED:
                    // no op
                    break;
                default:
                    throw new UnsupportedOperationException("unknown null ordering: " + input.getNullOrdering());
            }

            return builder.toString();
        };
    }
}
