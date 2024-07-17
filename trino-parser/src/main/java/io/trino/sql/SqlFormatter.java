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
package io.trino.sql;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import io.trino.sql.tree.AddColumn;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.Analyze;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Call;
import io.trino.sql.tree.CallArgument;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.Comment;
import io.trino.sql.tree.Commit;
import io.trino.sql.tree.CreateMaterializedView;
import io.trino.sql.tree.CreateRole;
import io.trino.sql.tree.CreateSchema;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.CreateTableAsSelect;
import io.trino.sql.tree.CreateView;
import io.trino.sql.tree.Deallocate;
import io.trino.sql.tree.Declare;
import io.trino.sql.tree.Delete;
import io.trino.sql.tree.Deny;
import io.trino.sql.tree.DescribeInput;
import io.trino.sql.tree.DescribeOutput;
import io.trino.sql.tree.DropColumn;
import io.trino.sql.tree.DropMaterializedView;
import io.trino.sql.tree.DropRole;
import io.trino.sql.tree.DropSchema;
import io.trino.sql.tree.DropTable;
import io.trino.sql.tree.DropView;
import io.trino.sql.tree.Except;
import io.trino.sql.tree.Execute;
import io.trino.sql.tree.Explain;
import io.trino.sql.tree.ExplainAnalyze;
import io.trino.sql.tree.ExplainFormat;
import io.trino.sql.tree.ExplainOption;
import io.trino.sql.tree.ExplainType;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FetchCursor;
import io.trino.sql.tree.FetchFirst;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.FunctionRelation;
import io.trino.sql.tree.Grant;
import io.trino.sql.tree.GrantRoles;
import io.trino.sql.tree.GrantorSpecification;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.ImpersonateUser;
import io.trino.sql.tree.Insert;
import io.trino.sql.tree.Intersect;
import io.trino.sql.tree.Isolation;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.JoinCriteria;
import io.trino.sql.tree.JoinOn;
import io.trino.sql.tree.JoinUsing;
import io.trino.sql.tree.Lateral;
import io.trino.sql.tree.LikeClause;
import io.trino.sql.tree.Limit;
import io.trino.sql.tree.Merge;
import io.trino.sql.tree.MergeCase;
import io.trino.sql.tree.MergeDelete;
import io.trino.sql.tree.MergeInsert;
import io.trino.sql.tree.MergeUpdate;
import io.trino.sql.tree.NaturalJoin;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Offset;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.PathRelation;
import io.trino.sql.tree.PatternRecognitionRelation;
import io.trino.sql.tree.Prepare;
import io.trino.sql.tree.PrincipalSpecification;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QueryPeriod;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.RefreshMaterializedView;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.RenameColumn;
import io.trino.sql.tree.RenameMaterializedView;
import io.trino.sql.tree.RenameSchema;
import io.trino.sql.tree.RenameTable;
import io.trino.sql.tree.RenameView;
import io.trino.sql.tree.ResetSession;
import io.trino.sql.tree.Revoke;
import io.trino.sql.tree.RevokeRoles;
import io.trino.sql.tree.Rollback;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.RowPattern;
import io.trino.sql.tree.SampledRelation;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SetPath;
import io.trino.sql.tree.SetProperties;
import io.trino.sql.tree.SetRole;
import io.trino.sql.tree.SetSchemaAuthorization;
import io.trino.sql.tree.SetSession;
import io.trino.sql.tree.SetTableAuthorization;
import io.trino.sql.tree.SetTimeZone;
import io.trino.sql.tree.SetViewAuthorization;
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
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.StartTransaction;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableExecute;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.TransactionAccessMode;
import io.trino.sql.tree.TransactionMode;
import io.trino.sql.tree.TruncateTable;
import io.trino.sql.tree.Union;
import io.trino.sql.tree.Unnest;
import io.trino.sql.tree.Update;
import io.trino.sql.tree.UpdateAssignment;
import io.trino.sql.tree.Values;
import io.trino.sql.tree.WithQuery;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.transform;
import static io.trino.sql.ExpressionFormatter.formatExpression;
import static io.trino.sql.ExpressionFormatter.formatGroupBy;
import static io.trino.sql.ExpressionFormatter.formatOrderBy;
import static io.trino.sql.ExpressionFormatter.formatSkipTo;
import static io.trino.sql.ExpressionFormatter.formatStringLiteral;
import static io.trino.sql.ExpressionFormatter.formatWindowSpecification;
import static io.trino.sql.RowPatternFormatter.formatPattern;
import static io.trino.sql.SqlFormatter.Dialect.BIGQUERY;
import static io.trino.sql.SqlFormatter.Dialect.DEFAULT;
import static io.trino.sql.SqlFormatter.Dialect.POSTGRES;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public final class SqlFormatter
{
    private static final String INDENT = "   ";

    public enum Dialect
    {
        DEFAULT,
        BIGQUERY,
        DUCKDB,
        POSTGRES
    }

    private SqlFormatter() {}

    public static String formatSql(Node root)
    {
        return formatSql(root, DEFAULT);
    }

    public static String formatSql(Node root, Dialect dialect)
    {
        StringBuilder builder = new StringBuilder();
        new Formatter(builder, dialect).process(root, 0);
        return builder.toString();
    }

    static String formatName(QualifiedName name, Dialect dialect)
    {
        return name.getOriginalParts().stream()
                .map(expression -> formatExpression(expression, dialect))
                .collect(joining("."));
    }

    private static class Formatter
            extends AstVisitor<Void, Integer>
    {
        private final StringBuilder builder;
        private final Dialect dialect;

        public Formatter(StringBuilder builder, Dialect dialect)
        {
            this.builder = builder;
            this.dialect = dialect;
        }

        @Override
        protected Void visitNode(Node node, Integer indent)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        protected Void visitExpression(Expression node, Integer indent)
        {
            checkArgument(indent == 0, "visitExpression should only be called at root");
            builder.append(formatExpression(node, dialect));
            return null;
        }

        @Override
        protected Void visitRowPattern(RowPattern node, Integer indent)
        {
            checkArgument(indent == 0, "visitRowPattern should only be called at root");
            builder.append(formatPattern(node, dialect));
            return null;
        }

        @Override
        protected Void visitFunctionRelation(FunctionRelation node, Integer indent)
        {
            builder.append(formatName(node.getName(), dialect))
                    .append("(")
                    .append(node.getArguments().stream()
                            .map(expression -> formatExpression(expression, dialect))
                            .collect(joining(", ")))
                    .append(")");
            return null;
        }

        @Override
        protected Void visitPathRelation(PathRelation node, Integer context)
        {
            builder.append(node.getPath());
            return null;
        }

        @Override
        protected Void visitUnnest(Unnest node, Integer indent)
        {
            // Postgres doesn't have `generate_array` function and `generate_series` is a table function.
            // Use `generate_series` to instead `UNNEST(generate_array(...))` for Postgres.
            if (dialect == POSTGRES &&
                    node.getExpressions().size() == 1 &&
                    node.getExpressions().get(0) instanceof FunctionCall &&
                    ((FunctionCall) node.getExpressions().get(0)).getName().equals(QualifiedName.of("generate_array"))) {
                builder.append("generate_series(")
                        .append(((FunctionCall) node.getExpressions().get(0)).getArguments().stream()
                                .map(expression -> formatExpression(expression, dialect))
                                .collect(joining(", ")))
                        .append(")");
                return null;
            }

            builder.append("UNNEST(")
                    .append(node.getExpressions().stream()
                            .map(expression -> formatExpression(expression, dialect))
                            .collect(joining(", ")))
                    .append(")");
            if (node.isWithOrdinality()) {
                builder.append(" WITH ORDINALITY");
            }
            return null;
        }

        @Override
        protected Void visitLateral(Lateral node, Integer indent)
        {
            append(indent, "LATERAL (");
            process(node.getQuery(), indent + 1);
            append(indent, ")");
            return null;
        }

        @Override
        protected Void visitPrepare(Prepare node, Integer indent)
        {
            append(indent, "PREPARE ");
            builder.append(node.getName());
            builder.append(" FROM");
            builder.append("\n");
            process(node.getStatement(), indent + 1);
            return null;
        }

        @Override
        protected Void visitDeallocate(Deallocate node, Integer indent)
        {
            append(indent, "DEALLOCATE PREPARE ");
            builder.append(node.getName());
            return null;
        }

        // pg syntax
        @Override
        protected Void visitDeclareCursor(Declare node, Integer ident)
        {
            append(ident, "DECLARE ");
            builder.append(node.getName());
            builder.append(" CURSOR FOR ");
            process(node.getBody(), ident + 1);
            return null;
        }

        // pg syntax
        @Override
        protected Void visitFetchCursor(FetchCursor node, Integer ident)
        {
            append(ident, "FETCH ")
                    .append(node.getRowCount())
                    .append(" FROM ")
                    .append(node.getCursor());
            return null;
        }

        @Override
        protected Void visitImpersonateUser(ImpersonateUser node, Integer ident)
        {
            append(ident, "IMPERSONATE ")
                    .append(node.getExpression());
            return null;
        }

        @Override
        protected Void visitExecute(Execute node, Integer indent)
        {
            append(indent, "EXECUTE ");
            builder.append(node.getName());
            List<Expression> parameters = node.getParameters();
            if (!parameters.isEmpty()) {
                builder.append(" USING ");
                Joiner.on(", ").appendTo(builder, parameters);
            }
            return null;
        }

        @Override
        protected Void visitDescribeOutput(DescribeOutput node, Integer indent)
        {
            append(indent, "DESCRIBE OUTPUT ");
            builder.append(node.getName());
            return null;
        }

        @Override
        protected Void visitDescribeInput(DescribeInput node, Integer indent)
        {
            append(indent, "DESCRIBE INPUT ");
            builder.append(node.getName());
            return null;
        }

        @Override
        protected Void visitQuery(Query node, Integer indent)
        {
            node.getWith().ifPresent(with -> {
                append(indent, "WITH");
                if (with.isRecursive()) {
                    builder.append(" RECURSIVE");
                }
                builder.append("\n  ");
                Iterator<WithQuery> queries = with.getQueries().iterator();
                while (queries.hasNext()) {
                    WithQuery query = queries.next();
                    append(indent, formatExpression(query.getName(), dialect));
                    query.getColumnNames().ifPresent(columnNames -> appendAliasColumns(builder, columnNames, dialect));
                    builder.append(" AS ");
                    process(new TableSubquery(query.getQuery()), indent);
                    builder.append('\n');
                    if (queries.hasNext()) {
                        builder.append(", ");
                    }
                }
            });

            processRelation(node.getQueryBody(), indent);
            node.getOrderBy().ifPresent(orderBy -> process(orderBy, indent));
            if (dialect.equals(BIGQUERY)) {
                node.getLimit().ifPresent(limit -> process(limit, indent));
                node.getOffset().ifPresent(offset -> process(offset, indent));
            }
            else {
                node.getOffset().ifPresent(offset -> process(offset, indent));
                node.getLimit().ifPresent(limit -> process(limit, indent));
            }
            return null;
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, Integer indent)
        {
            process(node.getSelect(), indent);

            node.getFrom().ifPresent(from -> {
                append(indent, "FROM");
                builder.append('\n');
                append(indent, "  ");
                process(from, indent);
            });

            builder.append('\n');

            node.getWhere().ifPresent(where ->
                    append(indent, "WHERE " + formatExpression(where, dialect)).append('\n'));

            node.getGroupBy().ifPresent(groupBy ->
                    append(indent, "GROUP BY " + (groupBy.isDistinct() ? " DISTINCT " : "") + formatGroupBy(groupBy.getGroupingElements(), dialect)).append('\n'));

            node.getHaving().ifPresent(having -> append(indent, "HAVING " + formatExpression(having, dialect))
                    .append('\n'));

            if (!node.getWindows().isEmpty()) {
                append(indent, "WINDOW");
                formatDefinitionList(node.getWindows().stream()
                        .map(definition -> formatExpression(definition.getName(), dialect) + " AS " + formatWindowSpecification(definition.getWindow(), dialect))
                        .collect(toImmutableList()), indent + 1);
            }

            node.getOrderBy().ifPresent(orderBy -> process(orderBy, indent));
            if (dialect.equals(BIGQUERY)) {
                node.getLimit().ifPresent(limit -> process(limit, indent));
                node.getOffset().ifPresent(offset -> process(offset, indent));
            }
            else {
                node.getOffset().ifPresent(offset -> process(offset, indent));
                node.getLimit().ifPresent(limit -> process(limit, indent));
            }
            return null;
        }

        @Override
        protected Void visitOrderBy(OrderBy node, Integer indent)
        {
            append(indent, formatOrderBy(node, dialect))
                    .append('\n');
            return null;
        }

        @Override
        protected Void visitOffset(Offset node, Integer indent)
        {
            append(indent, "OFFSET ")
                    .append(formatExpression(node.getRowCount(), dialect))
                    .append("\n");
            if (!dialect.equals(BIGQUERY)) {
                append(indent, "ROWS\n");
            }
            return null;
        }

        @Override
        protected Void visitFetchFirst(FetchFirst node, Integer indent)
        {
            append(indent, "FETCH FIRST " + node.getRowCount().map(count -> formatExpression(count, dialect) + " ROWS ").orElse("ROW "))
                    .append(node.isWithTies() ? "WITH TIES" : "ONLY")
                    .append('\n');
            return null;
        }

        @Override
        protected Void visitLimit(Limit node, Integer indent)
        {
            append(indent, "LIMIT ")
                    .append(formatExpression(node.getRowCount(), dialect))
                    .append('\n');
            return null;
        }

        @Override
        protected Void visitSelect(Select node, Integer indent)
        {
            append(indent, "SELECT");
            if (node.isDistinct()) {
                builder.append(" DISTINCT");
            }

            if (node.getSelectItems().size() > 1) {
                boolean first = true;
                for (SelectItem item : node.getSelectItems()) {
                    builder.append("\n")
                            .append(indentString(indent))
                            .append(first ? "  " : ", ");

                    process(item, indent);
                    first = false;
                }
            }
            else {
                builder.append(' ');
                process(getOnlyElement(node.getSelectItems()), indent);
            }

            builder.append('\n');

            return null;
        }

        @Override
        protected Void visitSingleColumn(SingleColumn node, Integer indent)
        {
            builder.append(formatExpression(node.getExpression(), dialect));
            node.getAlias().ifPresent(alias -> builder
                    .append(' ')
                    .append(formatExpression(alias, dialect)));

            return null;
        }

        @Override
        protected Void visitAllColumns(AllColumns node, Integer indent)
        {
            node.getTarget().ifPresent(value -> builder
                    .append(formatExpression(value, dialect))
                    .append("."));
            builder.append("*");

            if (!node.getAliases().isEmpty()) {
                builder.append(" AS (")
                        .append(Joiner.on(", ").join(node.getAliases().stream()
                                .map(expression -> formatExpression(expression, dialect))
                                .collect(toImmutableList())))
                        .append(")");
            }

            return null;
        }

        @Override
        protected Void visitTable(Table node, Integer indent)
        {
            builder.append(formatName(node.getName(), dialect));
            node.getQueryPeriod().ifPresent(queryPeriod -> builder
                    .append(" " + queryPeriod));
            return null;
        }

        @Override
        protected Void visitQueryPeriod(QueryPeriod node, Integer indent)
        {
            builder.append("FOR " + node.getRangeType().name() + " AS OF " + formatExpression(node.getEnd().get(), dialect));
            return null;
        }

        @Override
        protected Void visitJoin(Join node, Integer indent)
        {
            JoinCriteria criteria = node.getCriteria().orElse(null);
            String type = node.getType().toString();
            if (criteria instanceof NaturalJoin) {
                type = "NATURAL " + type;
            }

            process(node.getLeft(), indent);

            builder.append('\n');
            if (node.getType() == Join.Type.IMPLICIT) {
                append(indent, ", ");
            }
            else {
                append(indent, type).append(" JOIN ");
            }

            process(node.getRight(), indent);

            if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
                if (criteria instanceof JoinUsing) {
                    JoinUsing using = (JoinUsing) criteria;
                    builder.append(" USING (")
                            .append(Joiner.on(", ").join(using.getColumns()))
                            .append(")");
                }
                else if (criteria instanceof JoinOn) {
                    JoinOn on = (JoinOn) criteria;
                    builder.append(" ON ")
                            .append(formatExpression(on.getExpression(), dialect));
                }
                else if (!(criteria instanceof NaturalJoin)) {
                    throw new UnsupportedOperationException("unknown join criteria: " + criteria);
                }
            }

            return null;
        }

        @Override
        protected Void visitAliasedRelation(AliasedRelation node, Integer indent)
        {
            processRelationSuffix(node.getRelation(), indent);

            builder.append(' ')
                    .append(formatExpression(node.getAlias(), dialect));
            appendAliasColumns(builder, node.getColumnNames(), dialect);

            return null;
        }

        @Override
        protected Void visitPatternRecognitionRelation(PatternRecognitionRelation node, Integer indent)
        {
            processRelationSuffix(node.getInput(), indent);

            builder.append(" MATCH_RECOGNIZE (\n");
            if (!node.getPartitionBy().isEmpty()) {
                append(indent + 1, "PARTITION BY ")
                        .append(node.getPartitionBy().stream()
                                .map(expression -> formatExpression(expression, dialect))
                                .collect(joining(", ")))
                        .append("\n");
            }
            node.getOrderBy().ifPresent(orderBy -> process(orderBy, indent + 1));

            if (!node.getMeasures().isEmpty()) {
                append(indent + 1, "MEASURES");
                formatDefinitionList(node.getMeasures().stream()
                        .map(measure -> formatExpression(measure.getExpression(), dialect) + " AS " + formatExpression(measure.getName(), dialect))
                        .collect(toImmutableList()), indent + 2);
            }

            node.getRowsPerMatch().ifPresent(rowsPerMatch -> {
                String rowsPerMatchDescription;
                switch (rowsPerMatch) {
                    case ONE:
                        rowsPerMatchDescription = "ONE ROW PER MATCH";
                        break;
                    case ALL_SHOW_EMPTY:
                        rowsPerMatchDescription = "ALL ROWS PER MATCH SHOW EMPTY MATCHES";
                        break;
                    case ALL_OMIT_EMPTY:
                        rowsPerMatchDescription = "ALL ROWS PER MATCH OMIT EMPTY MATCHES";
                        break;
                    case ALL_WITH_UNMATCHED:
                        rowsPerMatchDescription = "ALL ROWS PER MATCH WITH UNMATCHED ROWS";
                        break;
                    default:
                        // RowsPerMatch of type WINDOW cannot occur in MATCH_RECOGNIZE clause
                        throw new IllegalStateException("unexpected rowsPerMatch: " + node.getRowsPerMatch().get());
                }
                append(indent + 1, rowsPerMatchDescription)
                        .append("\n");
            });

            node.getAfterMatchSkipTo().ifPresent(afterMatchSkipTo -> {
                String skipTo = formatSkipTo(afterMatchSkipTo, dialect);
                append(indent + 1, skipTo)
                        .append("\n");
            });

            node.getPatternSearchMode().ifPresent(patternSearchMode ->
                    append(indent + 1, patternSearchMode.getMode().name())
                            .append("\n"));

            append(indent + 1, "PATTERN (")
                    .append(formatPattern(node.getPattern(), dialect))
                    .append(")\n");
            if (!node.getSubsets().isEmpty()) {
                append(indent + 1, "SUBSET");
                formatDefinitionList(node.getSubsets().stream()
                        .map(subset -> formatExpression(subset.getName(), dialect) + " = " + subset.getIdentifiers().stream()
                                .map(expression -> formatExpression(expression, dialect)).collect(joining(", ", "(", ")")))
                        .collect(toImmutableList()), indent + 2);
            }
            append(indent + 1, "DEFINE");
            formatDefinitionList(node.getVariableDefinitions().stream()
                    .map(variable -> formatExpression(variable.getName(), dialect) + " AS " + formatExpression(variable.getExpression(), dialect))
                    .collect(toImmutableList()), indent + 2);

            builder.append(")");

            return null;
        }

        @Override
        protected Void visitSampledRelation(SampledRelation node, Integer indent)
        {
            processRelationSuffix(node.getRelation(), indent);

            builder.append(" TABLESAMPLE ")
                    .append(node.getType())
                    .append(" (")
                    .append(node.getSamplePercentage())
                    .append(')');

            return null;
        }

        private void processRelationSuffix(Relation relation, Integer indent)
        {
            if ((relation instanceof AliasedRelation) || (relation instanceof SampledRelation) || (relation instanceof PatternRecognitionRelation) || (relation instanceof Join)) {
                builder.append("( ");
                process(relation, indent + 1);
                append(indent, ")");
            }
            else {
                process(relation, indent);
            }
        }

        @Override
        protected Void visitValues(Values node, Integer indent)
        {
            builder.append(" VALUES ");

            boolean first = true;
            for (Expression row : node.getRows()) {
                builder.append("\n")
                        .append(indentString(indent))
                        .append(first ? "  " : ", ");
                if (row instanceof Row) {
                    builder.append(formatExpression(row, dialect));
                }
                else {
                    builder.append("(")
                            .append(formatExpression(row, dialect))
                            .append(")");
                }
                first = false;
            }
            builder.append('\n');

            return null;
        }

        @Override
        protected Void visitTableSubquery(TableSubquery node, Integer indent)
        {
            builder.append('(')
                    .append('\n');

            process(node.getQuery(), indent + 1);

            append(indent, ") ");

            return null;
        }

        @Override
        protected Void visitUnion(Union node, Integer indent)
        {
            Iterator<Relation> relations = node.getRelations().iterator();

            while (relations.hasNext()) {
                processRelation(relations.next(), indent);

                if (relations.hasNext()) {
                    builder.append("UNION ");
                    if (!node.isDistinct()) {
                        builder.append("ALL ");
                    }
                }
            }

            return null;
        }

        @Override
        protected Void visitExcept(Except node, Integer indent)
        {
            processRelation(node.getLeft(), indent);

            builder.append("EXCEPT ");
            if (!node.isDistinct()) {
                builder.append("ALL ");
            }

            processRelation(node.getRight(), indent);

            return null;
        }

        @Override
        protected Void visitIntersect(Intersect node, Integer indent)
        {
            Iterator<Relation> relations = node.getRelations().iterator();

            while (relations.hasNext()) {
                processRelation(relations.next(), indent);

                if (relations.hasNext()) {
                    builder.append("INTERSECT ");
                    if (!node.isDistinct()) {
                        builder.append("ALL ");
                    }
                }
            }

            return null;
        }

        @Override
        protected Void visitMerge(Merge node, Integer indent)
        {
            builder.append("MERGE INTO ")
                    .append(node.getTable().getName());

            node.getTargetAlias().ifPresent(value -> builder
                    .append(' ')
                    .append(value));
            builder.append("\n");

            append(indent + 1, "USING ");

            processRelation(node.getRelation(), indent + 2);

            builder.append("\n");
            append(indent + 1, "ON ");
            builder.append(formatExpression(node.getExpression(), dialect));

            for (MergeCase mergeCase : node.getMergeCases()) {
                builder.append("\n");
                process(mergeCase, indent);
            }

            return null;
        }

        @Override
        protected Void visitMergeInsert(MergeInsert node, Integer indent)
        {
            appendMergeCaseWhen(false, node.getExpression());
            append(indent + 1, "THEN INSERT ");

            if (!node.getColumns().isEmpty()) {
                builder.append("(");
                Joiner.on(", ").appendTo(builder, node.getColumns());
                builder.append(")");
            }

            builder.append("VALUES (");
            Joiner.on(", ").appendTo(builder, transform(node.getValues(), expression -> formatExpression(expression, dialect)));
            builder.append(")");

            return null;
        }

        @Override
        protected Void visitMergeUpdate(MergeUpdate node, Integer indent)
        {
            appendMergeCaseWhen(true, node.getExpression());
            append(indent + 1, "THEN UPDATE SET");

            boolean first = true;
            for (MergeUpdate.Assignment assignment : node.getAssignments()) {
                builder.append("\n");
                append(indent + 1, first ? "  " : ", ");
                builder.append(assignment.getTarget())
                        .append(" = ")
                        .append(formatExpression(assignment.getValue(), dialect));
                first = false;
            }

            return null;
        }

        @Override
        protected Void visitMergeDelete(MergeDelete node, Integer indent)
        {
            appendMergeCaseWhen(true, node.getExpression());
            append(indent + 1, "THEN DELETE");
            return null;
        }

        private void appendMergeCaseWhen(boolean matched, Optional<Expression> expression)
        {
            builder.append(matched ? "WHEN MATCHED" : "WHEN NOT MATCHED");
            expression.ifPresent(value -> builder
                    .append(" AND ")
                    .append(formatExpression(value, dialect)));
            builder.append("\n");
        }

        @Override
        protected Void visitCreateView(CreateView node, Integer indent)
        {
            builder.append("CREATE ");
            if (node.isReplace()) {
                builder.append("OR REPLACE ");
            }
            builder.append("VIEW ")
                    .append(formatName(node.getName(), dialect));

            node.getComment().ifPresent(comment -> builder
                    .append(" COMMENT ")
                    .append(formatStringLiteral(comment)));

            node.getSecurity().ifPresent(security -> builder
                    .append(" SECURITY ")
                    .append(security));

            builder.append(" AS\n");

            process(node.getQuery(), indent);

            return null;
        }

        @Override
        protected Void visitRenameView(RenameView node, Integer indent)
        {
            builder.append("ALTER VIEW ")
                    .append(node.getSource())
                    .append(" RENAME TO ")
                    .append(node.getTarget());

            return null;
        }

        @Override
        protected Void visitRenameMaterializedView(RenameMaterializedView node, Integer indent)
        {
            builder.append("ALTER MATERIALIZED VIEW ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getSource())
                    .append(" RENAME TO ")
                    .append(node.getTarget());

            return null;
        }

        @Override
        protected Void visitSetViewAuthorization(SetViewAuthorization node, Integer indent)
        {
            builder.append("ALTER VIEW ")
                    .append(formatName(node.getSource(), dialect))
                    .append(" SET AUTHORIZATION ")
                    .append(formatPrincipal(node.getPrincipal()));

            return null;
        }

        @Override
        protected Void visitCreateMaterializedView(CreateMaterializedView node, Integer indent)
        {
            builder.append("CREATE ");
            if (node.isReplace()) {
                builder.append("OR REPLACE ");
            }
            builder.append("MATERIALIZED VIEW ");

            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }

            builder.append(formatName(node.getName(), dialect));
            node.getComment().ifPresent(comment -> builder
                    .append("\nCOMMENT ")
                    .append(formatStringLiteral(comment)));
            builder.append(formatPropertiesMultiLine(node.getProperties()));
            builder.append(" AS\n");

            process(node.getQuery(), indent);

            return null;
        }

        @Override
        protected Void visitRefreshMaterializedView(RefreshMaterializedView node, Integer indent)
        {
            builder.append("REFRESH MATERIALIZED VIEW ");
            builder.append(formatName(node.getName(), dialect));

            return null;
        }

        @Override
        protected Void visitDropMaterializedView(DropMaterializedView node, Integer indent)
        {
            builder.append("DROP MATERIALIZED VIEW ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getName(), dialect));
            return null;
        }

        @Override
        protected Void visitDropView(DropView node, Integer indent)
        {
            builder.append("DROP VIEW ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getName());

            return null;
        }

        @Override
        protected Void visitExplain(Explain node, Integer indent)
        {
            builder.append("EXPLAIN ");

            List<String> options = new ArrayList<>();

            for (ExplainOption option : node.getOptions()) {
                if (option instanceof ExplainType) {
                    options.add("TYPE " + ((ExplainType) option).getType());
                }
                else if (option instanceof ExplainFormat) {
                    options.add("FORMAT " + ((ExplainFormat) option).getType());
                }
                else {
                    throw new UnsupportedOperationException("unhandled explain option: " + option);
                }
            }

            if (!options.isEmpty()) {
                builder.append("(");
                Joiner.on(", ").appendTo(builder, options);
                builder.append(")");
            }

            builder.append("\n");

            process(node.getStatement(), indent);

            return null;
        }

        @Override
        protected Void visitExplainAnalyze(ExplainAnalyze node, Integer indent)
        {
            builder.append("EXPLAIN ANALYZE");
            if (node.isVerbose()) {
                builder.append(" VERBOSE");
            }
            builder.append("\n");

            process(node.getStatement(), indent);

            return null;
        }

        @Override
        protected Void visitShowCatalogs(ShowCatalogs node, Integer indent)
        {
            builder.append("SHOW CATALOGS");

            node.getLikePattern().ifPresent(value -> builder
                    .append(" LIKE ")
                    .append(formatStringLiteral(value)));

            node.getEscape().ifPresent(value -> builder
                    .append(" ESCAPE ")
                    .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowSchemas(ShowSchemas node, Integer indent)
        {
            builder.append("SHOW SCHEMAS");

            node.getCatalog().ifPresent(catalog -> builder
                    .append(" FROM ")
                    .append(node.getCatalog().get()));

            node.getLikePattern().ifPresent(value -> builder
                    .append(" LIKE ")
                    .append(formatStringLiteral(value)));

            node.getEscape().ifPresent(value -> builder
                    .append(" ESCAPE ")
                    .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowTables(ShowTables node, Integer indent)
        {
            builder.append("SHOW TABLES");

            node.getSchema().ifPresent(value -> builder
                    .append(" FROM ")
                    .append(formatName(value, dialect)));

            node.getLikePattern().ifPresent(value -> builder
                    .append(" LIKE ")
                    .append(formatStringLiteral(value)));

            node.getEscape().ifPresent(value -> builder
                    .append(" ESCAPE ")
                    .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowCreate(ShowCreate node, Integer indent)
        {
            if (node.getType() == ShowCreate.Type.TABLE) {
                builder.append("SHOW CREATE TABLE ")
                        .append(formatName(node.getName(), dialect));
            }
            else if (node.getType() == ShowCreate.Type.VIEW) {
                builder.append("SHOW CREATE VIEW ")
                        .append(formatName(node.getName(), dialect));
            }
            else if (node.getType() == ShowCreate.Type.MATERIALIZED_VIEW) {
                builder.append("SHOW CREATE MATERIALIZED VIEW ")
                        .append(formatName(node.getName(), dialect));
            }
            return null;
        }

        @Override
        protected Void visitShowColumns(ShowColumns node, Integer indent)
        {
            builder.append("SHOW COLUMNS FROM ")
                    .append(formatName(node.getTable(), dialect));

            node.getLikePattern().ifPresent(value -> builder
                    .append(" LIKE ")
                    .append(formatStringLiteral(value)));

            node.getEscape().ifPresent(value -> builder
                    .append(" ESCAPE ")
                    .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowStats(ShowStats node, Integer indent)
        {
            builder.append("SHOW STATS FOR ");
            process(node.getRelation(), 0);

            return null;
        }

        @Override
        protected Void visitShowFunctions(ShowFunctions node, Integer indent)
        {
            builder.append("SHOW FUNCTIONS");

            node.getLikePattern().ifPresent(value -> builder
                    .append(" LIKE ")
                    .append(formatStringLiteral(value)));

            node.getEscape().ifPresent(value -> builder
                    .append(" ESCAPE ")
                    .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowSession(ShowSession node, Integer indent)
        {
            builder.append("SHOW SESSION");

            node.getLikePattern().ifPresent(value -> builder
                    .append(" LIKE ")
                    .append(formatStringLiteral(value)));

            node.getEscape().ifPresent(value -> builder
                    .append(" ESCAPE ")
                    .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitDelete(Delete node, Integer indent)
        {
            builder.append("DELETE FROM ")
                    .append(formatName(node.getTable().getName(), dialect));

            node.getWhere().ifPresent(where -> builder
                    .append(" WHERE ")
                    .append(formatExpression(where, dialect)));

            return null;
        }

        @Override
        protected Void visitCreateSchema(CreateSchema node, Integer indent)
        {
            builder.append("CREATE SCHEMA ");
            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            builder.append(formatName(node.getSchemaName(), dialect));
            node.getPrincipal().ifPresent(principal -> builder
                    .append("\nAUTHORIZATION ")
                    .append(formatPrincipal(principal)));
            builder.append(formatPropertiesMultiLine(node.getProperties()));

            return null;
        }

        @Override
        protected Void visitDropSchema(DropSchema node, Integer indent)
        {
            builder.append("DROP SCHEMA ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getSchemaName(), dialect))
                    .append(" ")
                    .append(node.isCascade() ? "CASCADE" : "RESTRICT");

            return null;
        }

        @Override
        protected Void visitRenameSchema(RenameSchema node, Integer indent)
        {
            builder.append("ALTER SCHEMA ")
                    .append(formatName(node.getSource(), dialect))
                    .append(" RENAME TO ")
                    .append(formatExpression(node.getTarget(), dialect));

            return null;
        }

        @Override
        protected Void visitSetSchemaAuthorization(SetSchemaAuthorization node, Integer indent)
        {
            builder.append("ALTER SCHEMA ")
                    .append(formatName(node.getSource(), dialect))
                    .append(" SET AUTHORIZATION ")
                    .append(formatPrincipal(node.getPrincipal()));

            return null;
        }

        @Override
        protected Void visitCreateTableAsSelect(CreateTableAsSelect node, Integer indent)
        {
            builder.append("CREATE TABLE ");
            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            builder.append(formatName(node.getName(), dialect));

            node.getColumnAliases().ifPresent(columnAliases -> {
                String columnList = columnAliases.stream()
                        .map(alias -> formatExpression(alias, dialect))
                        .collect(joining(", "));
                builder.append(format("( %s )", columnList));
            });

            node.getComment().ifPresent(comment -> builder
                    .append("\nCOMMENT ")
                    .append(formatStringLiteral(comment)));
            builder.append(formatPropertiesMultiLine(node.getProperties()));

            builder.append(" AS ");
            process(node.getQuery(), indent);

            if (!node.isWithData()) {
                builder.append(" WITH NO DATA");
            }

            return null;
        }

        @Override
        protected Void visitCreateTable(CreateTable node, Integer indent)
        {
            builder.append("CREATE TABLE ");
            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            String tableName = formatName(node.getName(), dialect);
            builder.append(tableName).append(" (\n");

            String elementIndent = indentString(indent + 1);
            String columnList = node.getElements().stream()
                    .map(element -> {
                        if (element instanceof ColumnDefinition) {
                            ColumnDefinition column = (ColumnDefinition) element;
                            return elementIndent + formatColumnDefinition(column);
                        }
                        if (element instanceof LikeClause) {
                            LikeClause likeClause = (LikeClause) element;
                            StringBuilder builder = new StringBuilder(elementIndent);
                            builder.append("LIKE ")
                                    .append(formatName(likeClause.getTableName(), dialect));

                            likeClause.getPropertiesOption().ifPresent(propertiesOption -> builder
                                    .append(" ")
                                    .append(propertiesOption.name())
                                    .append(" PROPERTIES"));

                            return builder.toString();
                        }
                        throw new UnsupportedOperationException("unknown table element: " + element);
                    })
                    .collect(joining(",\n"));
            builder.append(columnList);
            builder.append("\n").append(")");

            node.getComment().ifPresent(comment -> builder
                    .append("\nCOMMENT ")
                    .append(formatStringLiteral(comment)));

            builder.append(formatPropertiesMultiLine(node.getProperties()));

            return null;
        }

        private String formatPropertiesMultiLine(List<Property> properties)
        {
            if (properties.isEmpty()) {
                return "";
            }

            String propertyList = properties.stream()
                    .map(element -> INDENT +
                            formatExpression(element.getName(), dialect) + " = " +
                            (element.isSetToDefault() ? "DEFAULT" : formatExpression(element.getNonDefaultValue(), dialect)))
                    .collect(joining(",\n"));

            return "\nWITH (\n" + propertyList + "\n)";
        }

        private String formatPropertiesSingleLine(List<Property> properties)
        {
            if (properties.isEmpty()) {
                return "";
            }

            return " WITH ( " + joinProperties(properties) + " )";
        }

        private String formatColumnDefinition(ColumnDefinition column)
        {
            StringBuilder builder = new StringBuilder()
                    .append(formatExpression(column.getName(), dialect))
                    .append(" ").append(column.getType());
            if (!column.isNullable()) {
                builder.append(" NOT NULL");
            }
            column.getComment().ifPresent(comment -> builder
                    .append(" COMMENT ")
                    .append(formatStringLiteral(comment)));
            builder.append(formatPropertiesSingleLine(column.getProperties()));
            return builder.toString();
        }

        private static String formatGrantor(GrantorSpecification grantor)
        {
            GrantorSpecification.Type type = grantor.getType();
            switch (type) {
                case CURRENT_ROLE:
                case CURRENT_USER:
                    return type.name();
                case PRINCIPAL:
                    return formatPrincipal(grantor.getPrincipal().get());
            }
            throw new IllegalArgumentException("Unsupported principal type: " + type);
        }

        private static String formatPrincipal(PrincipalSpecification principal)
        {
            PrincipalSpecification.Type type = principal.getType();
            switch (type) {
                case UNSPECIFIED:
                    return principal.getName().toString();
                case USER:
                case ROLE:
                    return format("%s %s", type.name(), principal.getName());
            }
            throw new IllegalArgumentException("Unsupported principal type: " + type);
        }

        @Override
        protected Void visitDropTable(DropTable node, Integer indent)
        {
            builder.append("DROP TABLE ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getTableName(), dialect));

            return null;
        }

        @Override
        protected Void visitRenameTable(RenameTable node, Integer indent)
        {
            builder.append("ALTER TABLE ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getSource())
                    .append(" RENAME TO ")
                    .append(node.getTarget());

            return null;
        }

        @Override
        protected Void visitSetProperties(SetProperties node, Integer context)
        {
            SetProperties.Type type = node.getType();
            builder.append("ALTER ");
            switch (type) {
                case TABLE:
                    builder.append("TABLE ");
                    break;
                case MATERIALIZED_VIEW:
                    builder.append("MATERIALIZED VIEW ");
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported SetProperties.Type: " + type);
            }
            builder.append(formatName(node.getName(), dialect))
                    .append(" SET PROPERTIES ")
                    .append(joinProperties(node.getProperties()));

            return null;
        }

        private String joinProperties(List<Property> properties)
        {
            return properties.stream()
                    .map(element -> formatExpression(element.getName(), dialect) + " = " +
                            (element.isSetToDefault() ? "DEFAULT" : formatExpression(element.getNonDefaultValue(), dialect)))
                    .collect(joining(", "));
        }

        @Override
        protected Void visitComment(Comment node, Integer context)
        {
            String comment = node.getComment()
                    .map(ExpressionFormatter::formatStringLiteral)
                    .orElse("NULL");

            switch (node.getType()) {
                case TABLE:
                    builder.append("COMMENT ON TABLE ")
                            .append(node.getName())
                            .append(" IS ")
                            .append(comment);
                    break;
                case COLUMN:
                    builder.append("COMMENT ON COLUMN ")
                            .append(node.getName())
                            .append(" IS ")
                            .append(comment);
                    break;
            }

            return null;
        }

        @Override
        protected Void visitRenameColumn(RenameColumn node, Integer indent)
        {
            builder.append("ALTER TABLE ");
            if (node.isTableExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getTable())
                    .append(" RENAME COLUMN ");
            if (node.isColumnExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getSource())
                    .append(" TO ")
                    .append(node.getTarget());

            return null;
        }

        @Override
        protected Void visitDropColumn(DropColumn node, Integer indent)
        {
            builder.append("ALTER TABLE ");
            if (node.isTableExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getTable(), dialect))
                    .append(" DROP COLUMN ");
            if (node.isColumnExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatExpression(node.getColumn(), dialect));

            return null;
        }

        @Override
        protected Void visitTableExecute(TableExecute node, Integer indent)
        {
            builder.append("ALTER TABLE ");
            builder.append(formatName(node.getTable().getName(), dialect));
            builder.append(" EXECUTE ");
            builder.append(formatExpression(node.getProcedureName(), dialect));
            if (!node.getArguments().isEmpty()) {
                builder.append("(");
                formatCallArguments(indent, node.getArguments());
                builder.append(")");
            }
            node.getWhere().ifPresent(where -> builder
                    .append("\n")
                    .append(indentString(indent))
                    .append("WHERE ")
                    .append(formatExpression(where, dialect)));
            return null;
        }

        @Override
        protected Void visitAnalyze(Analyze node, Integer indent)
        {
            builder.append("ANALYZE ")
                    .append(formatName(node.getTableName(), dialect));
            builder.append(formatPropertiesMultiLine(node.getProperties()));
            return null;
        }

        @Override
        protected Void visitAddColumn(AddColumn node, Integer indent)
        {
            builder.append("ALTER TABLE ");
            if (node.isTableExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getName())
                    .append(" ADD COLUMN ");
            if (node.isColumnNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            builder.append(formatColumnDefinition(node.getColumn()));

            return null;
        }

        @Override
        protected Void visitSetTableAuthorization(SetTableAuthorization node, Integer indent)
        {
            builder.append("ALTER TABLE ")
                    .append(formatName(node.getSource(), dialect))
                    .append(" SET AUTHORIZATION ")
                    .append(formatPrincipal(node.getPrincipal()));

            return null;
        }

        @Override
        protected Void visitInsert(Insert node, Integer indent)
        {
            builder.append("INSERT INTO ")
                    .append(formatName(node.getTarget(), dialect));

            node.getColumns().ifPresent(columns -> builder
                    .append(" (")
                    .append(Joiner.on(", ").join(columns))
                    .append(")"));

            builder.append("\n");

            process(node.getQuery(), indent);

            return null;
        }

        @Override
        protected Void visitUpdate(Update node, Integer indent)
        {
            builder.append("UPDATE ")
                    .append(node.getTable().getName())
                    .append(" SET");
            int setCounter = node.getAssignments().size() - 1;
            for (UpdateAssignment assignment : node.getAssignments()) {
                builder.append("\n")
                        .append(indentString(indent + 1))
                        .append(assignment.getName().getValue())
                        .append(" = ")
                        .append(formatExpression(assignment.getValue(), dialect));
                if (setCounter > 0) {
                    builder.append(",");
                }
                setCounter--;
            }
            node.getWhere().ifPresent(where -> builder
                    .append("\n")
                    .append(indentString(indent))
                    .append("WHERE ").append(formatExpression(where, dialect)));
            return null;
        }

        @Override
        protected Void visitTruncateTable(TruncateTable node, Integer indent)
        {
            builder.append("TRUNCATE TABLE ");
            builder.append(formatName(node.getTableName(), dialect));

            return null;
        }

        @Override
        public Void visitSetSession(SetSession node, Integer indent)
        {
            builder.append("SET SESSION ")
                    .append(formatName(node.getName(), dialect))
                    .append(" = ")
                    .append(formatExpression(node.getValue(), dialect));

            return null;
        }

        @Override
        public Void visitResetSession(ResetSession node, Integer indent)
        {
            builder.append("RESET SESSION ")
                    .append(formatName(node.getName(), dialect));

            return null;
        }

        @Override
        protected Void visitCallArgument(CallArgument node, Integer indent)
        {
            node.getName().ifPresent(name -> builder
                    .append(name)
                    .append(" => "));
            builder.append(formatExpression(node.getValue(), dialect));

            return null;
        }

        @Override
        protected Void visitCall(Call node, Integer indent)
        {
            builder.append("CALL ")
                    .append(node.getName())
                    .append("(");
            formatCallArguments(indent, node.getArguments());
            builder.append(")");

            return null;
        }

        private void formatCallArguments(Integer indent, List<CallArgument> arguments)
        {
            Iterator<CallArgument> iterator = arguments.iterator();
            while (iterator.hasNext()) {
                process(iterator.next(), indent);
                if (iterator.hasNext()) {
                    builder.append(", ");
                }
            }
        }

        @Override
        protected Void visitRow(Row node, Integer indent)
        {
            builder.append("ROW(");
            boolean firstItem = true;
            for (Expression item : node.getItems()) {
                if (!firstItem) {
                    builder.append(", ");
                }
                process(item, indent);
                firstItem = false;
            }
            builder.append(")");
            return null;
        }

        @Override
        protected Void visitStartTransaction(StartTransaction node, Integer indent)
        {
            builder.append("START TRANSACTION");

            Iterator<TransactionMode> iterator = node.getTransactionModes().iterator();
            while (iterator.hasNext()) {
                builder.append(" ");
                process(iterator.next(), indent);
                if (iterator.hasNext()) {
                    builder.append(",");
                }
            }
            return null;
        }

        @Override
        protected Void visitIsolationLevel(Isolation node, Integer indent)
        {
            builder.append("ISOLATION LEVEL ").append(node.getLevel().getText());
            return null;
        }

        @Override
        protected Void visitTransactionAccessMode(TransactionAccessMode node, Integer indent)
        {
            builder.append(node.isReadOnly() ? "READ ONLY" : "READ WRITE");
            return null;
        }

        @Override
        protected Void visitCommit(Commit node, Integer indent)
        {
            builder.append("COMMIT");
            return null;
        }

        @Override
        protected Void visitRollback(Rollback node, Integer indent)
        {
            builder.append("ROLLBACK");
            return null;
        }

        @Override
        protected Void visitCreateRole(CreateRole node, Integer indent)
        {
            builder.append("CREATE ROLE ").append(node.getName());
            node.getGrantor().ifPresent(grantor -> builder
                    .append(" WITH ADMIN ")
                    .append(formatGrantor(grantor)));
            node.getCatalog().ifPresent(catalog -> builder
                    .append(" IN ")
                    .append(catalog));
            return null;
        }

        @Override
        protected Void visitDropRole(DropRole node, Integer indent)
        {
            builder.append("DROP ROLE ").append(node.getName());
            node.getCatalog().ifPresent(catalog -> builder
                    .append(" IN ")
                    .append(catalog));
            return null;
        }

        @Override
        protected Void visitGrantRoles(GrantRoles node, Integer indent)
        {
            builder.append("GRANT ");
            builder.append(node.getRoles().stream()
                    .map(Identifier::toString)
                    .collect(joining(", ")));
            builder.append(" TO ");
            builder.append(node.getGrantees().stream()
                    .map(Formatter::formatPrincipal)
                    .collect(joining(", ")));
            if (node.isAdminOption()) {
                builder.append(" WITH ADMIN OPTION");
            }
            node.getGrantor().ifPresent(grantor -> builder
                    .append(" GRANTED BY ")
                    .append(formatGrantor(grantor)));
            node.getCatalog().ifPresent(catalog -> builder
                    .append(" IN ")
                    .append(catalog));
            return null;
        }

        @Override
        protected Void visitRevokeRoles(RevokeRoles node, Integer indent)
        {
            builder.append("REVOKE ");
            if (node.isAdminOption()) {
                builder.append("ADMIN OPTION FOR ");
            }
            builder.append(node.getRoles().stream()
                    .map(Identifier::toString)
                    .collect(joining(", ")));
            builder.append(" FROM ");
            builder.append(node.getGrantees().stream()
                    .map(Formatter::formatPrincipal)
                    .collect(joining(", ")));
            node.getGrantor().ifPresent(grantor -> builder
                    .append(" GRANTED BY ")
                    .append(formatGrantor(grantor)));
            node.getCatalog().ifPresent(catalog -> builder
                    .append(" IN ")
                    .append(catalog));
            return null;
        }

        @Override
        protected Void visitSetRole(SetRole node, Integer indent)
        {
            builder.append("SET ROLE ");
            SetRole.Type type = node.getType();
            switch (type) {
                case ALL:
                case NONE:
                    builder.append(type);
                    break;
                case ROLE:
                    builder.append(node.getRole().get());
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported type: " + type);
            }
            node.getCatalog().ifPresent(catalog -> builder
                    .append(" IN ")
                    .append(catalog));
            return null;
        }

        @Override
        public Void visitGrant(Grant node, Integer indent)
        {
            builder.append("GRANT ");

            builder.append(node.getPrivileges()
                    .map(privileges -> String.join(", ", privileges))
                    .orElse("ALL PRIVILEGES"));

            builder.append(" ON ");
            node.getType().ifPresent(type -> builder
                    .append(type)
                    .append(' '));
            builder.append(formatName(node.getName(), dialect))
                    .append(" TO ")
                    .append(formatPrincipal(node.getGrantee()));
            if (node.isWithGrantOption()) {
                builder.append(" WITH GRANT OPTION");
            }

            return null;
        }

        @Override
        public Void visitDeny(Deny node, Integer indent)
        {
            builder.append("DENY ");

            if (node.getPrivileges().isPresent()) {
                builder.append(String.join(", ", node.getPrivileges().get()));
            }
            else {
                builder.append("ALL PRIVILEGES");
            }

            builder.append(" ON ");
            if (node.getType().isPresent()) {
                builder.append(node.getType().get());
                builder.append(" ");
            }
            builder.append(formatName(node.getName(), dialect))
                    .append(" TO ")
                    .append(formatPrincipal(node.getGrantee()));

            return null;
        }

        @Override
        public Void visitRevoke(Revoke node, Integer indent)
        {
            builder.append("REVOKE ");

            if (node.isGrantOptionFor()) {
                builder.append("GRANT OPTION FOR ");
            }

            builder.append(node.getPrivileges()
                    .map(privileges -> String.join(", ", privileges))
                    .orElse("ALL PRIVILEGES"));

            builder.append(" ON ");
            node.getType().ifPresent(type -> builder
                    .append(type)
                    .append(' '));
            builder.append(node.getName())
                    .append(" FROM ")
                    .append(formatPrincipal(node.getGrantee()));

            return null;
        }

        @Override
        public Void visitShowGrants(ShowGrants node, Integer indent)
        {
            builder.append("SHOW GRANTS ");

            node.getTableName().ifPresent(tableName -> {
                builder.append("ON ");
                if (node.getTable()) {
                    builder.append("TABLE ");
                }
                builder.append(tableName);
            });

            return null;
        }

        @Override
        protected Void visitShowRoles(ShowRoles node, Integer indent)
        {
            builder.append("SHOW ");
            if (node.isCurrent()) {
                builder.append("CURRENT ");
            }
            builder.append("ROLES");
            node.getCatalog().ifPresent(catalog -> builder
                    .append(" FROM ")
                    .append(catalog));

            return null;
        }

        @Override
        protected Void visitShowRoleGrants(ShowRoleGrants node, Integer indent)
        {
            builder.append("SHOW ROLE GRANTS");
            node.getCatalog().ifPresent(catalog -> builder
                    .append(" FROM ")
                    .append(catalog));
            return null;
        }

        @Override
        public Void visitSetPath(SetPath node, Integer indent)
        {
            builder.append("SET PATH ");
            builder.append(Joiner.on(", ").join(node.getPathSpecification().getPath()));
            return null;
        }

        @Override
        public Void visitSetTimeZone(SetTimeZone node, Integer indent)
        {
            builder.append("SET TIME ZONE ");
            builder.append(node.getTimeZone().map(expression -> formatExpression(expression, dialect)).orElse("LOCAL"));
            return null;
        }

        private void processRelation(Relation relation, Integer indent)
        {
            // TODO: handle this properly
            if (relation instanceof Table) {
                builder.append("TABLE ")
                        .append(((Table) relation).getName())
                        .append('\n');
            }
            else {
                process(relation, indent);
            }
        }

        private StringBuilder append(int indent, String value)
        {
            return builder.append(indentString(indent))
                    .append(value);
        }

        private static String indentString(int indent)
        {
            return Strings.repeat(INDENT, indent);
        }

        private void formatDefinitionList(List<String> elements, int indent)
        {
            if (elements.size() == 1) {
                builder.append(" ")
                        .append(getOnlyElement(elements))
                        .append("\n");
            }
            else {
                builder.append("\n");
                for (int i = 0; i < elements.size() - 1; i++) {
                    append(indent, elements.get(i))
                            .append(",\n");
                }
                append(indent, elements.get(elements.size() - 1))
                        .append("\n");
            }
        }
    }

    private static void appendAliasColumns(StringBuilder builder, List<Identifier> columns, Dialect dialect)
    {
        if ((columns != null) && (!columns.isEmpty())) {
            String formattedColumns = columns.stream()
                    .map(expression -> formatExpression(expression, dialect))
                    .collect(Collectors.joining(", "));

            builder.append(" (")
                    .append(formattedColumns)
                    .append(')');
        }
    }
}
