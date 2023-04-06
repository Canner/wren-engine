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

package io.graphmdl.main.calcite;

import com.google.common.collect.ImmutableList;
import io.graphmdl.base.GraphMDLException;
import io.graphmdl.main.metadata.Metadata;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArrayConstructor;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CharLiteral;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DataTypeParameter;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.ExistsPredicate;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Extract;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.GroupingElement;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.JoinCriteria;
import io.trino.sql.tree.JoinOn;
import io.trino.sql.tree.JoinUsing;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.Limit;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LogicalBinaryExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NaturalJoin;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.NumericParameter;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.PatternRecognitionRelation;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SampledRelation;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.SimpleGroupBy;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.TimeLiteral;
import io.trino.sql.tree.TimestampLiteral;
import io.trino.sql.tree.Union;
import io.trino.sql.tree.Unnest;
import io.trino.sql.tree.Values;
import io.trino.sql.tree.WhenClause;
import io.trino.sql.tree.WindowSpecification;
import io.trino.sql.tree.WithQuery;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCollectionTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlTableRef;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.graphmdl.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.graphmdl.base.type.StandardTypes.BIGINT;
import static io.graphmdl.base.type.StandardTypes.BOOLEAN;
import static io.graphmdl.base.type.StandardTypes.BYTEA;
import static io.graphmdl.base.type.StandardTypes.CHAR;
import static io.graphmdl.base.type.StandardTypes.DATE;
import static io.graphmdl.base.type.StandardTypes.DECIMAL;
import static io.graphmdl.base.type.StandardTypes.DOUBLE;
import static io.graphmdl.base.type.StandardTypes.INTEGER;
import static io.graphmdl.base.type.StandardTypes.JSON;
import static io.graphmdl.base.type.StandardTypes.REAL;
import static io.graphmdl.base.type.StandardTypes.SMALLINT;
import static io.graphmdl.base.type.StandardTypes.TINYINT;
import static io.graphmdl.base.type.StandardTypes.UUID;
import static io.graphmdl.base.type.StandardTypes.VARCHAR;
import static io.graphmdl.main.calcite.CalciteTypes.toCalciteType;
import static io.graphmdl.main.calcite.CalciteTypesUtil.extractTimestampPrecision;
import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.toList;
import static org.apache.calcite.rel.rel2sql.SqlImplementor.POS;
import static org.apache.calcite.sql.SqlIdentifier.STAR;
import static org.apache.calcite.sql.SqlIdentifier.star;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.BETWEEN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DESC;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DIVIDE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EXISTS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EXTRACT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_DISTINCT_FROM;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ITEM;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LIKE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MOD;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTIPLY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_IN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_LIKE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NULLIF;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OVER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ROW;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.UNION;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.UNION_ALL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.UNNEST;
import static org.apache.calcite.sql.parser.SqlParserPos.ZERO;

public class CalciteSqlNodeConverter
{
    private CalciteSqlNodeConverter() {}

    public static SqlNode convert(Node statement, Analysis analysis, Metadata metadata)
    {
        Visitor visitor = new Visitor(analysis, metadata);
        return visitor.process(statement);
    }

    private static class Visitor
            extends AstVisitor<SqlNode, ConvertContext>
    {
        private final Analysis analysis;
        private int paramCount;

        private Metadata metadata;

        public Visitor(Analysis analysis, Metadata metadata)
        {
            this.analysis = analysis;
            this.metadata = metadata;
        }

        @Override
        public SqlNode visitQuery(Query node, ConvertContext ignored)
        {
            SqlNode queryBody;
            ConvertContext context = ConvertContext.builder().build();
            if (node.getWith().isEmpty()) {
                queryBody = process(node.getQueryBody(), context);
            }
            else {
                node.getWith().get().getQueries().stream()
                        .map(WithQuery::getName)
                        .map(identifier -> QualifiedName.of(List.of(identifier)))
                        .forEach(analysis::addVisitedWithQuery);

                queryBody = new SqlWith(
                        toCalcitePos(node.getLocation()),
                        SqlNodeList.of(POS, visitNodes(node.getWith().get().getQueries())),
                        process(node.getQueryBody(), context));
            }

            if (context.getOrderByList().isPresent()) {
                return new SqlOrderBy(
                        POS,
                        queryBody,
                        context.getOrderByList().get(),
                        context.getVisitedOffset().orElse(null),
                        context.getVisitedLimit().orElse(null));
            }

            return queryBody;
        }

        @Override
        protected SqlNode visitWithQuery(WithQuery node, ConvertContext context)
        {
            return new SqlWithItem(
                    toCalcitePos(node.getLocation()),
                    (SqlIdentifier) visitNode(node.getName()),
                    node.getColumnNames().map(this::visitNodes).map(list -> SqlNodeList.of(POS, list)).orElse(null),
                    process(node.getQuery(), context));
        }

        @Override
        public SqlNode visitQuerySpecification(QuerySpecification node, ConvertContext context)
        {
            SqlNode limit = node.getLimit().map(this::visitNode).orElse(null);
            SqlNode offset = node.getOffset().map(this::visitNode).orElse(null);

            context.setVisitedOffset(offset);
            context.setVisitedLimit(limit);

            if (node.getOrderBy().isPresent()) {
                context.setOrderByList(node.getOrderBy().map(orderBy -> SqlNodeList.of(POS, visitNodes(orderBy.getSortItems()))).get());
            }

            return new SqlSelect(
                    toCalcitePos(node.getLocation()),
                    null, // fill up this
                    (SqlNodeList) visitNode(node.getSelect()),
                    node.getFrom().map(this::visitNode).orElse(null),
                    node.getWhere().map(this::visitNode).orElse(null),
                    node.getGroupBy().map(groupBy -> SqlNodeList.of(toCalcitePos(groupBy.getLocation()), visitNodes(groupBy.getGroupingElements()))).orElse(null),
                    node.getHaving().map(this::visitNode).orElse(null),
                    SqlNodeList.of(POS, visitNodes(node.getWindows())),
                    null,
                    node.getOrderBy().isEmpty() ? offset : null,
                    node.getOrderBy().isEmpty() ? limit : null,
                    null);
        }

        @Override
        protected SqlNode visitValues(Values node, ConvertContext context)
        {
            List<Expression> expressions = node.getRows().stream()
                    .map(row -> {
                        if (row instanceof Row) {
                            return row;
                        }
                        return new Row(ImmutableList.of(row));
                    })
                    .collect(toImmutableList());
            return new SqlBasicCall(SqlStdOperatorTable.VALUES, visitNodes(expressions), toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitLimit(Limit node, ConvertContext context)
        {
            return visitNode(node.getRowCount());
        }

        @Override
        public SqlNode visitSelect(Select node, ConvertContext context)
        {
            List<SqlNode> selectItems = node.getSelectItems().stream()
                    .map(this::visitNode)
                    .collect(toList());

            return SqlNodeList.of(POS, selectItems);
        }

        @Override
        public SqlNode visitTable(Table node, ConvertContext context)
        {
            if (!analysis.getVisitedWithQueries().contains(node.getName())) {
                analysis.addVisitedTable(node.getName());
            }
            List<String> tableName = node.getName().getParts();

            SqlIdentifier sqlIdentifier = new SqlIdentifier(tableName, ZERO);
            return new SqlTableRef(
                    sqlIdentifier.getParserPosition(),
                    sqlIdentifier,
                    SqlNodeList.of(ZERO, new ArrayList<>()));
        }

        @Override
        protected SqlNode visitSingleColumn(SingleColumn node, ConvertContext context)
        {
            if (node.getAlias().isPresent()) {
                return SqlStdOperatorTable.AS.createCall(
                        POS,
                        visitNode(node.getExpression()),
                        visitNode(node.getAlias().get()));
            }
            return visitNode(node.getExpression());
        }

        @Override
        protected SqlNode visitAllColumns(AllColumns node, ConvertContext context)
        {
            return STAR;
        }

        @Override
        public SqlNode visitIdentifier(Identifier identifier, ConvertContext context)
        {
            return new SqlIdentifier(identifier.getValue(), POS);
        }

        @Override
        protected SqlNode visitGenericLiteral(GenericLiteral node, ConvertContext context)
        {
            switch (node.getType().toLowerCase(ENGLISH)) {
                case BOOLEAN:
                    return SqlLiteral.createBoolean(parseBoolean(node.getValue()), POS);
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case REAL:
                case DOUBLE:
                case DECIMAL:
                    return SqlLiteral.createExactNumeric(node.getValue(), POS);
                case BYTEA:
                    return SqlLiteral.createBinaryString(node.getValue().getBytes(UTF_8), POS);
                case DATE:
                    return SqlLiteral.createDate(new DateString(node.getValue()), POS);
                case CHAR:
                case VARCHAR:
                case UUID:
                case JSON:
                    return SqlLiteral.createCharString(node.getValue(), POS);
            }
            throw new IllegalArgumentException();
        }

        @Override
        protected SqlNode visitLogicalBinaryExpression(LogicalBinaryExpression node, ConvertContext context)
        {
            return new SqlBasicCall(
                    toCalciteSqlOperator((node.getOperator())),
                    ImmutableList.of(visitNode(node.getLeft()), visitNode(node.getRight())),
                    toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitComparisonExpression(ComparisonExpression node, ConvertContext context)
        {
            return new SqlBasicCall(
                    toCalciteSqlOperator(node.getOperator()),
                    ImmutableList.of(visitNode(node.getLeft()), visitNode(node.getRight())),
                    POS);
        }

        @Override
        protected SqlNode visitBetweenPredicate(BetweenPredicate node, ConvertContext context)
        {
            return new SqlBasicCall(
                    BETWEEN,
                    ImmutableList.of(visitNode(node.getValue()), visitNode(node.getMin()), visitNode(node.getMax())),
                    toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitArithmeticBinary(ArithmeticBinaryExpression node, ConvertContext context)
        {
            return new SqlBasicCall(
                    toCalciteSqlOperator(node.getOperator()),
                    ImmutableList.of(visitNode(node.getLeft()), visitNode(node.getRight())),
                    POS);
        }

        @Override
        protected SqlNode visitLiteral(Literal node, ConvertContext context)
        {
            return super.visitLiteral(node, context);
        }

        @Override
        protected SqlNode visitDoubleLiteral(DoubleLiteral node, ConvertContext context)
        {
            return SqlLiteral.createExactNumeric(String.valueOf(node.getValue()), toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitDecimalLiteral(DecimalLiteral node, ConvertContext context)
        {
            return SqlLiteral.createExactNumeric(node.getValue(), toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitTimeLiteral(TimeLiteral node, ConvertContext context)
        {
            return super.visitTimeLiteral(node, context);
        }

        @Override
        protected SqlNode visitTimestampLiteral(TimestampLiteral node, ConvertContext context)
        {
            try {
                int precision = extractTimestampPrecision(node.getValue());
                return SqlLiteral.createTimestamp(new TimestampString(node.getValue()), precision, toCalcitePos(node.getLocation()));
            }
            catch (IllegalArgumentException e) {
                // TODO support timestamp with time zone
                return super.visitTimestampLiteral(node, context);
            }
        }

        @Override
        protected SqlNode visitIntervalLiteral(IntervalLiteral node, ConvertContext context)
        {
            return SqlLiteral.createInterval(
                    node.getSign().multiplier(),
                    node.getValue(),
                    new SqlIntervalQualifier(
                            toCalciteTimeUnit(node.getStartField()),
                            node.getEndField().map(CalciteSqlNodeConverter::toCalciteTimeUnit).orElse(null),
                            toCalcitePos(node.getLocation())),
                    toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitStringLiteral(StringLiteral node, ConvertContext context)
        {
            return SqlLiteral.createCharString(node.getValue(), toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitCharLiteral(CharLiteral node, ConvertContext context)
        {
            return SqlLiteral.createCharString(node.getValue(), toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitBinaryLiteral(BinaryLiteral node, ConvertContext context)
        {
            return SqlLiteral.createBinaryString(node.getValue().getBytes(), toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitBooleanLiteral(BooleanLiteral node, ConvertContext context)
        {
            return SqlLiteral.createBoolean(node.getValue(), toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitNullLiteral(NullLiteral node, ConvertContext context)
        {
            return SqlLiteral.createNull(toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitLongLiteral(LongLiteral node, ConvertContext context)
        {
            return SqlLiteral.createExactNumeric(Long.toString(node.getValue()), POS);
        }

        @Override
        protected SqlNode visitFunctionCall(FunctionCall node, ConvertContext context)
        {
            SqlUnresolvedFunction function = new SqlUnresolvedFunction(new SqlIdentifier(metadata.resolveFunction(node.getName().toString(), node.getArguments().size()),
                    toCalcitePos(node.getLocation())), null, null, null, null, SqlFunctionCategory.USER_DEFINED_FUNCTION);
            if (node.getWindow().isPresent()) {
                SqlBasicCall sqlBasicCall = new SqlBasicCall(function, visitNodes(node.getArguments()), toCalcitePos(node.getLocation()));
                return new SqlBasicCall(OVER,
                        List.of(sqlBasicCall, visitNode((Node) node.getWindow().get())), toCalcitePos(node.getLocation()));
            }

            return new SqlBasicCall(function,
                    !(node.getArguments().isEmpty() && "count".equalsIgnoreCase(node.getName().toString())) ?
                            visitNodes(node.getArguments()) :
                            ImmutableList.of(star(toCalcitePos(node.getLocation()))),
                    toCalcitePos(node.getLocation()),
                    node.isDistinct() ? SqlLiteral.createSymbol(SqlSelectKeyword.DISTINCT, toCalcitePos(node.getLocation())) : null);
        }

        @Override
        protected SqlNode visitWindowSpecification(WindowSpecification node, ConvertContext context)
        {
            // TODO: https://github.com/Canner/canner-metric-layer/issues/94
            //  support window function formally
            return SqlWindow.create(null,
                    null,
                    SqlNodeList.of(ZERO, visitNodes(node.getPartitionBy())),
                    node.getOrderBy().map(orderBy -> SqlNodeList.of(ZERO, visitNodes(orderBy.getSortItems()))).orElse(null),
                    SqlLiteral.createBoolean(false, ZERO), null, null, null, toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitDereferenceExpression(DereferenceExpression node, ConvertContext context)
        {
            // split catalog.schema.table
            List<String> expressions = Arrays.stream(node.toString().split("\\.")).map(Visitor::deQuoted).collect(toImmutableList());
            return new SqlIdentifier(expressions, POS);
        }

        private static String deQuoted(String value)
        {
            if (value.startsWith("\"") && value.endsWith("\"")) {
                return value.substring(1, value.length() - 1);
            }
            return value;
        }

        @Override
        protected SqlNode visitPatternRecognitionRelation(PatternRecognitionRelation node, ConvertContext context)
        {
            return super.visitPatternRecognitionRelation(node, context);
        }

        @Override
        protected SqlNode visitAliasedRelation(AliasedRelation node, ConvertContext context)
        {
            ImmutableList.Builder<SqlNode> operandListBuilder = ImmutableList.builder();
            operandListBuilder
                    .add(visitNode(node.getRelation()))
                    .add(visitNode(node.getAlias()));

            if (node.getColumnNames() != null) {
                node.getColumnNames().stream().map(this::visitNode).forEach(operandListBuilder::add);
            }

            return new SqlBasicCall(
                    AS,
                    operandListBuilder.build(),
                    toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitSampledRelation(SampledRelation node, ConvertContext context)
        {
            return super.visitSampledRelation(node, context);
        }

        @Override
        protected SqlNode visitTableSubquery(TableSubquery node, ConvertContext context)
        {
            return visitNode(node.getQuery());
        }

        @Override
        protected SqlNode visitSubqueryExpression(SubqueryExpression node, ConvertContext context)
        {
            return visitNode(node.getQuery());
        }

        @Override
        protected SqlNode visitJoin(Join node, ConvertContext context)
        {
            if (Join.Type.IMPLICIT.equals(node.getType())) {
                return new SqlJoin(
                        POS,
                        visitNode(node.getLeft()),
                        SqlLiteral.createBoolean(false, POS),
                        toCalciteJoinType(node.getType()),
                        visitNode(node.getRight()),
                        JoinConditionType.NONE.symbol(POS),
                        null);
            }

            JoinCriteria joinCriteria = node.getCriteria().orElseThrow(() -> new GraphMDLException(GENERIC_INTERNAL_ERROR, "join criteria is empty"));

            if (node.getCriteria().isPresent() && node.getCriteria().get() instanceof NaturalJoin) {
                return new SqlJoin(
                        POS,
                        visitNode(node.getLeft()),
                        SqlLiteral.createBoolean(true, POS),
                        toCalciteJoinType(node.getType()),
                        visitNode(node.getRight()),
                        toCalciteConditionType(joinCriteria),
                        processJoinCriteria(joinCriteria));
            }

            return new SqlJoin(
                    POS,
                    visitNode(node.getLeft()),
                    SqlLiteral.createBoolean(false, POS),
                    toCalciteJoinType(node.getType()),
                    visitNode(node.getRight()),
                    toCalciteConditionType(joinCriteria),
                    processJoinCriteria(joinCriteria));
        }

        private SqlNode processJoinCriteria(JoinCriteria joinCriteria)
        {
            if (joinCriteria instanceof JoinOn) {
                return visitNode(((JoinOn) joinCriteria).getExpression());
            }
            else if (joinCriteria instanceof JoinUsing) {
                return SqlNodeList.of(POS, visitNodes(((JoinUsing) joinCriteria).getColumns()));
            }

            throw new IllegalArgumentException();
        }

        @Override
        protected SqlNode visitUnion(Union node, ConvertContext context)
        {
            if (node.isDistinct()) {
                return new SqlBasicCall(UNION, visitNodes(node.getRelations(), context), toCalcitePos(node.getLocation()));
            }
            return new SqlBasicCall(UNION_ALL, visitNodes(node.getRelations(), context), toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitGroupingElement(GroupingElement node, ConvertContext context)
        {
            if (node instanceof SimpleGroupBy && node.getExpressions().size() == 1) {
                return visitNode(node.getExpressions().get(0));
            }
            return super.visitGroupingElement(node, context);
        }

        @Override
        protected SqlNode visitSortItem(SortItem node, ConvertContext context)
        {
            if (node.getOrdering().equals(SortItem.Ordering.DESCENDING)) {
                return new SqlBasicCall(DESC, ImmutableList.of(visitNode(node.getSortKey())), toCalcitePos(node.getLocation()));
            }

            return visitNode(node.getSortKey());
        }

        @Override
        protected SqlNode visitLikePredicate(LikePredicate node, ConvertContext context)
        {
            if (Optional.ofNullable(context).isPresent() && context.isFromNotExpression()) {
                return new SqlBasicCall(
                        NOT_LIKE,
                        ImmutableList.of(visitNode(node.getValue()), visitNode(node.getPattern())),
                        toCalcitePos(node.getLocation()));
            }
            return new SqlBasicCall(
                    LIKE,
                    ImmutableList.of(visitNode(node.getValue()), visitNode(node.getPattern())),
                    toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitExists(ExistsPredicate node, ConvertContext context)
        {
            return new SqlBasicCall(
                    EXISTS,
                    ImmutableList.of(visitNode(node.getSubquery())),
                    toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitExtract(Extract node, ConvertContext context)
        {
            return new SqlBasicCall(
                    EXTRACT,
                    ImmutableList.of(toSqlIntervalQualifier(node.getField(), toCalcitePos(node.getLocation())),
                            visitNode(node.getExpression())),
                    toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitSearchedCaseExpression(SearchedCaseExpression node, ConvertContext context)
        {
            return new SqlCase(
                    toCalcitePos(node.getLocation()),
                    null,
                    SqlNodeList.of(toCalcitePos(node.getLocation()), visitNodes(node.getWhenClauses().stream().map(WhenClause::getOperand).collect(toList()))),
                    SqlNodeList.of(toCalcitePos(node.getLocation()), visitNodes(node.getWhenClauses().stream().map(WhenClause::getResult).collect(toList()))),
                    node.getDefaultValue().map(this::visitNode).orElse(null));
        }

        @Override
        protected SqlNode visitSimpleCaseExpression(SimpleCaseExpression node, ConvertContext context)
        {
            // Calcite will transfer simple case to searched case.
            // e.g. CASE col WHEN 1 THEN 'good' ELSE 'bad' END -> CASE WHEN col = 1 THEN 'good' ELSE 'bad' END
            // We should follow this logical to build the calcite case node.
            List<Expression> operands = node.getWhenClauses().stream()
                    .map(whenClause -> new ComparisonExpression(ComparisonExpression.Operator.EQUAL, node.getOperand(), whenClause.getOperand()))
                    .collect(toList());
            return new SqlCase(
                    toCalcitePos(node.getLocation()),
                    null,
                    SqlNodeList.of(toCalcitePos(node.getLocation()), visitNodes(operands)),
                    SqlNodeList.of(toCalcitePos(node.getLocation()), visitNodes(node.getWhenClauses().stream().map(WhenClause::getResult).collect(toList()))),
                    node.getDefaultValue().map(this::visitNode).orElse(null));
        }

        @Override
        protected SqlNode visitCast(Cast node, ConvertContext context)
        {
            return new SqlBasicCall(CAST, ImmutableList.of(visitNode(node.getExpression()), visitNode(node.getType())), toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitGenericDataType(GenericDataType node, ConvertContext context)
        {
            SqlTypeName typeName = toCalciteType(node.getName().toString());
            if (typeName.equals(SqlTypeName.CHAR)) {
                return new SqlDataTypeSpec(
                        new SqlBasicTypeNameSpec(typeName, handleNumericParameter(node.getArguments().get(0)), ZERO),
                        toCalcitePos(node.getLocation()));
            }
            else if (typeName.equals(SqlTypeName.DECIMAL)) {
                List<Integer> params = node.getArguments().stream().map(Visitor::handleNumericParameter).collect(toImmutableList());
                checkArgument(params.size() == 2, format("decimal type should have 2 parameters but %s", params.size()));
                return new SqlDataTypeSpec(
                        new SqlBasicTypeNameSpec(typeName, params.get(0), params.get(1), ZERO),
                        toCalcitePos(node.getLocation()));
            }
            else if (typeName.equals(SqlTypeName.ARRAY)) {
                return new SqlDataTypeSpec(
                        new SqlCollectionTypeNameSpec(toCalciteTypeNameSpec(node.getArguments().get(0).toString()), typeName, ZERO),
                        toCalcitePos(node.getLocation()));
            }
            return new SqlDataTypeSpec(
                    new SqlBasicTypeNameSpec(typeName, ZERO),
                    toCalcitePos(node.getLocation()));
        }

        private SqlTypeNameSpec toCalciteTypeNameSpec(String typeName)
        {
            return new SqlBasicTypeNameSpec(toCalciteType(typeName), ZERO);
        }

        private static int handleNumericParameter(DataTypeParameter dataTypeParameter)
        {
            if (dataTypeParameter instanceof NumericParameter) {
                return Integer.parseInt(((NumericParameter) dataTypeParameter).getValue());
            }
            throw new IllegalArgumentException(format("%s isn't NumericParameter", dataTypeParameter));
        }

        private SqlIntervalQualifier toSqlIntervalQualifier(Extract.Field field, SqlParserPos pos)
        {
            return new SqlIntervalQualifier(toCalciteTimeUnit(IntervalLiteral.IntervalField.valueOf(field.name())), null, pos);
        }

        @Override
        protected SqlNode visitInPredicate(InPredicate node, ConvertContext context)
        {
            if (Optional.ofNullable(context).isPresent() && context.isFromNotExpression()) {
                return new SqlBasicCall(
                        NOT_IN,
                        ImmutableList.of(visitNode(node.getValue()), visitNode(node.getValueList())),
                        toCalcitePos(node.getLocation()));
            }
            return new SqlBasicCall(
                    IN,
                    ImmutableList.of(visitNode(node.getValue()), visitNode(node.getValueList())),
                    toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitInListExpression(InListExpression node, ConvertContext context)
        {
            return SqlNodeList.of(toCalcitePos(node.getLocation()), visitNodes(node.getValues()));
        }

        @Override
        protected SqlNode visitNotExpression(NotExpression node, ConvertContext context)
        {
            // Calcite change NOT (id LIKE 'xxx') to id NOT LIKE 'xxx'
            if (node.getValue() instanceof LikePredicate ||
                    node.getValue() instanceof InPredicate) {
                return process(node.getValue(), ConvertContext.builder().setFromNotExpression(true).build());
            }
            return new SqlBasicCall(NOT, ImmutableList.of(visitNode(node.getValue())), toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitSubscriptExpression(SubscriptExpression node, ConvertContext context)
        {
            return new SqlBasicCall(ITEM, ImmutableList.of(visitNode(node.getBase()), visitNode(node.getIndex())), toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitUnnest(Unnest node, ConvertContext context)
        {
            return new SqlBasicCall(UNNEST, visitNodes(node.getExpressions()), toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitRow(Row node, ConvertContext context)
        {
            return new SqlBasicCall(ROW, visitNodes(node.getItems()), toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitParameter(Parameter node, ConvertContext context)
        {
            return new SqlDynamicParam(paramCount++, toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitArrayConstructor(ArrayConstructor node, ConvertContext context)
        {
            return new SqlBasicCall(ARRAY_VALUE_CONSTRUCTOR, visitNodes(node.getValues()), toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitNullIfExpression(NullIfExpression node, ConvertContext context)
        {
            return new SqlBasicCall(
                    new SqlUnresolvedFunction(new SqlIdentifier(NULLIF.getName(), ZERO),
                            null, null, null, null,
                            SqlFunctionCategory.USER_DEFINED_FUNCTION),
                    List.of(visitNode(node.getFirst()), visitNode(node.getSecond())), toCalcitePos(node.getLocation()));
        }

        @Override
        protected SqlNode visitNode(Node node, ConvertContext context)
        {
            throw new UnsupportedOperationException("Unsupported node: " + node);
        }

        @SuppressWarnings("unchecked")
        protected <T extends SqlNode> List<SqlNode> visitNodes(List<? extends Node> nodes)
        {
            return visitNodes(nodes, null);
        }

        @SuppressWarnings("unchecked")
        protected <T extends SqlNode> List<SqlNode> visitNodes(List<? extends Node> nodes, ConvertContext context)
        {
            return nodes.stream()
                    .map(node -> (T) process(node, context))
                    .collect(toList());
        }

        protected SqlNode visitNode(Node node)
        {
            return process(node);
        }
    }

    private static TimeUnit toCalciteTimeUnit(IntervalLiteral.IntervalField intervalField)
    {
        switch (intervalField) {
            case YEAR:
                return TimeUnit.YEAR;
            case MONTH:
                return TimeUnit.MONTH;
            case DAY:
                return TimeUnit.DAY;
            case HOUR:
                return TimeUnit.HOUR;
            case MINUTE:
                return TimeUnit.MINUTE;
            case SECOND:
                return TimeUnit.SECOND;
        }
        throw new IllegalArgumentException();
    }

    private static SqlLiteral toCalciteJoinType(Join.Type type)
    {
        switch (type) {
            case CROSS:
                return JoinType.CROSS.symbol(POS);
            case FULL:
                return JoinType.FULL.symbol(POS);
            case LEFT:
                return JoinType.LEFT.symbol(POS);
            case INNER:
                return JoinType.INNER.symbol(POS);
            case RIGHT:
                return JoinType.RIGHT.symbol(POS);
            case IMPLICIT:
                return JoinType.COMMA.symbol(POS);
        }
        throw new IllegalArgumentException("Illegal type: " + type);
    }

    private static SqlLiteral toCalciteConditionType(JoinCriteria joinCriteria)
    {
        if (joinCriteria instanceof JoinOn) {
            return JoinConditionType.ON.symbol(POS);
        }
        else if (joinCriteria instanceof JoinUsing) {
            return JoinConditionType.USING.symbol(POS);
        }
        throw new IllegalArgumentException();
    }

    private static SqlOperator toCalciteSqlOperator(ComparisonExpression.Operator operator)
    {
        switch (operator) {
            case EQUAL:
                return EQUALS;
            case NOT_EQUAL:
                return NOT_EQUALS;
            case LESS_THAN:
                return LESS_THAN;
            case LESS_THAN_OR_EQUAL:
                return LESS_THAN_OR_EQUAL;
            case GREATER_THAN:
                return GREATER_THAN;
            case GREATER_THAN_OR_EQUAL:
                return GREATER_THAN_OR_EQUAL;
            case IS_DISTINCT_FROM:
                return IS_DISTINCT_FROM;
        }
        throw new UnsupportedOperationException(format("Unsupported operator %s" + operator));
    }

    private static SqlOperator toCalciteSqlOperator(ArithmeticBinaryExpression.Operator operator)
    {
        switch (operator) {
            case MULTIPLY:
                return MULTIPLY;
            case SUBTRACT:
                return MINUS;
            case ADD:
                return PLUS;
            case DIVIDE:
                return DIVIDE;
            case MODULUS:
                return MOD;
        }
        throw new UnsupportedOperationException(format("Unsupported operator %s" + operator));
    }

    private static SqlOperator toCalciteSqlOperator(LogicalBinaryExpression.Operator operator)
    {
        switch (operator) {
            case OR:
                return OR;
            case AND:
                return AND;
        }
        throw new IllegalArgumentException();
    }

    private static SqlParserPos toCalcitePos(Optional<NodeLocation> location)
    {
        if (location.isEmpty()) {
            return POS;
        }
        return new SqlParserPos(location.get().getLineNumber(), location.get().getColumnNumber());
    }

    static class ConvertContext
    {
        private final boolean fromNotExpression;
        private SqlNode visitedOffset;
        private SqlNode visitedLimit;

        private SqlNodeList orderByList;

        public static Builder builder()
        {
            return new Builder();
        }

        private ConvertContext(boolean fromNotExpression)
        {
            this.fromNotExpression = fromNotExpression;
        }

        public boolean isFromNotExpression()
        {
            return fromNotExpression;
        }

        public Optional<SqlNode> getVisitedOffset()
        {
            return Optional.ofNullable(visitedOffset);
        }

        public void setVisitedOffset(SqlNode visitedOffset)
        {
            this.visitedOffset = visitedOffset;
        }

        public Optional<SqlNode> getVisitedLimit()
        {
            return Optional.ofNullable(visitedLimit);
        }

        public void setVisitedLimit(SqlNode visitedLimit)
        {
            this.visitedLimit = visitedLimit;
        }

        public Optional<SqlNodeList> getOrderByList()
        {
            return Optional.ofNullable(orderByList);
        }

        public void setOrderByList(SqlNodeList orderByList)
        {
            this.orderByList = orderByList;
        }

        static class Builder
        {
            private boolean fromNotExpression;

            public Builder setFromNotExpression(boolean fromNotExpression)
            {
                this.fromNotExpression = fromNotExpression;
                return this;
            }

            public ConvertContext build()
            {
                return new ConvertContext(fromNotExpression);
            }
        }
    }
}
