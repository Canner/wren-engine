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

package io.wren.base.sqlrewrite.analyzer;

import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArrayConstructor;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CharLiteral;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.CurrentCatalog;
import io.trino.sql.tree.CurrentPath;
import io.trino.sql.tree.CurrentSchema;
import io.trino.sql.tree.CurrentTime;
import io.trino.sql.tree.CurrentUser;
import io.trino.sql.tree.DateTimeDataType;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.ExistsPredicate;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.IntervalDayTimeDataType;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.QuantifiedComparisonExpression;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.RowDataType;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.TimeLiteral;
import io.trino.sql.tree.TimestampLiteral;
import io.wren.base.WrenMDL;
import io.wren.base.metadata.Function;
import io.wren.base.metadata.FunctionBundle;
import io.wren.base.type.BigIntType;
import io.wren.base.type.BooleanType;
import io.wren.base.type.ByteaType;
import io.wren.base.type.DateType;
import io.wren.base.type.DoubleType;
import io.wren.base.type.IntervalType;
import io.wren.base.type.NumericType;
import io.wren.base.type.PGType;
import io.wren.base.type.PGTypes;
import io.wren.base.type.RecordType;
import io.wren.base.type.TimestampType;
import io.wren.base.type.VarcharType;

import java.util.Optional;

import static io.trino.sql.tree.DereferenceExpression.getQualifiedName;
import static java.util.Objects.requireNonNull;

public class ExpressionTypeAnalyzer
        extends DefaultTraversalVisitor<Void>
{
    public static PGType<?> analyze(WrenMDL mdl, Scope scope, Expression expression)
    {
        ExpressionTypeAnalyzer analyzer = new ExpressionTypeAnalyzer(mdl, scope);
        analyzer.process(expression);
        return analyzer.result;
    }

    private final WrenMDL mdl;
    private final Scope scope;
    private PGType<?> result;

    public ExpressionTypeAnalyzer(WrenMDL mdl, Scope scope)
    {
        this.mdl = requireNonNull(mdl, "mdl is null");
        this.scope = requireNonNull(scope, "scope is null");
    }

    @Override
    protected Void visitStringLiteral(StringLiteral node, Void context)
    {
        result = VarcharType.VARCHAR;
        return null;
    }

    @Override
    protected Void visitDoubleLiteral(DoubleLiteral node, Void context)
    {
        result = DoubleType.DOUBLE;
        return null;
    }

    @Override
    protected Void visitDecimalLiteral(DecimalLiteral node, Void context)
    {
        result = NumericType.NUMERIC;
        return null;
    }

    @Override
    protected Void visitGenericLiteral(GenericLiteral node, Void context)
    {
        PGTypes.nameToPgType(node.getType())
                .ifPresent(pgType -> result = pgType);
        return null;
    }

    @Override
    protected Void visitTimeLiteral(TimeLiteral node, Void context)
    {
        // TODO: we don't support time type yet, so we treat it as timestamp type.
        result = TimestampType.TIMESTAMP;
        return null;
    }

    @Override
    protected Void visitTimestampLiteral(TimestampLiteral node, Void context)
    {
        // TODO: timestamp literal may contain timezone, we need to handle it.
        result = TimestampType.TIMESTAMP;
        return null;
    }

    @Override
    protected Void visitIntervalLiteral(IntervalLiteral node, Void context)
    {
        result = IntervalType.INTERVAL;
        return null;
    }

    @Override
    protected Void visitCharLiteral(CharLiteral node, Void context)
    {
        result = VarcharType.VARCHAR;
        return null;
    }

    @Override
    protected Void visitBinaryLiteral(BinaryLiteral node, Void context)
    {
        result = ByteaType.BYTEA;
        return null;
    }

    @Override
    protected Void visitBooleanLiteral(BooleanLiteral node, Void context)
    {
        result = BooleanType.BOOLEAN;
        return null;
    }

    @Override
    protected Void visitLongLiteral(LongLiteral node, Void context)
    {
        result = BigIntType.BIGINT;
        return null;
    }

    @Override
    protected Void visitNullLiteral(NullLiteral node, Void context)
    {
        return super.visitNullLiteral(node, context);
    }

    @Override
    protected Void visitCast(Cast node, Void context)
    {
        process(node.getType());
        // The type is the final output. We don't need to dig into the expression.
        return null;
    }

    @Override
    protected Void visitRowDataType(RowDataType node, Void context)
    {
        result = RecordType.EMPTY_RECORD;
        return null;
    }

    @Override
    protected Void visitDateTimeType(DateTimeDataType node, Void context)
    {
        // TODO: it may contain timezone, we need to handle it.
        result = TimestampType.TIMESTAMP;
        return null;
    }

    @Override
    protected Void visitIntervalDataType(IntervalDayTimeDataType node, Void context)
    {
        result = IntervalType.INTERVAL;
        return null;
    }

    @Override
    protected Void visitGenericDataType(GenericDataType node, Void context)
    {
        PGTypes.nameToPgType(node.getName().getValue()).ifPresent(pgType -> result = pgType);
        return null;
    }

    @Override
    protected Void visitInPredicate(InPredicate node, Void context)
    {
        result = BooleanType.BOOLEAN;
        return null;
    }

    @Override
    protected Void visitLikePredicate(LikePredicate node, Void context)
    {
        result = BooleanType.BOOLEAN;
        return null;
    }

    @Override
    protected Void visitBetweenPredicate(BetweenPredicate node, Void context)
    {
        result = BooleanType.BOOLEAN;
        return null;
    }

    @Override
    protected Void visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
    {
        result = BooleanType.BOOLEAN;
        return null;
    }

    @Override
    protected Void visitIsNullPredicate(IsNullPredicate node, Void context)
    {
        result = BooleanType.BOOLEAN;
        return null;
    }

    @Override
    protected Void visitExists(ExistsPredicate node, Void context)
    {
        result = BooleanType.BOOLEAN;
        return null;
    }

    @Override
    protected Void visitComparisonExpression(ComparisonExpression node, Void context)
    {
        result = BooleanType.BOOLEAN;
        return null;
    }

    @Override
    protected Void visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, Void context)
    {
        // TODO: we don't support quantified comparison yet.
        return null;
    }

    @Override
    protected Void visitFunctionCall(FunctionCall node, Void context)
    {
        FunctionBundle.getFunction(node.getName().getSuffix(), node.getArguments().size())
                .flatMap(Function::getReturnType)
                .ifPresent(type -> result = type);
        // TODO: handle the remote name
        if (node.getName().getSuffix().equalsIgnoreCase("now") ||
                node.getName().getSuffix().equalsIgnoreCase("now___timestamp")) {
            result = TimestampType.TIMESTAMP;
        }
        return null;
    }

    @Override
    protected Void visitDereferenceExpression(DereferenceExpression node, Void context)
    {
        QualifiedName qualifiedName = getQualifiedName(node);
        if (qualifiedName != null) {
            Optional<Field> fieldOptional = scope.getRelationType().getFields().stream()
                    .filter(field -> field.canResolve(qualifiedName))
                    .findFirst();

            fieldOptional.ifPresent(field -> result = getColumnType(field));
        }
        return null;
    }

    @Override
    protected Void visitIdentifier(Identifier node, Void context)
    {
        QualifiedName qualifiedName = QualifiedName.of(ImmutableList.of(node.getValue()));
        scope.getRelationType().getFields().stream()
                .filter(field -> field.canResolve(qualifiedName))
                .findFirst()
                .ifPresent(field -> result = getColumnType(field));
        return null;
    }

    private PGType<?> getColumnType(Field field)
    {
        String objectName = field.getTableName().getSchemaTableName().getTableName();
        String columnName = field.getColumnName();
        return PGTypes.nameToPgType(mdl.getColumnType(objectName, columnName)).orElse(null);
    }

    @Override
    protected Void visitRow(Row node, Void context)
    {
        result = RecordType.EMPTY_RECORD;
        return null;
    }

    @Override
    protected Void visitSubscriptExpression(SubscriptExpression node, Void context)
    {
        process(node.getBase(), context);
        if (result != null) {
            result = PGTypes.getArrayType(result.oid());
        }
        return null;
    }

    @Override
    protected Void visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
    {
        // TODO: check the type coercion rule. For now, we just use the left type.
        process(node.getLeft());
        return null;
    }

    @Override
    protected Void visitCurrentTime(CurrentTime node, Void context)
    {
        if (node.getFunction().equals(CurrentTime.Function.DATE)) {
            result = DateType.DATE;
        }
        else {
            result = TimestampType.TIMESTAMP;
        }
        return null;
    }

    @Override
    protected Void visitCurrentUser(CurrentUser node, Void context)
    {
        result = VarcharType.VARCHAR;
        return null;
    }

    @Override
    protected Void visitCurrentSchema(CurrentSchema node, Void context)
    {
        result = VarcharType.VARCHAR;
        return null;
    }

    @Override
    protected Void visitCurrentCatalog(CurrentCatalog node, Void context)
    {
        result = VarcharType.VARCHAR;
        return null;
    }

    @Override
    protected Void visitCurrentPath(CurrentPath node, Void context)
    {
        result = VarcharType.VARCHAR;
        return null;
    }

    @Override
    protected Void visitArrayConstructor(ArrayConstructor node, Void context)
    {
        // ALl value should be same type in array, we only check first value type here.
        process(node.getValues().get(0));
        if (result != null) {
            result = PGTypes.getArrayType(result.oid());
        }
        return null;
    }
}
