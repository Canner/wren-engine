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

package io.accio.main.sql.bigquery.analyzer;

import com.google.common.collect.ImmutableList;
import io.accio.base.AccioMDL;
import io.accio.base.metadata.FunctionBundle;
import io.accio.base.sqlrewrite.analyzer.ExpressionTypeAnalyzer;
import io.accio.base.sqlrewrite.analyzer.Scope;
import io.accio.base.sqlrewrite.analyzer.TypeCoercion;
import io.accio.base.type.DateType;
import io.accio.base.type.PGType;
import io.accio.base.type.TimestampType;
import io.accio.base.type.TimestampWithTimeZoneType;
import io.airlift.log.Logger;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeLocation;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class BigQueryTypeCoercion
        implements TypeCoercion
{
    private static final Logger LOG = Logger.get(BigQueryTypeCoercion.class);
    private final AccioMDL mdl;
    private final FunctionBundle functionBundle;

    public BigQueryTypeCoercion(AccioMDL mdl, FunctionBundle functionBundle)
    {
        this.mdl = requireNonNull(mdl, "mdl is null");
        this.functionBundle = requireNonNull(functionBundle, "functionBundle is null");
    }

    @Override
    public Optional<Expression> coerceExpression(Expression expression, Scope scope)
    {
        Expression rewritten = ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteComparisonExpression(ComparisonExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                PGType<?> leftType = ExpressionTypeAnalyzer.analyze(mdl, scope, node.getLeft(), functionBundle);
                PGType<?> rightType = ExpressionTypeAnalyzer.analyze(mdl, scope, node.getRight(), functionBundle);
                LOG.debug("left: %s leftType: %s, right: %s rightType: %s",
                        node.getLeft(),
                        leftType == null ? null : leftType.typName(),
                        node.getRight(),
                        rightType == null ? null : rightType.typName());
                if (shouldBeCoerced(leftType, rightType)) {
                    if (node.getLocation().isPresent()) {
                        return new ComparisonExpression(node.getLocation().get(), node.getOperator(), node.getLeft(), cast(node.getLocation(), node.getRight(), leftType));
                    }
                    return new ComparisonExpression(node.getOperator(), node.getLeft(), cast(node.getLocation(), node.getRight(), leftType));
                }
                return super.rewriteComparisonExpression(node, context, treeRewriter);
            }
        }, expression, null);

        if (!rewritten.equals(expression)) {
            return Optional.of(rewritten);
        }

        return Optional.empty();
    }

    private boolean shouldBeCoerced(PGType<?> leftType, PGType<?> rightType)
    {
        if (leftType == null || rightType == null) {
            return false;
        }

        if (leftType.equals(rightType)) {
            return false;
        }

        if (leftType.equals(DateType.DATE) ||
                leftType.equals(TimestampType.TIMESTAMP) ||
                leftType.equals(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIMEZONE)) {
            return true;
        }

        return false;
    }

    private Cast cast(Optional<NodeLocation> nodeLocation, Expression expression, PGType<?> type)
    {
        return nodeLocation.map(location -> new Cast(location, expression, new GenericDataType(Optional.empty(), new Identifier(type.typName()), ImmutableList.of())))
                .orElseGet(() -> new Cast(expression, new GenericDataType(Optional.empty(), new Identifier(type.typName()), ImmutableList.of())));
    }
}
