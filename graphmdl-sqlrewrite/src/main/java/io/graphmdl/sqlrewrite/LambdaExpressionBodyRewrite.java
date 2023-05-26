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

import io.graphmdl.sqlrewrite.analyzer.Field;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.Window;
import io.trino.sql.tree.WindowReference;
import io.trino.sql.tree.WindowSpecification;

import java.util.List;
import java.util.Optional;

import static io.graphmdl.sqlrewrite.RelationshipCteGenerator.TARGET_REFERENCE;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class LambdaExpressionBodyRewrite
{
    public static Expression rewrite(Node node, Field baseField, Identifier argument)
    {
        return (Expression) new Visitor(baseField, argument).process(node, Optional.empty());
    }

    private LambdaExpressionBodyRewrite() {}

    static class Visitor
            extends BaseRewriter<Optional<Node>>
    {
        private final Field baseField;
        private final Identifier argument;

        Visitor(Field baseField, Identifier argument)
        {
            this.baseField = requireNonNull(baseField, "baseField is null");
            this.argument = requireNonNull(argument, "argument is null");
        }

        @Override
        protected Node visitDereferenceExpression(DereferenceExpression node, Optional<Node> context)
        {
            if (node.getBase() instanceof Identifier) {
                return visitAndCast(node.getBase(), Optional.ofNullable(node.getField().orElse(null)));
            }
            return new DereferenceExpression(visitAndCast(node.getBase(), Optional.ofNullable(node.getField().orElse(null))), node.getField().orElseThrow());
        }

        @Override
        protected Node visitSubscriptExpression(SubscriptExpression node, Optional<Node> context)
        {
            if (node.getBase() instanceof DereferenceExpression) {
                return new SubscriptExpression(visitAndCast(node.getBase(),
                        Optional.ofNullable((((DereferenceExpression) node.getBase()).getBase()))),
                        node.getIndex());
            }
            return new SubscriptExpression(visitAndCast(node.getBase(), Optional.empty()), node.getIndex());
        }

        @Override
        protected Node visitIdentifier(Identifier node, Optional<Node> context)
        {
            if (context.isEmpty()) {
                return new StringLiteral(String.format("Relationship<%s>", baseField.getModelName().getSchemaTableName().getTableName()));
            }
            if (argument.equals(node)) {
                return new DereferenceExpression(new Identifier(TARGET_REFERENCE), (Identifier) context.get());
            }
            return node;
        }

        @Override
        protected Node visitFunctionCall(FunctionCall node, Optional<Node> context)
        {
            return new FunctionCall(
                    node.getLocation(),
                    node.getName(),
                    node.getWindow().map(window -> visitAndCast(window, context)),
                    node.getFilter().map(filter -> visitAndCast(filter, context)),
                    node.getOrderBy().map(orderBy -> visitAndCast(orderBy, context)), node.isDistinct(),
                    node.getNullTreatment(), node.getProcessingMode(),
                    visitNodes(node.getArguments(), context));
        }

        @Override
        protected Node visitComparisonExpression(ComparisonExpression node, Optional<Node> context)
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
        protected Node visitLiteral(Literal node, Optional<Node> context)
        {
            return node;
        }

        protected <S extends Node> S visitAndCast(S node, Optional<Node> context)
        {
            return (S) process(node, context);
        }

        protected <S extends Window> S visitAndCast(S window, Optional<Node> context)
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
        protected <S extends Node> List<S> visitNodes(List<S> nodes, Optional<Node> context)
        {
            return nodes.stream()
                    .map(node -> (S) process(node, context))
                    .collect(toList());
        }
    }
}
