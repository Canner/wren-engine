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
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubscriptExpression;

import java.util.Optional;

import static io.graphmdl.sqlrewrite.RelationshipCteGenerator.TARGET_REFERENCE;
import static java.util.Objects.requireNonNull;

public class LambdaExpressionRewrite
{
    public static Expression rewrite(Node node, Field baseField, Identifier argument)
    {
        return (Expression) new Visitor(baseField, argument).process(node, Optional.empty());
    }

    private LambdaExpressionRewrite()
    {
    }

    static class Visitor
            extends AstVisitor<Node, Optional<Node>>
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
                return process(node.getBase(), Optional.ofNullable(node.getField()));
            }
            return new DereferenceExpression((Expression) process(node.getBase(), Optional.ofNullable(node.getField())), node.getField());
        }

        @Override
        protected Node visitSubscriptExpression(SubscriptExpression node, Optional<Node> context)
        {
            if (node.getBase() instanceof DereferenceExpression) {
                return new SubscriptExpression((Expression) process(node.getBase(),
                        Optional.ofNullable((((DereferenceExpression) node.getBase()).getBase()))),
                        node.getIndex());
            }
            return new SubscriptExpression((Expression) process(node.getBase(), Optional.empty()), node.getIndex());
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
    }
}
