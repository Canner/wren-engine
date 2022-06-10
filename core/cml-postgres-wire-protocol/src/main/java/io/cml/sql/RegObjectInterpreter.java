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

package io.cml.sql;

import io.cml.pgcatalog.regtype.RegObject;
import io.cml.pgcatalog.regtype.RegObjectFactory;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.StringLiteral;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class RegObjectInterpreter
{
    private final RegObjectFactory regObjectFactory;

    public RegObjectInterpreter(RegObjectFactory regObjectFactory)
    {
        this.regObjectFactory = requireNonNull(regObjectFactory, "regObjectFactory is null");
    }

    public Optional<Object> evaluate(Expression node, boolean showObjectAsName)
    {
        return Optional.ofNullable(new Visitor(showObjectAsName, regObjectFactory).process(node));
    }

    private static class Visitor
            extends AstVisitor<Object, Object>
    {
        private final boolean showObjectAsName;
        private final RegObjectFactory regObjectFactory;

        private Visitor(boolean showObjectAsName, RegObjectFactory regObjectFactory)
        {
            this.showObjectAsName = showObjectAsName;
            this.regObjectFactory = regObjectFactory;
        }

        @Override
        protected Object visitGenericLiteral(GenericLiteral node, Object context)
        {
            if (node.getType().startsWith("reg")) {
                RegObject regObject = regObjectFactory.of(node.getType(), node.getValue());
                if (showObjectAsName) {
                    return new StringLiteral(regObject.getName());
                }
                return new LongLiteral(Integer.toString(regObject.getOid()));
            }
            return node;
        }

        @Override
        protected Object visitCast(Cast node, Object context)
        {
            // oid is a longLiteral
            if (!(node.getExpression() instanceof LongLiteral)) {
                return null;
            }
            RegObject regObject = regObjectFactory.of(node.getType().toString(), (int) ((LongLiteral) node.getExpression()).getValue());
            if (showObjectAsName) {
                return new StringLiteral(regObject.getName());
            }
            return new LongLiteral(Integer.toString(regObject.getOid()));
        }

        @Override
        protected Object visitNode(Node node, Object context)
        {
            return null;
        }

        @Override
        protected Object visitIdentifier(Identifier node, Object context)
        {
            return null;
        }
    }
}
