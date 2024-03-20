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

package io.wren.main.wireprotocol;

import io.trino.sql.tree.Cast;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Statement;
import io.wren.main.pgcatalog.regtype.RegObjectFactory;
import io.wren.main.sql.RegObjectInterpreter;

import java.util.List;
import java.util.Optional;

import static io.wren.main.sql.PgOidTypeTableInfo.REGCLASS;
import static io.wren.main.sql.PgOidTypeTableInfo.REGPROC;
import static java.util.Locale.ROOT;
import static java.util.Objects.requireNonNull;

public class MetastoreSqlRewrite
{
    public static Statement rewrite(RegObjectFactory regObjectFactory, Statement statement)
    {
        return (Statement) new Visitor(new RegObjectInterpreter(regObjectFactory)).process(statement);
    }

    private MetastoreSqlRewrite() {}

    private static class Visitor
            extends BaseRewriteVisitor<Void>
    {
        private final RegObjectInterpreter regObjectInterpreter;

        public Visitor(RegObjectInterpreter regObjectInterpreter)
        {
            this.regObjectInterpreter = requireNonNull(regObjectInterpreter, "regObjectInterpreter is null");
        }

        @Override
        protected Node visitSingleColumn(SingleColumn node, Void context)
        {
            Expression expression = node.getExpression();
            // Show regObject's name when it's in a single column.
            if (expression instanceof GenericLiteral || expression instanceof Cast) {
                Optional<Object> result = regObjectInterpreter.evaluate(expression, true);
                expression = result.map(obj -> (Expression) obj).orElse(expression);
            }

            return (node.getLocation().isPresent()) ?
                    new SingleColumn(
                            node.getLocation().get(),
                            visitAndCast(expression),
                            node.getAlias()) :
                    new SingleColumn(
                            visitAndCast(expression),
                            node.getAlias());
        }

        @Override
        protected Node visitFunctionCall(FunctionCall node, Void context)
        {
            if (node.getName().equals(QualifiedName.of("information_schema", "_pg_expandarray"))) {
                return new FunctionCall(
                        node.getLocation().get(),
                        QualifiedName.of("_pg_expandarray"),
                        node.getArguments());
            }
            else if (node.getName().equals(QualifiedName.of("pg_relation_size")) &&
                    node.getArguments().size() == 1) {
                return new FunctionCall(
                        node.getLocation().get(),
                        QualifiedName.of("pg_relation_size"),
                        List.of(node.getArguments().get(0), new NullLiteral()));
            }
            // duckdb only support pg_get_expr(pg_node_tree, relation_oid)
            else if (node.getName().equals(QualifiedName.of("pg_get_expr")) &&
                    node.getArguments().size() == 3) {
                return new FunctionCall(
                        node.getLocation().get(),
                        QualifiedName.of("pg_get_expr"),
                        List.of(node.getArguments().get(0), node.getArguments().get(1)));
            }
            return node;
        }

        @Override
        protected Node visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            return super.visitDereferenceExpression(node, context);
        }

        @Override
        protected Node visitCast(Cast node, Void context)
        {
            String type = node.getType().toString().toUpperCase(ROOT);
            if (type.equals(REGPROC.name()) || type.equals(REGCLASS.name())) {
                Optional<Object> result = regObjectInterpreter.evaluate(node, false);
                return result.map(obj -> (Expression) obj).orElse(node);
            }

            return node;
        }

        @Override
        protected Node visitExpression(Expression expression, Void context)
        {
            Optional<Object> result = regObjectInterpreter.evaluate(expression, false);
            return result.map(o -> (Node) o).orElse(expression);
        }
    }
}
