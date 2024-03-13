package io.accio.main.sql.duckdb;

import io.accio.base.sqlrewrite.BaseRewriter;
import io.accio.main.metadata.Metadata;
import io.accio.main.sql.SqlRewrite;
import io.trino.sql.tree.ArrayConstructor;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SubscriptExpression;

public class RewriteArray
        implements SqlRewrite
{
    public static final RewriteArray INSTANCE = new RewriteArray();

    private RewriteArray() {}

    @Override
    public Node rewrite(Node node, Metadata metadata)
    {
        return new RewriteArrayRewriter().process(node, null);
    }

    private static class RewriteArrayRewriter
            extends BaseRewriter<Void>
    {
        @Override
        protected Node visitSubscriptExpression(SubscriptExpression node, Void context)
        {
            Expression base = node.getBase();
            if (base instanceof ArrayConstructor) {
                base = convertToArrayValue((ArrayConstructor) base);
            }
            if (node.getLocation().isPresent()) {
                return new SubscriptExpression(
                        node.getLocation().get(),
                        visitAndCast(base, context),
                        visitAndCast(node.getIndex(), context));
            }
            return new SubscriptExpression(visitAndCast(base, context), visitAndCast(node.getIndex(), context));
        }

        private Expression convertToArrayValue(ArrayConstructor node)
        {
            if (node.getLocation().isPresent()) {
                return new FunctionCall(
                        node.getLocation().get(),
                        QualifiedName.of("array_value"),
                        node.getValues());
            }
            return new FunctionCall(
                    QualifiedName.of("array_value"),
                    node.getValues());
        }
    }
}
