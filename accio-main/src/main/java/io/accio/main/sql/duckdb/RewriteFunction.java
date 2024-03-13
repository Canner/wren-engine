package io.accio.main.sql.duckdb;

import io.accio.base.sqlrewrite.BaseRewriter;
import io.accio.main.metadata.Metadata;
import io.accio.main.sql.SqlRewrite;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;

import static java.util.Objects.requireNonNull;

public class RewriteFunction
        implements SqlRewrite
{
    public static final RewriteFunction INSTANCE = new RewriteFunction();

    private RewriteFunction() {}

    @Override
    public Node rewrite(Node node, Metadata metadata)
    {
        return new RewriteFunctionRewriter(metadata).process(node, null);
    }

    private static class RewriteFunctionRewriter
            extends BaseRewriter<Void>
    {
        private final Metadata metadata;

        public RewriteFunctionRewriter(Metadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        protected Node visitFunctionCall(FunctionCall node, Void context)
        {
            QualifiedName functionName = metadata.resolveFunction(node.getName().toString(), node.getArguments().size());

            FunctionCall newFunctionNode = FunctionCall.builder(node)
                    .name(functionName)
                    .build();

            return super.visitFunctionCall(newFunctionNode, context);
        }
    }
}
