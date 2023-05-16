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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.graphmdl.base.GraphMDL;
import io.graphmdl.base.SessionContext;
import io.graphmdl.sqlrewrite.analyzer.Field;
import io.graphmdl.sqlrewrite.analyzer.Scope;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.SubscriptExpression;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterables.getLast;
import static io.graphmdl.sqlrewrite.Utils.analyzeFrom;
import static io.graphmdl.sqlrewrite.Utils.getNextPart;
import static io.graphmdl.sqlrewrite.Utils.toQualifiedName;
import static io.trino.sql.QueryUtil.identifier;
import static java.lang.String.format;

/**
 * Rewrite the AST to replace all identifiers or dereference expressions
 * without a relation prefix with the relation prefix.
 */
public class ScopeAwareRewrite
{
    public static final ScopeAwareRewrite SCOPE_AWARE_REWRITE = new ScopeAwareRewrite();

    public Statement rewrite(Node root, GraphMDL graphMDL, SessionContext sessionContext)
    {
        return (Statement) new Rewriter(graphMDL, sessionContext).process(root);
    }

    private static class Rewriter
            extends BaseRewriter<Scope>
    {
        private final GraphMDL graphMDL;
        private final SessionContext sessionContext;

        public Rewriter(GraphMDL graphMDL, SessionContext sessionContext)
        {
            this.graphMDL = graphMDL;
            this.sessionContext = sessionContext;
        }

        @Override
        protected Node visitQuerySpecification(QuerySpecification node, Scope context)
        {
            Scope relationScope;
            if (node.getFrom().isPresent()) {
                relationScope = analyzeFrom(graphMDL, sessionContext, node.getFrom().get(), Optional.ofNullable(context));
            }
            else {
                relationScope = context;
            }
            return super.visitQuerySpecification(node, relationScope);
        }

        @Override
        protected Node visitIdentifier(Identifier node, Scope context)
        {
            if (context.getRelationType().isPresent()) {
                List<Field> field = context.getRelationType().get().resolveFields(QualifiedName.of(node.getValue()));
                if (field.size() == 1) {
                    return new DereferenceExpression(identifier(field.get(0).getRelationAlias()
                            .orElse(toQualifiedName(field.get(0).getModelName()))
                            .getSuffix()), identifier(field.get(0).getColumnName()));
                }
                if (field.size() > 1) {
                    throw new IllegalArgumentException("Ambiguous column name: " + node.getValue());
                }
            }
            return node;
        }

        @Override
        protected Node visitDereferenceExpression(DereferenceExpression node, Scope context)
        {
            if (context != null && context.getRelationType().isPresent()) {
                List<String> parts = getParts(node);
                for (int i = 0; i < parts.size(); i++) {
                    List<Field> field = context.getRelationType().get().resolveFields(QualifiedName.of(parts.subList(0, i + 1)));
                    if (field.size() == 1) {
                        if (i > 0) {
                            // The node is resolvable and has the relation prefix. No need to rewrite.
                            return node;
                        }
                        return addPrefix(node, identifier(field.get(0).getRelationAlias().orElse(toQualifiedName(field.get(0).getModelName())).getSuffix()));
                    }
                    if (field.size() > 1) {
                        throw new IllegalArgumentException("Ambiguous column name: " + DereferenceExpression.getQualifiedName(node));
                    }
                }
            }
            return node;
        }

        private List<String> getParts(Expression expression)
        {
            if (expression instanceof Identifier) {
                return ImmutableList.of(((Identifier) expression).getValue());
            }
            else if (expression instanceof DereferenceExpression) {
                DereferenceExpression dereferenceExpression = (DereferenceExpression) expression;
                List<String> baseQualifiedName = getParts(dereferenceExpression.getBase());
                ImmutableList.Builder<String> builder = ImmutableList.builder();
                builder.addAll(baseQualifiedName);
                builder.add(dereferenceExpression.getField().orElseThrow().getValue());
                return builder.build();
            }
            else if (expression instanceof SubscriptExpression) {
                SubscriptExpression subscriptExpression = (SubscriptExpression) expression;
                List<String> baseQualifiedName = getParts(subscriptExpression.getBase());
                if (baseQualifiedName != null) {
                    ImmutableList.Builder<String> builder = ImmutableList.builder();
                    builder.addAll(baseQualifiedName.subList(0, baseQualifiedName.size() - 1));
                    builder.add(format("%s[%s]", getLast(baseQualifiedName), subscriptExpression.getIndex().toString()));
                    return builder.build();
                }
            }
            return ImmutableList.of();
        }
    }

    @VisibleForTesting
    public static Expression addPrefix(Expression source, Identifier prefix)
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();

        Expression node = source;
        while (node instanceof DereferenceExpression || node instanceof SubscriptExpression) {
            if (node instanceof DereferenceExpression) {
                DereferenceExpression dereferenceExpression = (DereferenceExpression) node;
                builder.add(dereferenceExpression.getField().orElseThrow());
                node = dereferenceExpression.getBase();
            }
            else {
                SubscriptExpression subscriptExpression = (SubscriptExpression) node;
                Identifier base;
                if (subscriptExpression.getBase() instanceof Identifier) {
                    base = (Identifier) subscriptExpression.getBase();
                }
                else {
                    base = ((DereferenceExpression) subscriptExpression.getBase()).getField().orElseThrow();
                }
                builder.add(new SubscriptExpression(base, subscriptExpression.getIndex()));
                node = getNextPart(subscriptExpression);
            }
        }

        if (node instanceof Identifier) {
            builder.add(node);
        }

        return builder.add(prefix).build().reverse().stream().reduce((a, b) -> {
            if (b instanceof SubscriptExpression) {
                SubscriptExpression subscriptExpression = (SubscriptExpression) b;
                return new SubscriptExpression(new DereferenceExpression(a, (Identifier) subscriptExpression.getBase()), ((SubscriptExpression) b).getIndex());
            }
            else if (b instanceof Identifier) {
                return new DereferenceExpression(a, (Identifier) b);
            }
            throw new IllegalArgumentException(format("Unexpected expression: %s", b));
        }).orElseThrow(() -> new IllegalArgumentException(format("Unexpected expression: %s", source)));
    }
}
