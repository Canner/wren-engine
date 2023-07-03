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

package io.accio.sqlrewrite;

import io.accio.base.AccioMDL;
import io.accio.base.SessionContext;
import io.accio.base.dto.Model;
import io.accio.sqlrewrite.analyzer.Analysis;
import io.accio.sqlrewrite.analyzer.Field;
import io.accio.sqlrewrite.analyzer.Scope;
import io.accio.sqlrewrite.analyzer.StatementAnalyzer;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.SubscriptExpression;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Rewrite syntactic sugar to standard Accio SQL:
 * <p>
 * 1. Directly access a relationship field. (e.g. `select author from Book` -> `select author.userId from Book`)
 * 2. `any` Function is an alias of to-many result accessing. (e.g. `select any(books) from User` -> `select books[1] from User`)
 * 3. `first` Function is an alias of sorted to-many result accessing. (e.g. `select first(books) from User` -> `select array_sort(books)[1] from User`)
 */
public class SyntacticSugarRewrite
        implements AccioRule
{
    public static final SyntacticSugarRewrite SYNTACTIC_SUGAR_REWRITE = new SyntacticSugarRewrite();

    @Override
    public Statement apply(Statement root, SessionContext sessionContext, AccioMDL accioMDL)
    {
        Analysis analysis = StatementAnalyzer.analyze(root, sessionContext, accioMDL);
        return apply(root, sessionContext, analysis, accioMDL);
    }

    @Override
    public Statement apply(Statement root, SessionContext sessionContext, Analysis analysis, AccioMDL accioMDL)
    {
        return (Statement) new SyntacticSugarRewrite.Rewriter(accioMDL, analysis).process(root);
    }

    private static class Rewriter
            extends BaseRewriter<Void>
    {
        private final AccioMDL accioMDL;
        private final Analysis analysis;

        Rewriter(AccioMDL accioMDL, Analysis analysis)
        {
            this.accioMDL = accioMDL;
            this.analysis = analysis;
        }

        @Override
        protected Node visitIdentifier(Identifier node, Void context)
        {
            // To directly access the relationship, we rewrite it to its primary key.
            QualifiedName qualifiedName = QualifiedName.of(List.of(node));
            return analysis.tryGetScope(node)
                    .flatMap(Scope::getRelationType)
                    .map(relationType -> relationType.resolveAnyField(qualifiedName))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .filter(Field::isRelationship).flatMap(field -> accioMDL.getModel(field.getType())
                            .map(Model::getPrimaryKey)
                            .map(Identifier::new))
                    .map(primaryKey -> (Node) new DereferenceExpression(node, primaryKey))
                    .orElse(node);
        }

        @Override
        protected Node visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            QualifiedName qualifiedName = DereferenceExpression.getQualifiedName(node);
            return analysis.tryGetScope(node)
                    .flatMap(Scope::getRelationType)
                    .map(relationType -> relationType.resolveAnyField(qualifiedName))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .filter(Field::isRelationship).flatMap(field -> accioMDL.getModel(field.getType())
                            .map(Model::getPrimaryKey)
                            .map(Identifier::new))
                    .map(primaryKey -> (Node) new DereferenceExpression(node, primaryKey))
                    .orElse(super.visitDereferenceExpression(node, context));
        }

        @Override
        protected Node visitSingleColumn(SingleColumn node, Void context)
        {
            // Because we rewrite the relationship field to its primary key,
            // we need to add an alias to keep its original name.
            Expression result = visitAndCast(node.getExpression(), context);

            if (result.equals(node.getExpression())) {
                return node;
            }

            Identifier resultAlias = node.getAlias().orElse(null);
            if (node.getExpression() instanceof Identifier) {
                Identifier identifier = (Identifier) node.getExpression();
                resultAlias = node.getAlias().orElse(identifier);
            }
            else if (node.getExpression() instanceof DereferenceExpression) {
                DereferenceExpression dereferenceExpression = (DereferenceExpression) node.getExpression();
                resultAlias = node.getAlias().orElse(dereferenceExpression.getField().orElse(null));
            }
            if (node.getLocation().isPresent()) {
                return new SingleColumn(node.getLocation().get(), result, Optional.ofNullable(resultAlias));
            }
            return new SingleColumn(result, Optional.ofNullable(resultAlias));
        }

        @Override
        protected Node visitFunctionCall(FunctionCall node, Void context)
        {
            String name = node.getName().toString();
            if (name.equalsIgnoreCase("any")) {
                return new SubscriptExpression(requireNonNull(node.getArguments().get(0)), new LongLiteral("1"));
            }
            if (node.getName().toString().equalsIgnoreCase("first")) {
                return new SubscriptExpression(new FunctionCall(QualifiedName.of("array_sort"), node.getArguments()), new LongLiteral("1"));
            }
            return super.visitFunctionCall(node, context);
        }
    }
}
