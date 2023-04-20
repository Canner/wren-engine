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

import com.google.common.collect.ImmutableList;
import io.graphmdl.base.GraphMDL;
import io.graphmdl.base.SessionContext;
import io.graphmdl.base.dto.Model;
import io.graphmdl.sqlrewrite.analyzer.Analysis;
import io.graphmdl.sqlrewrite.analyzer.Field;
import io.graphmdl.sqlrewrite.analyzer.Scope;
import io.graphmdl.sqlrewrite.analyzer.StatementAnalyzer;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GroupBy;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SimpleGroupBy;
import io.trino.sql.tree.Statement;

import java.util.Optional;

import static io.graphmdl.sqlrewrite.analyzer.Analysis.GroupByAnalysis;
import static io.trino.sql.QueryUtil.getQualifiedName;
import static java.lang.Math.toIntExact;

/**
 * If the grouping element is a relationship field,
 * rewrite it to the corresponding model's primary key.
 * <p>
 * SELECT count(*) FROM Book GROUP BY author -> SELECT count(*) FROM Book GROUP BY author.userId
 * <p>
 * If the grouping element is a ordinal field and the corresponding SelectItem is a relationship field,
 * rewrite it to the corresponding model's primary key.
 * <p>
 * SELECT author, count(*) FROM Book GROUP BY 1 -> SELECT author, count(*) FROM Book GROUP BY author.userId
 */
public class GroupByKeyRewrite
        implements GraphMDLRule
{
    public static final GroupByKeyRewrite GROUP_BY_KEY_REWRITE = new GroupByKeyRewrite();

    @Override
    public Statement apply(Statement root, SessionContext sessionContext, Analysis analysis, GraphMDL graphMDL)
    {
        return (Statement) new Rewriter(graphMDL, analysis).process(root);
    }

    @Override
    public Statement apply(Statement root, SessionContext sessionContext, GraphMDL graphMDL)
    {
        Analysis analysis = StatementAnalyzer.analyze(root, sessionContext, graphMDL);
        return apply(root, sessionContext, analysis, graphMDL);
    }

    static class Rewriter
            extends BaseRewriter<GroupByAnalysis>
    {
        private final GraphMDL graphMDL;
        private final Analysis analysis;

        Rewriter(GraphMDL graphMDL, Analysis analysis)
        {
            this.graphMDL = graphMDL;
            this.analysis = analysis;
        }

        @Override
        protected Node visitGroupBy(GroupBy node, GroupByAnalysis context)
        {
            GroupByAnalysis groupByAnalysis = analysis.getGroupByAnalysis().get(NodeRef.of(node));
            return super.visitGroupBy(node, groupByAnalysis);
        }

        @Override
        protected Node visitSimpleGroupBy(SimpleGroupBy node, GroupByAnalysis context)
        {
            ImmutableList.Builder<Expression> builder = ImmutableList.builder();
            for (Expression expression : node.getExpressions()) {
                Optional<DereferenceExpression> rewritten = analysis.tryGetScope(expression)
                        .map(scope -> rewriteGroupByKeyIfNeeded(scope, expression, context))
                        .orElseThrow(() -> new IllegalStateException("No scope found for " + expression));
                if (rewritten.isPresent()) {
                    builder.add(rewritten.get());
                }
                else {
                    builder.add(expression);
                }
            }
            return new SimpleGroupBy(builder.build());
        }

        private Optional<DereferenceExpression> rewriteGroupByKeyIfNeeded(Scope scope, Expression key, GroupByAnalysis groupByAnalysis)
        {
            if (key instanceof LongLiteral) {
                Expression expression = groupByAnalysis.getOriginalExpressions().get(toIntExact(((LongLiteral) key).getValue()) - 1);
                Optional<Field> field = scope.getRelationType().map(relationType -> relationType.resolveFields(getQualifiedName(expression)))
                        // If it can't be resolved, it means it could be a field of a relationship or ambiguous.
                        .map(fields -> fields.size() == 1 ? fields.get(0) : null);
                if (field.isPresent() && field.get().isRelationship()) {
                    return graphMDL.getModel(field.get().getType()).map(Model::getPrimaryKey)
                            .map(primaryKey -> new DereferenceExpression(
                                    field.get().getName().map(QualifiedName::of)
                                            .map(DereferenceExpression::from)
                                            .orElseThrow(() -> new IllegalStateException("No field name found for" + key)),
                                    new Identifier(primaryKey)));
                }
                return Optional.empty();
            }

            return scope.getRelationType().map(relationType -> relationType.resolveFields(getQualifiedName(key)).get(0))
                    .filter(Field::isRelationship)
                    .map(field -> graphMDL.getModel(field.getType()).map(Model::getPrimaryKey)
                            .map(primaryKey -> new DereferenceExpression(key, new Identifier(primaryKey)))
                            .orElseThrow(() -> new IllegalStateException("No model found for " + field.getType())));
        }
    }
}
