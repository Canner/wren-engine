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

package io.wren.base.sqlrewrite.analyzer.decisionpoint;

import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.FunctionRelation;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.JoinCriteria;
import io.trino.sql.tree.JoinOn;
import io.trino.sql.tree.JoinUsing;
import io.trino.sql.tree.Lateral;
import io.trino.sql.tree.NaturalJoin;
import io.trino.sql.tree.PatternRecognitionRelation;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.SampledRelation;
import io.trino.sql.tree.SetOperation;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.Unnest;
import io.trino.sql.tree.Values;
import io.wren.base.SessionContext;
import io.wren.base.WrenMDL;

import java.util.List;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class RelationAnalyzer
{
    private RelationAnalyzer() {}

    public static RelationAnalysis analyze(Relation relation, SessionContext sessionContext, WrenMDL wrenMDL)
    {
        return new Visitor(sessionContext, wrenMDL).process(relation, null);
    }

    static class Visitor
            extends AstVisitor<RelationAnalysis, Void>
    {
        private final SessionContext sessionContext;
        private final WrenMDL wrenMDL;

        public Visitor(SessionContext sessionContext, WrenMDL wrenMDL)
        {
            this.sessionContext = sessionContext;
            this.wrenMDL = wrenMDL;
        }

        @Override
        protected RelationAnalysis visitTable(Table node, Void context)
        {
            return new RelationAnalysis.TableRelation(node.getName().toString(), null);
        }

        @Override
        protected RelationAnalysis visitSetOperation(SetOperation node, Void context)
        {
            // TODO: implement this
            // except, intersect, union
            throw new UnsupportedOperationException("Analyze Set operation is not supported yet");
        }

        @Override
        protected RelationAnalysis visitValues(Values node, Void context)
        {
            // TODO: implement this
            throw new UnsupportedOperationException("Analyze Values is not supported yet");
        }

        @Override
        protected RelationAnalysis visitFunctionRelation(FunctionRelation node, Void context)
        {
            // TODO: implement this
            throw new UnsupportedOperationException("Analyze FunctionRelation is not supported yet");
        }

        @Override
        protected RelationAnalysis visitTableSubquery(TableSubquery node, Void context)
        {
            List<QueryAnalysis> analyses = DecisionPointAnalyzer.analyze(node.getQuery(), sessionContext, wrenMDL);
            return new RelationAnalysis.SubqueryRelation(null, analyses);
        }

        @Override
        protected RelationAnalysis visitQuerySpecification(QuerySpecification node, Void context)
        {
            // TODO: implement this
            return super.visitQuerySpecification(node, context);
        }

        @Override
        protected RelationAnalysis visitJoin(Join node, Void context)
        {
            RelationAnalysis left = process(node.getLeft(), context);
            RelationAnalysis right = process(node.getRight(), context);
            return new RelationAnalysis.JoinRelation(
                    RelationAnalysis.Type.valueOf(format("%s_JOIN", node.getType())),
                    null, left, right, node.getCriteria().map(this::formatCriteria).orElse(null));
        }

        private String formatCriteria(JoinCriteria criteria)
        {
            StringBuilder builder = new StringBuilder();
            switch (criteria) {
                case JoinOn joinOn:
                    builder.append("ON ");
                    builder.append(joinOn.getExpression());
                    break;
                case JoinUsing joinUsing:
                    builder.append("USING (");
                    builder.append(joinUsing.getColumns().stream().map(Identifier::getValue).collect(joining(", ")));
                    builder.append(")");
                    break;
                case NaturalJoin ignored:
                    return null;
                default:
                    throw new IllegalArgumentException("Unsupported join criteria: " + criteria);
            }
            return builder.toString();
        }

        @Override
        protected RelationAnalysis visitAliasedRelation(AliasedRelation node, Void context)
        {
            RelationAnalysis relationAnalysis = process(node.getRelation(), context);

            return switch (relationAnalysis) {
                case RelationAnalysis.TableRelation tableRelation -> RelationAnalysis.table(tableRelation.getTableName(), node.getAlias().getValue());
                case RelationAnalysis.JoinRelation joinRelation ->
                        RelationAnalysis.join(joinRelation.getType(), node.getAlias().getValue(), joinRelation.getLeft(), joinRelation.getRight(), joinRelation.getCriteria());
                case RelationAnalysis.SubqueryRelation subqueryRelation -> RelationAnalysis.subquery(node.getAlias().getValue(), subqueryRelation.getBody());
                default -> throw new IllegalStateException("Unexpected value: " + relationAnalysis);
            };
        }

        @Override
        protected RelationAnalysis visitSampledRelation(SampledRelation node, Void context)
        {
            // TODO: implement this
            throw new UnsupportedOperationException("Analyze SampledRelation is not supported yet");
        }

        @Override
        protected RelationAnalysis visitPatternRecognitionRelation(PatternRecognitionRelation node, Void context)
        {
            // TODO: implement this
            throw new UnsupportedOperationException("Analyze PatternRecognitionRelation is not supported yet");
        }

        @Override
        protected RelationAnalysis visitUnnest(Unnest node, Void context)
        {
            // TODO: implement this
            throw new UnsupportedOperationException("Analyze Unnest is not supported yet");
        }

        @Override
        protected RelationAnalysis visitLateral(Lateral node, Void context)
        {
            // TODO: implement this
            throw new UnsupportedOperationException("Analyze Lateral is not supported yet");
        }
    }
}
