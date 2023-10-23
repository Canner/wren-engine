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
import io.accio.base.dto.EnumDefinition;
import io.accio.base.dto.EnumValue;
import io.accio.base.dto.Model;
import io.accio.sqlrewrite.analyzer.Analysis;
import io.accio.sqlrewrite.analyzer.StatementAnalyzer;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionRelation;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class AccioSqlRewrite
        implements AccioRule
{
    public static final AccioSqlRewrite ACCIO_SQL_REWRITE = new AccioSqlRewrite();

    private AccioSqlRewrite() {}

    @Override
    public Statement apply(Statement root, SessionContext sessionContext, Analysis analysis, AccioMDL accioMDL)
    {
        Map<String, Query> modelQueries =
                analysis.getModels().stream()
                        .collect(toUnmodifiableMap(Model::getName, Utils::parseModelSql));

        Node rewriteWith = new WithRewriter(
                modelQueries.entrySet()
                        .stream().map(e -> new WithQuery(new Identifier(e.getKey()), e.getValue(), Optional.empty()))
                        .collect(toList()))
                .process(root);
        return (Statement) new Rewriter(analysis, accioMDL).process(rewriteWith);
    }

    @Override
    public Statement apply(Statement root, SessionContext sessionContext, AccioMDL accioMDL)
    {
        Analysis analysis = StatementAnalyzer.analyze(root, sessionContext, accioMDL);
        return apply(root, sessionContext, analysis, accioMDL);
    }

    private static class WithRewriter
            extends BaseRewriter<Void>
    {
        private final List<WithQuery> withQueries;

        public WithRewriter(List<WithQuery> withQueries)
        {
            this.withQueries = requireNonNull(withQueries, "withQueries is null");
        }

        @Override
        protected Node visitQuery(Query node, Void context)
        {
            return new Query(
                    node.getWith()
                            .map(with -> new With(
                                    with.isRecursive(),
                                    // model queries must come first since with-queries may use models
                                    // and tables in with query should all be in order.
                                    Stream.concat(withQueries.stream(), with.getQueries().stream())
                                            .collect(toUnmodifiableList())))
                            .or(() -> withQueries.isEmpty() ? Optional.empty() : Optional.of(new With(false, withQueries))),
                    node.getQueryBody(),
                    node.getOrderBy(),
                    node.getOffset(),
                    node.getLimit());
        }
    }

    private static class Rewriter
            extends BaseRewriter<Void>
    {
        private final Analysis analysis;
        private final AccioMDL accioMDL;

        Rewriter(Analysis analysis, AccioMDL accioMDL)
        {
            this.analysis = analysis;
            this.accioMDL = accioMDL;
        }

        @Override
        protected Node visitTable(Table node, Void context)
        {
            Node result = node;
            if (analysis.getModelNodeRefs().contains(NodeRef.of(node))) {
                result = applyModelRule(node);
            }
            return result;
        }

        @Override
        protected Node visitFunctionRelation(FunctionRelation node, Void context)
        {
            if (analysis.getMetricRollups().containsKey(NodeRef.of(node))) {
                return new Table(QualifiedName.of(analysis.getMetricRollups().get(NodeRef.of(node)).getMetric().getName()));
            }
            // this should not happen, every MetricRollup node should be captured and syntax checked in StatementAnalyzer
            throw new IllegalArgumentException("MetricRollup node is not replaced");
        }

        @Override
        protected Node visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            Expression newNode = rewriteEnumIfNeed(node);
            if (newNode != node) {
                return newNode;
            }
            return new DereferenceExpression(node.getLocation(), (Expression) process(node.getBase()), node.getField());
        }

        private Expression rewriteEnumIfNeed(DereferenceExpression node)
        {
            QualifiedName qualifiedName = DereferenceExpression.getQualifiedName(node);
            if (qualifiedName == null || qualifiedName.getOriginalParts().size() != 2) {
                return node;
            }

            String enumName = qualifiedName.getOriginalParts().get(0).getValue();
            Optional<EnumDefinition> enumDefinitionOptional = accioMDL.getEnum(enumName);
            if (enumDefinitionOptional.isEmpty()) {
                return node;
            }

            return enumDefinitionOptional.get().valueOf(qualifiedName.getOriginalParts().get(1).getValue())
                    .map(EnumValue::getValue)
                    .map(StringLiteral::new)
                    .orElseThrow(() -> new IllegalArgumentException(format("Enum value '%s' not found in enum '%s'", qualifiedName.getParts().get(1), qualifiedName.getParts().get(0))));
        }

        // the model is added in with query, and the catalog and schema should be removed
        private Node applyModelRule(Table table)
        {
            return new Table(QualifiedName.of(table.getName().getSuffix()));
        }
    }
}
