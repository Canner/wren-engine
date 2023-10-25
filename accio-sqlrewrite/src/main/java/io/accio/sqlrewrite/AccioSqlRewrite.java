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

import com.google.common.collect.ImmutableSet;
import io.accio.base.AccioMDL;
import io.accio.base.SessionContext;
import io.accio.sqlrewrite.analyzer.Analysis;
import io.accio.sqlrewrite.analyzer.StatementAnalyzer;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.FunctionRelation;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.graph.GraphCycleProhibitedException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Strings.nullToEmpty;
import static io.accio.sqlrewrite.Utils.parseQuery;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Collectors.toUnmodifiableList;

public class AccioSqlRewrite
        implements AccioRule
{
    public static final AccioSqlRewrite ACCIO_SQL_REWRITE = new AccioSqlRewrite();

    private AccioSqlRewrite() {}

    @Override
    public Statement apply(Statement root, SessionContext sessionContext, Analysis analysis, AccioMDL accioMDL)
    {
        DirectedAcyclicGraph<String, Object> graph = new DirectedAcyclicGraph<>(Object.class);
        Set<ModelInfo> modelInfos = analysis.getModels().stream().map(model -> ModelInfo.get(model, accioMDL)).collect(toSet());
        Set<ModelInfo> requiredModelInfos = new HashSet<>();
        modelInfos.forEach(modelInfo -> addModelToGraph(modelInfo, graph, accioMDL, requiredModelInfos));
        Set<ModelInfo> allModelInfos = ImmutableSet.<ModelInfo>builder().addAll(modelInfos).addAll(requiredModelInfos).build();

        List<WithQuery> withQueries = new ArrayList<>();
        graph.iterator().forEachRemaining(modelName -> {
            ModelInfo modelInfo = allModelInfos.stream()
                    .filter(info -> info.getModel().getName().equals(modelName))
                    .findAny()
                    .orElseThrow(() -> new IllegalArgumentException(format("Missing model name %s in graph", modelName)));
            withQueries.add(new WithQuery(new Identifier(modelInfo.getModel().getName()), parseQuery(modelInfo.getSql()), Optional.empty()));
        });

        Node rewriteWith = new WithRewriter(withQueries).process(root);
        return (Statement) new Rewriter(accioMDL, analysis).process(rewriteWith);
    }

    private static void addModelToGraph(ModelInfo modelInfo, DirectedAcyclicGraph<String, Object> graph, AccioMDL mdl, Set<ModelInfo> modelInfos)
    {
        // add vertex
        graph.addVertex(modelInfo.getModel().getName());
        modelInfo.getRequiredModels().forEach(graph::addVertex);

        //add edge
        try {
            modelInfo.getRequiredModels().forEach(modelName ->
                    graph.addEdge(modelName, modelInfo.getModel().getName()));
        }
        catch (GraphCycleProhibitedException ex) {
            throw new IllegalArgumentException("found cycle in models", ex);
        }

        // add required models to graph
        for (String modelName : modelInfo.getRequiredModels()) {
            ModelInfo info = ModelInfo.get(mdl.getModel(modelName).orElseThrow(), mdl);
            modelInfos.add(info);
            addModelToGraph(info, graph, mdl, modelInfos);
        }
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
        private final AccioMDL accioMDL;
        private final Analysis analysis;

        Rewriter(AccioMDL accioMDL, Analysis analysis)
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

        // remove catalog schema from expression if exist since all tables are in with cte
        @Override
        protected Node visitDereferenceExpression(DereferenceExpression dereferenceExpression, Void context)
        {
            QualifiedName qualifiedName = DereferenceExpression.getQualifiedName(dereferenceExpression);
            if (qualifiedName != null && !nullToEmpty(accioMDL.getCatalog()).isEmpty() && !nullToEmpty(accioMDL.getSchema()).isEmpty()) {
                if (qualifiedName.hasPrefix(QualifiedName.of(accioMDL.getCatalog(), accioMDL.getSchema()))) {
                    return DereferenceExpression.from(
                            QualifiedName.of(qualifiedName.getOriginalParts().subList(2, qualifiedName.getOriginalParts().size())));
                }
                if (qualifiedName.hasPrefix(QualifiedName.of(accioMDL.getSchema()))) {
                    return DereferenceExpression.from(
                            QualifiedName.of(qualifiedName.getOriginalParts().subList(1, qualifiedName.getOriginalParts().size())));
                }
            }
            return dereferenceExpression;
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

        // the model is added in with query, and the catalog and schema should be removed
        private Node applyModelRule(Table table)
        {
            return new Table(QualifiedName.of(table.getName().getSuffix()));
        }
    }
}
