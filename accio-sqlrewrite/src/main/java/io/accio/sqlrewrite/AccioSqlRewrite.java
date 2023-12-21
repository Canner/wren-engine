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
import io.accio.base.dto.Model;
import io.accio.sqlrewrite.analyzer.Analysis;
import io.accio.sqlrewrite.analyzer.StatementAnalyzer;
import io.trino.sql.tree.DereferenceExpression;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.accio.base.Utils.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Collectors.toUnmodifiableList;

public class AccioSqlRewrite
        implements AccioRule
{
    public static final AccioSqlRewrite ACCIO_SQL_REWRITE = new AccioSqlRewrite();

    private AccioSqlRewrite() {}

    private LinkedHashMap<String, Set<String>> getRequiredFields(AccioDataLineage dataLineage, Analysis analysis)
    {
        List<QualifiedName> collectedColumns = analysis.getCollectedColumns().asMap().entrySet().stream()
                .map(e ->
                        e.getValue().stream()
                                .map(columnName -> QualifiedName.of(e.getKey().getSchemaTableName().getTableName(), columnName))
                                .collect(toImmutableList()))
                .flatMap(List::stream)
                .collect(toImmutableList());
        return dataLineage.getRequiredFields(collectedColumns);
    }

    @Override
    public Statement apply(Statement root, SessionContext sessionContext, Analysis analysis, AccioMDL accioMDL)
    {
        Set<QueryDescriptor> allDescriptors;
        // TODO: Currently DynamicCalculatedField is a experimental feature, and buggy. After all issues are solved,
        //  we should always enable this setting.
        if (sessionContext.isEnableDynamicCalculatedField()) {
            // TODO: make AccioDataLineage static instead of create a new one everytime as it will change only when accio mdl changed.
            AccioDataLineage dataLineage = AccioDataLineage.analyze(accioMDL);
            LinkedHashMap<String, Set<String>> modelRequiredFields = getRequiredFields(dataLineage, analysis);
            List<QueryDescriptor> modelDescriptors = modelRequiredFields.entrySet().stream()
                    .map(e -> {
                        String modelName = e.getKey();
                        Model model = accioMDL.getModel(modelName).orElseThrow();
                        return RelationInfo.get(model, accioMDL, e.getValue());
                    })
                    .collect(toImmutableList());

            List<WithQuery> withQueries = new ArrayList<>();
            modelDescriptors.forEach(queryDescriptor -> withQueries.add(getWithQuery(queryDescriptor)));

            Node rewriteWith = new WithRewriter(withQueries).process(root);
            return (Statement) new Rewriter(accioMDL, analysis).process(rewriteWith);
        }
        else {
            Set<QueryDescriptor> modelDescriptors = analysis.getModels().stream().map(model -> RelationInfo.get(model, accioMDL)).collect(toSet());
            Set<QueryDescriptor> metricDescriptors = analysis.getMetrics().stream().map(metric -> RelationInfo.get(metric, accioMDL)).collect(toSet());
            Set<QueryDescriptor> cumulativeMetricDescriptors = analysis.getCumulativeMetrics().stream().map(metric -> CumulativeMetricInfo.get(metric, accioMDL)).collect(toSet());
            Set<QueryDescriptor> viewDescriptors = analysis.getViews().stream().map(view -> ViewInfo.get(view, accioMDL, sessionContext)).collect(toSet());
            allDescriptors = ImmutableSet.<QueryDescriptor>builder()
                    .addAll(modelDescriptors)
                    .addAll(metricDescriptors)
                    .addAll(viewDescriptors)
                    .addAll(cumulativeMetricDescriptors)
                    .build();
            return apply(root, sessionContext, analysis, accioMDL, allDescriptors);
        }
    }

    private Statement apply(
            Statement root,
            SessionContext sessionContext,
            Analysis analysis,
            AccioMDL accioMDL,
            Set<QueryDescriptor> allDescriptors)
    {
        DirectedAcyclicGraph<String, Object> graph = new DirectedAcyclicGraph<>(Object.class);
        Set<QueryDescriptor> requiredQueryDescriptors = new HashSet<>();
        // add to graph
        allDescriptors.forEach(queryDescriptor -> addSqlDescriptorToGraph(queryDescriptor, graph, accioMDL, requiredQueryDescriptors, sessionContext));

        Map<String, QueryDescriptor> descriptorMap = new HashMap<>();
        allDescriptors.forEach(queryDescriptor -> descriptorMap.put(queryDescriptor.getName(), queryDescriptor));
        requiredQueryDescriptors.forEach(queryDescriptor -> descriptorMap.put(queryDescriptor.getName(), queryDescriptor));

        List<WithQuery> withQueries = new ArrayList<>();
        graph.iterator().forEachRemaining(objectName -> {
            QueryDescriptor queryDescriptor = descriptorMap.get(objectName);
            checkArgument(queryDescriptor != null, objectName + " not found in query descriptors");
            withQueries.add(getWithQuery(queryDescriptor));
        });

        Node rewriteWith = new WithRewriter(withQueries).process(root);
        return (Statement) new Rewriter(accioMDL, analysis).process(rewriteWith);
    }

    private static void addSqlDescriptorToGraph(
            QueryDescriptor queryDescriptor,
            DirectedAcyclicGraph<String, Object> graph,
            AccioMDL mdl,
            Set<QueryDescriptor> queryDescriptors,
            SessionContext sessionContext)
    {
        // add vertex
        graph.addVertex(queryDescriptor.getName());
        queryDescriptor.getRequiredObjects().forEach(graph::addVertex);

        //add edge
        try {
            queryDescriptor.getRequiredObjects().forEach(modelName ->
                    graph.addEdge(modelName, queryDescriptor.getName()));
        }
        catch (GraphCycleProhibitedException ex) {
            throw new IllegalArgumentException("found cycle in models", ex);
        }

        // add required models to graph
        for (String objectName : queryDescriptor.getRequiredObjects()) {
            QueryDescriptor descriptor = QueryDescriptor.of(objectName, mdl, sessionContext);
            queryDescriptors.add(descriptor);
            addSqlDescriptorToGraph(descriptor, graph, mdl, queryDescriptors, sessionContext);
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

        // the model is added in with query, and the catalog and schema should be removed
        private Node applyModelRule(Table table)
        {
            return new Table(QualifiedName.of(table.getName().getSuffix()));
        }
    }

    private static WithQuery getWithQuery(QueryDescriptor queryDescriptor)
    {
        return new WithQuery(new Identifier(queryDescriptor.getName()), queryDescriptor.getQuery(), Optional.empty());
    }
}
