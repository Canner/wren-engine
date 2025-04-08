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

package io.wren.base.sqlrewrite;

import io.trino.sql.tree.Statement;
import io.trino.sql.tree.WithQuery;
import io.wren.base.AnalyzedMDL;
import io.wren.base.SessionContext;
import io.wren.base.Utils;
import io.wren.base.sqlrewrite.analyzer.Analysis;
import io.wren.base.sqlrewrite.analyzer.StatementAnalyzer;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.graph.GraphCycleProhibitedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.wren.base.sqlrewrite.WithRewriter.getWithQuery;
import static java.util.stream.Collectors.toSet;

public class GenerateViewRewrite
        implements WrenRule
{
    public static final GenerateViewRewrite GENERATE_VIEW_REWRITE = new GenerateViewRewrite();

    @Override
    public Statement apply(Statement root, SessionContext sessionContext, AnalyzedMDL analyzedMDL)
    {
        Analysis analysis = new Analysis(root);
        StatementAnalyzer.analyze(analysis, root, sessionContext, analyzedMDL.getWrenMDL());
        return apply(root, sessionContext, analysis, analyzedMDL);
    }

    @Override
    public Statement apply(Statement root, SessionContext sessionContext, Analysis analysis, AnalyzedMDL analyzedMDL)
    {
        Set<QueryDescriptor> viewDescriptors = analysis.getViews().stream().map(view -> ViewInfo.get(view, analyzedMDL, sessionContext)).collect(toSet());
        DirectedAcyclicGraph<String, Object> graph = new DirectedAcyclicGraph<>(Object.class);
        Set<QueryDescriptor> requiredQueryDescriptors = new HashSet<>();
        viewDescriptors.forEach(viewDescriptor -> addSqlDescriptorToGraph(viewDescriptor, graph, analyzedMDL, requiredQueryDescriptors, sessionContext));

        Map<String, QueryDescriptor> descriptorMap = new HashMap<>();
        viewDescriptors.forEach(queryDescriptor -> descriptorMap.put(queryDescriptor.getName(), queryDescriptor));
        requiredQueryDescriptors.forEach(queryDescriptor -> descriptorMap.put(queryDescriptor.getName(), queryDescriptor));

        List<WithQuery> withQueries = new ArrayList<>();
        graph.iterator().forEachRemaining(objectName -> {
            QueryDescriptor queryDescriptor = descriptorMap.get(objectName);
            Utils.checkArgument(queryDescriptor != null, objectName + " not found in query descriptors");
            withQueries.add(getWithQuery(queryDescriptor));
        });

        return (Statement) new WithRewriter(withQueries).process(root);
    }

    private static void addSqlDescriptorToGraph(
            QueryDescriptor queryDescriptor,
            DirectedAcyclicGraph<String, Object> graph,
            AnalyzedMDL analyzedMDL,
            Set<QueryDescriptor> queryDescriptors,
            SessionContext sessionContext)
    {
        // add vertex
        graph.addVertex(queryDescriptor.getName());
        Set<String> requiredViews = queryDescriptor.getRequiredObjects().stream()
                .filter(name -> analyzedMDL.getWrenMDL().getView(name).isPresent()).collect(toSet());
        requiredViews.forEach(graph::addVertex);

        //add edge
        try {
            requiredViews.forEach(name ->
                    graph.addEdge(name, queryDescriptor.getName()));
        }
        catch (GraphCycleProhibitedException ex) {
            throw new IllegalArgumentException("found cycle in view", ex);
        }
        catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("found issue in view", ex);
        }

        // add required view to graph
        requiredViews.forEach(name -> {
            ViewInfo descriptor = (ViewInfo) QueryDescriptor.of(name, analyzedMDL, sessionContext);
            queryDescriptors.add(descriptor);
            addSqlDescriptorToGraph(descriptor, graph, analyzedMDL, queryDescriptors, sessionContext);
        });
    }
}
