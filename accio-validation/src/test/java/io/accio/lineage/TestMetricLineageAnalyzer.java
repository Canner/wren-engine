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

package io.accio.lineage;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.accio.base.AccioMDL;
import io.accio.base.dto.AccioObject;
import io.accio.base.dto.Model;
import io.accio.testing.AbstractTestFramework;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.DepthFirstIterator;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestMetricLineageAnalyzer
        extends AbstractTestFramework
{
    private final AccioMDL accioMDL;

    public TestMetricLineageAnalyzer()
            throws IOException
    {
        accioMDL = AccioMDL.fromJson(Files.readString(Path.of(requireNonNull(getClass().getClassLoader().getResource("tpch_mdl.json")).getPath())));
    }

    @Test
    public void testMetricLineageAnalyzer()
    {
        Graph<AccioObject, DefaultEdge> graph = MetricLineageAnalyzer.analyze(accioMDL);
        assertRelatedObject("Orders", ImmutableSet.of("Orders", "Revenue", "RevenueByNation"), graph);
        assertRelatedObject("Customer", ImmutableSet.of("Customer", "ConsumptionCustomer", "RevenueByNation"), graph);
        assertRelatedObject("Nation", ImmutableSet.of("Nation", "ConsumptionCustomer", "RevenueByNation"), graph);
    }

    private void assertRelatedObject(String startName, Set<String> expectedNames, Graph<AccioObject, DefaultEdge> graph)
    {
        Model model = accioMDL.getModel(startName)
                .orElseThrow(() -> new RuntimeException("Orders model not found"));
        Iterable<AccioObject> ordersRelatedIterable = () -> new DepthFirstIterator<>(graph, model);
        Set<String> modelRelated = Streams.stream(ordersRelatedIterable).map(AccioObject::getName).collect(Collectors.toSet());
        assertThat(modelRelated).isEqualTo(expectedNames);
    }
}
