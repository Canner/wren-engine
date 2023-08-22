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

package io.accio.cli;

import io.accio.base.AccioMDL;
import io.accio.base.dto.AccioObject;
import io.accio.base.dto.Metric;
import io.accio.lineage.MetricLineageAnalyzer;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.DepthFirstIterator;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import static java.lang.String.format;

@CommandLine.Command(name = "metric-lineage", mixinStandardHelpOptions = true, version = "accio-0.1.0",
        description = "Analyze metric lineage")
public class MetricLineage
        implements Callable<Integer>
{
    @CommandLine.Option(names = "-i", required = true, description = "The path of Accio MDL file")
    private File accioMDLFile;

    @CommandLine.Option(names = "-m", description = "The name of the specific model. If not specified, all models will be analyzed")
    private String targetModelName;

    public boolean run()
            throws IOException
    {
        AccioMDL accioMDL = importAccioMDL(accioMDLFile.toPath());
        Graph<AccioObject, DefaultEdge> graph = MetricLineageAnalyzer.analyze(accioMDL);
        if (targetModelName != null) {
            AccioObject targetModel = accioMDL.getModel(targetModelName).orElseThrow(() -> new RuntimeException(format("Model %s not found", targetModelName)));
            new DepthFirstIterator<>(graph, targetModel).forEachRemaining(this::printAccioObject);
        }
        else {
            accioMDL.listModels().forEach(model ->
                    new DepthFirstIterator<>(graph, model).forEachRemaining(this::printAccioObject));
        }
        return true;
    }

    private void printAccioObject(AccioObject accioObject)
    {
        if (accioObject instanceof Metric) {
            System.out.println("  " + accioObject.getName());
        }
        else {
            System.out.println("Model: " + accioObject.getName());
        }
    }

    @Override
    public Integer call()
            throws Exception
    {
        return run() ? 0 : 1;
    }

    private static AccioMDL importAccioMDL(Path path)
            throws IOException
    {
        return AccioMDL.fromJson(Files.readString(path));
    }
}
