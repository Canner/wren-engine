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

package io.cml.graphml;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.airlift.log.Logger;
import io.cml.graphml.base.GraphML;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicReference;

import static io.cml.graphml.base.GraphML.EMPTY_GRAPHML;
import static java.util.Objects.requireNonNull;

public class GraphMLManager
        implements GraphMLMetastore
{
    private static final Logger LOG = Logger.get(GraphMLManager.class);
    private final AtomicReference<GraphML> graphML = new AtomicReference<>(EMPTY_GRAPHML);
    private final File graphMLFile;

    @Inject
    public GraphMLManager(GraphMLConfig graphMLConfig)
            throws IOException
    {
        this.graphMLFile = requireNonNull(graphMLConfig.getGraphMLFile(), "graphMLFile is null");
        if (graphMLFile.exists()) {
            loadGraphMLFromFile();
        }
        else {
            LOG.warn("GraphML file %s does not exist", graphMLFile);
        }
    }

    public void loadGraphMLFromFile()
            throws IOException
    {
        loadGraphML(Files.readString(graphMLFile.toPath()));
    }

    private void loadGraphML(String json)
            throws JsonProcessingException
    {
        graphML.set(GraphML.fromJson(json));
    }

    @Override
    public GraphML getGraphML()
    {
        return graphML.get();
    }
}
