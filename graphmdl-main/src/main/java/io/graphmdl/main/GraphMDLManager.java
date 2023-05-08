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

package io.graphmdl.main;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.airlift.log.Logger;
import io.graphmdl.base.GraphMDL;
import io.graphmdl.preaggregation.PreAggregationManager;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicReference;

import static io.graphmdl.base.GraphMDL.EMPTY_GRAPHMDL;
import static java.util.Objects.requireNonNull;

public class GraphMDLManager
        implements GraphMDLMetastore
{
    private static final Logger LOG = Logger.get(GraphMDLManager.class);
    private final AtomicReference<GraphMDL> graphMDL = new AtomicReference<>(EMPTY_GRAPHMDL);
    private final File graphMDLFile;
    private final PreAggregationManager preAggregationManager;

    @Inject
    public GraphMDLManager(GraphMDLConfig graphMDLConfig, PreAggregationManager preAggregationManager)
            throws IOException
    {
        this.graphMDLFile = requireNonNull(graphMDLConfig.getGraphMDLFile(), "graphMDLFile is null");
        this.preAggregationManager = requireNonNull(preAggregationManager, "preAggregationManager is null");
        if (graphMDLFile.exists()) {
            loadGraphMDLFromFile();
        }
        else {
            LOG.warn("GraphMDL file %s does not exist", graphMDLFile);
        }
    }

    public void loadGraphMDLFromFile()
            throws IOException
    {
        loadGraphMDL(Files.readString(graphMDLFile.toPath()));
        preAggregationManager.doPreAggregation(getGraphMDL()).join();
    }

    private void loadGraphMDL(String json)
            throws JsonProcessingException
    {
        graphMDL.set(GraphMDL.fromJson(json));
    }

    @Override
    public GraphMDL getGraphMDL()
    {
        return graphMDL.get();
    }
}
