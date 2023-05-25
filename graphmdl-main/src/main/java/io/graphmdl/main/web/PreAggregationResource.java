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
package io.graphmdl.main.web;

import io.graphmdl.main.GraphMDLManager;
import io.graphmdl.preaggregation.PreAggregationManager;

import javax.inject.Inject;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

import static java.util.Objects.requireNonNull;

@Path("/v1/preAggregation")
public class PreAggregationResource
{
    private final PreAggregationManager preAggregationManager;
    private final GraphMDLManager graphMDLManager;

    @Inject
    public PreAggregationResource(PreAggregationManager preAggregationManager, GraphMDLManager graphMDLManager)
    {
        this.preAggregationManager = requireNonNull(preAggregationManager, "preAggregationManager is null");
        this.graphMDLManager = requireNonNull(graphMDLManager, "graphMDLManager is null");
    }

    @PUT
    @Path("reload")
    public void reload(@Suspended AsyncResponse asyncResponse)
    {
        preAggregationManager.importPreAggregation(graphMDLManager.getGraphMDL());
        asyncResponse.resume(Response.ok().build());
    }
}
