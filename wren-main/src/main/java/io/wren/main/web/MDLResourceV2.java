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

package io.wren.main.web;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.inject.Inject;
import io.wren.base.WrenMDL;
import io.wren.main.PreviewService;
import io.wren.main.web.dto.DryPlanDtoV2;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Response;

import static io.wren.main.web.WrenExceptionMapper.bindAsyncResponse;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static java.util.Objects.requireNonNull;

@Path("/v2/mdl")
public class MDLResourceV2
{
    private final PreviewService previewService;

    @Inject
    public MDLResourceV2(PreviewService previewService)
    {
        this.previewService = requireNonNull(previewService, "previewService is null");
    }

    @GET
    @Path("/dry-plan")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public void dryPlan(
            DryPlanDtoV2 dryPlanDto,
            @Suspended AsyncResponse asyncResponse)
            throws JsonProcessingException
    {
        if (dryPlanDto.getManifestStr() == null) {
            asyncResponse.resume(Response
                    .status(BAD_REQUEST)
                    .entity("Manifest is required")
                    .build());
        }
        WrenMDL mdl = WrenMDL.fromJson(dryPlanDto.getManifestStr());
        previewService.dryPlan(mdl, dryPlanDto.getSql(), true)
                .whenComplete(bindAsyncResponse(asyncResponse));
    }
}
