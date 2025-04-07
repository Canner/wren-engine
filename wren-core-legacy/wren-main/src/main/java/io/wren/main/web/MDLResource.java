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

import com.google.inject.Inject;
import io.wren.base.AnalyzedMDL;
import io.wren.base.WrenMDL;
import io.wren.main.PreviewService;
import io.wren.main.ValidationService;
import io.wren.main.web.dto.DryPlanDto;
import io.wren.main.web.dto.PreviewDto;
import io.wren.main.web.dto.ValidateDto;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;

import java.util.Map;
import java.util.Optional;

import static io.wren.main.web.WrenExceptionMapper.bindAsyncResponse;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

@Path("/v1/mdl")
public class MDLResource
{
    private final PreviewService previewService;
    private final ValidationService validationService;

    @Inject
    public MDLResource(
            PreviewService previewService,
            ValidationService validationService)
    {
        this.previewService = requireNonNull(previewService, "previewService is null");
        this.validationService = requireNonNull(validationService, "validationService is null");
    }

    @GET
    @Path("/preview")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public void preview(
            PreviewDto previewDto,
            @Suspended AsyncResponse asyncResponse)
    {
        if (previewDto.getManifest() == null) {
            asyncResponse.resume(new IllegalArgumentException("Manifest is required"));
        }
        previewService.preview(
                        WrenMDL.fromManifest(previewDto.getManifest()),
                        previewDto.getSql(),
                        Optional.ofNullable(previewDto.getLimit()).orElse(100L))
                .whenComplete(bindAsyncResponse(asyncResponse));
    }

    @GET
    @Path("/dry-plan")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public void dryPlan(
            DryPlanDto dryPlanDto,
            @Suspended AsyncResponse asyncResponse)
    {
        if (dryPlanDto.getManifest() == null) {
            asyncResponse.resume(new IllegalArgumentException("Manifest is required"));
        }
        previewService.dryPlan(WrenMDL.fromManifest(dryPlanDto.getManifest()), dryPlanDto.getSql(), dryPlanDto.isModelingOnly())
                .whenComplete(bindAsyncResponse(asyncResponse));
    }

    @GET
    @Path("/dry-run")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public void dryRun(
            PreviewDto previewDto,
            @Suspended AsyncResponse asyncResponse)
    {
        if (previewDto.getManifest() == null) {
            asyncResponse.resume(new IllegalArgumentException("Manifest is required"));
        }
        previewService.dryRun(WrenMDL.fromManifest(previewDto.getManifest()), previewDto.getSql())
                .whenComplete(bindAsyncResponse(asyncResponse));
    }

    @POST
    @Path("/validate/{ruleName}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public void validate(
            @PathParam("ruleName") String ruleName,
            ValidateDto validateDto,
            @Suspended AsyncResponse asyncResponse)
    {
        if (validateDto == null || validateDto.getManifest() == null) {
            asyncResponse.resume(new IllegalArgumentException("Manifest is required"));
        }
        else {
            Map<String, Object> parameters = Map.of();
            parameters = validateDto.getParameters() != null ? validateDto.getParameters() : parameters;
            AnalyzedMDL analyzedMDL = new AnalyzedMDL(WrenMDL.fromManifest(validateDto.getManifest()), null);
            validationService.validate(ruleName, parameters, analyzedMDL)
                    .whenComplete(bindAsyncResponse(asyncResponse));
        }
    }
}
