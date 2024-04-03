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
import io.wren.base.WrenMDL;
import io.wren.main.PreviewService;
import io.wren.main.WrenManager;
import io.wren.main.web.dto.CheckOutputDto;
import io.wren.main.web.dto.DeployInputDto;
import io.wren.main.web.dto.PreviewDto;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Response;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.wren.main.web.WrenExceptionMapper.bindAsyncResponse;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

@Path("/v1/mdl")
public class MDLResource
{
    private final WrenManager wrenManager;
    private final PreviewService previewService;

    @Inject
    public MDLResource(WrenManager wrenManager,
            PreviewService previewService)
    {
        this.wrenManager = requireNonNull(wrenManager, "wrenManager is null");
        this.previewService = requireNonNull(previewService, "previewService is null");
    }

    @POST
    @Path("/deploy")
    @Consumes(APPLICATION_JSON)
    public void deploy(
            DeployInputDto deployInputDto,
            @Suspended AsyncResponse asyncResponse)
    {
        wrenManager.deployAndArchive(deployInputDto.getManifest(), deployInputDto.getVersion());
        asyncResponse.resume(Response.accepted().build());
    }

    @GET
    @Produces(APPLICATION_JSON)
    public void getCurrentManifest(@Suspended AsyncResponse asyncResponse)
    {
        CompletableFuture
                .supplyAsync(() -> wrenManager.getAnalyzedMDL().getWrenMDL().getManifest())
                .whenComplete(bindAsyncResponse(asyncResponse));
    }

    @GET
    @Path("/status")
    @Produces(APPLICATION_JSON)
    public void status(@Suspended AsyncResponse asyncResponse)
    {
        CompletableFuture
                .supplyAsync(() -> {
                    if (wrenManager.checkStatus()) {
                        return CheckOutputDto.ready(wrenManager.getAnalyzedMDL().getVersion());
                    }
                    return CheckOutputDto.prepare(wrenManager.getAnalyzedMDL().getVersion());
                })
                .whenComplete(bindAsyncResponse(asyncResponse));
    }

    @GET
    @Path("/preview")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public void preview(
            PreviewDto previewDto,
            @Suspended AsyncResponse asyncResponse)
    {
        WrenMDL mdl;
        if (previewDto.getManifest() == null) {
            mdl = wrenManager.getAnalyzedMDL().getWrenMDL();
        }
        else {
            mdl = WrenMDL.fromManifest(previewDto.getManifest());
        }
        previewService.preview(
                        mdl,
                        previewDto.getSql(),
                        Optional.ofNullable(previewDto.getLimit()).orElse(100L))
                .whenComplete(bindAsyncResponse(asyncResponse));
    }
}
