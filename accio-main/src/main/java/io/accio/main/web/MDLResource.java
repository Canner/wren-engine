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

package io.accio.main.web;

import io.accio.base.AccioMDL;
import io.accio.base.dto.Manifest;
import io.accio.main.AccioManager;
import io.accio.main.PreviewService;
import io.accio.main.web.dto.PreviewDto;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.accio.main.web.AccioExceptionMapper.bindAsyncResponse;
import static io.accio.main.web.dto.CheckOutputDto.prepare;
import static io.accio.main.web.dto.CheckOutputDto.ready;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/v1/mdl")
public class MDLResource
{
    private final AccioManager accioManager;
    private final PreviewService previewService;

    @Inject
    public MDLResource(AccioManager accioManager,
            PreviewService previewService)
    {
        this.accioManager = requireNonNull(accioManager, "accioManager is null");
        this.previewService = requireNonNull(previewService, "previewService is null");
    }

    @POST
    @Path("/deploy")
    @Consumes(APPLICATION_JSON)
    public void deploy(
            Manifest manifest,
            @Suspended AsyncResponse asyncResponse)
    {
        CompletableFuture
                .runAsync(() -> accioManager.deployAndArchive(manifest));
        asyncResponse.resume(Response.accepted().build());
    }

    @GET
    @Path("/status")
    @Produces(APPLICATION_JSON)
    public void status(@Suspended AsyncResponse asyncResponse)
    {
        CompletableFuture
                .supplyAsync(() -> {
                    if (accioManager.checkStatus()) {
                        return ready(accioManager.getAccioMDL().getManifest());
                    }
                    return prepare(accioManager.getAccioMDL().getManifest());
                })
                .whenComplete(bindAsyncResponse(asyncResponse));
    }

    @GET
    @Path("/preview")
    @Consumes(APPLICATION_JSON)
    public void preview(
            PreviewDto previewDto,
            @Suspended AsyncResponse asyncResponse)
    {
        previewService.preview(
                        AccioMDL.fromManifest(previewDto.getManifest()),
                        previewDto.getSql(),
                        Optional.ofNullable(previewDto.getLimit()).orElse(100L))
                .whenComplete(bindAsyncResponse(asyncResponse));
    }
}
