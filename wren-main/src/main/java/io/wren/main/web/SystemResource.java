package io.wren.main.web;

import com.google.inject.Inject;
import io.wren.main.PreviewService;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Response;

@Path("/v2")
public class SystemResource
{
    private final PreviewService previewService;

    @Inject
    public SystemResource(PreviewService previewService)
    {
        this.previewService = previewService;
    }

    @GET
    @Path("/health")
    public void health(@Suspended AsyncResponse asyncResponse)
    {
        if (previewService.isWarmed()) {
            asyncResponse.resume(Response.ok().build());
        }
        else {
            asyncResponse.resume(Response.status(Response.Status.SERVICE_UNAVAILABLE).build());
        }
    }
}
