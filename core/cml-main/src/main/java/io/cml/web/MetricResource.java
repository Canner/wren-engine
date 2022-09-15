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

package io.cml.web;

import io.cml.metrics.Metric;
import io.cml.metrics.MetricHook;
import io.cml.metrics.MetricStore;
import io.cml.spi.CmlException;
import io.cml.web.dto.MetricDto;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;

import java.util.concurrent.CompletableFuture;

import static io.cml.spi.metadata.StandardErrorCode.NOT_FOUND;
import static io.cml.web.CompletableFutureAsyncResponseHandler.bindAsyncResponse;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/v1/metric")
public class MetricResource
{
    private final MetricHook metricHook;
    private final MetricStore metricStore;

    @Inject
    public MetricResource(
            MetricHook metricHook,
            MetricStore metricStore)
    {
        this.metricHook = requireNonNull(metricHook, "metricHook is null");
        this.metricStore = requireNonNull(metricStore, "metricStore is null");
    }

    @GET
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public void getMetrics(@Suspended AsyncResponse asyncResponse)
    {
        CompletableFuture
                .supplyAsync(metricStore::listMetrics)
                .thenApply(metrics -> metrics.stream().map(metric -> MetricDto.from(metric, metricStore.listMetricSqls(metric.getName()))))
                .whenComplete(bindAsyncResponse(asyncResponse));
    }

    @GET
    @Path("/{metricName}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public void getOneMetric(
            @PathParam("metricName") String metricName,
            @Suspended AsyncResponse asyncResponse)
    {
        CompletableFuture
                .supplyAsync(() -> metricStore.getMetric(metricName))
                .thenApply(metricOptional -> MetricDto.from(
                        metricOptional.orElseThrow(() -> new CmlException(NOT_FOUND, format("metric %s is not found", metricName))),
                        metricStore.listMetricSqls(metricName)))
                .whenComplete(bindAsyncResponse(asyncResponse));
    }

    @DELETE
    @Path("/{metricName}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public void deleteOneMetric(
            @PathParam("metricName") String metricName,
            @Suspended AsyncResponse asyncResponse)
    {
        CompletableFuture
                .runAsync(() -> metricHook.handleDrop(metricName))
                .whenComplete(bindAsyncResponse(asyncResponse));
    }

    @PUT
    @Path("/{metricName}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public void updateMetric(
            @PathParam("metricName") String metricName,
            Metric metric,
            @Suspended AsyncResponse asyncResponse)
    {
        CompletableFuture
                .runAsync(() -> metricHook.handleUpdate(metric))
                .whenComplete(bindAsyncResponse(asyncResponse));
    }

    @POST
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public void createMetric(
            Metric metric,
            @Suspended AsyncResponse asyncResponse)
    {
        CompletableFuture
                .runAsync(() -> metricHook.handleCreate(metric))
                .whenComplete(bindAsyncResponse(asyncResponse));
    }
}
