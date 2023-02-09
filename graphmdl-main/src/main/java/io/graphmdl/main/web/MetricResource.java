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

import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.graphmdl.main.metrics.Metric;
import io.graphmdl.main.metrics.MetricHook;
import io.graphmdl.main.metrics.MetricStore;
import io.graphmdl.main.web.dto.MetricDto;
import io.graphmdl.spi.GraphMDLException;

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

import java.util.List;
import java.util.concurrent.Callable;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.jaxrs.AsyncResponseHandler.bindAsyncResponse;
import static io.graphmdl.spi.metadata.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
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
        ListenableFuture<List<MetricDto>> future = FluentFuture.from(createDirectListenableFuture(metricStore::listMetrics))
                .transform(metrics -> metrics.stream()
                        .map(metric -> MetricDto.from(metric, metricStore.listMetricSqls(metric.getName()))).collect(toList()), directExecutor());
        bindAsyncResponse(asyncResponse, future, directExecutor());
    }

    @GET
    @Path("/{metricName}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public void getOneMetric(
            @PathParam("metricName") String metricName,
            @Suspended AsyncResponse asyncResponse)
    {
        ListenableFuture<MetricDto> future = FluentFuture.from(createDirectListenableFuture(() -> metricStore.getMetric(metricName)))
                .transform(metricOptional -> MetricDto.from(
                        metricOptional.orElseThrow(() -> new GraphMDLException(NOT_FOUND, format("metric %s is not found", metricName))),
                        metricStore.listMetricSqls(metricName)), directExecutor());
        bindAsyncResponse(asyncResponse, future, directExecutor());
    }

    @DELETE
    @Path("/{metricName}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public void deleteOneMetric(
            @PathParam("metricName") String metricName,
            @Suspended AsyncResponse asyncResponse)
    {
        bindAsyncResponse(
                asyncResponse,
                runDirect(() -> metricHook.handleDrop(metricName)),
                directExecutor());
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
        bindAsyncResponse(
                asyncResponse,
                runDirect(() -> metricHook.handleUpdate(metric)),
                directExecutor());
    }

    @POST
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public void createMetric(
            Metric metric,
            @Suspended AsyncResponse asyncResponse)
    {
        bindAsyncResponse(
                asyncResponse,
                runDirect(() -> metricHook.handleCreate(metric)),
                directExecutor());
    }

    private <O> ListenableFuture<O> createDirectListenableFuture(Callable<O> callable)
    {
        return Futures.submit(callable, directExecutor());
    }

    private ListenableFuture<Void> runDirect(Runnable runnable)
    {
        return FluentFuture.from(Futures.submit(runnable, directExecutor()));
    }
}
