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

import io.accio.base.AccioException;
import io.accio.base.CatalogSchemaTableName;
import io.accio.base.metadata.SchemaTableName;
import io.accio.cache.CacheManager;
import io.accio.main.AccioManager;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

import static io.accio.base.metadata.StandardErrorCode.NOT_FOUND;
import static io.accio.main.web.AccioExceptionMapper.bindAsyncResponse;
import static java.util.Objects.requireNonNull;

@Path("/v1/cache")
public class CacheResource
{
    private final CacheManager cacheManager;
    private final AccioManager accioManager;

    @Inject
    public CacheResource(CacheManager cacheManager, AccioManager accioManager)
    {
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
        this.accioManager = requireNonNull(accioManager, "accioManager is null");
    }

    @Deprecated
    @PUT
    @Path("reload")
    public void reload(@Suspended AsyncResponse asyncResponse)
    {
        cacheManager.createTaskUtilDone(accioManager.getAccioMDL());
        asyncResponse.resume(Response.ok().build());
    }

    @POST
    @Path("reload/async")
    public void reloadAsync(@Suspended AsyncResponse asyncResponse)
    {
        cacheManager
                .createTask(accioManager.getAccioMDL())
                .whenComplete(bindAsyncResponse(asyncResponse));
    }

    @GET
    @Path("info/{catalogName}/{schemaName}/{tableName}")
    public void getTaskInfo(
            @PathParam("catalogName") String catalogName,
            @PathParam("schemaName") String schemaName,
            @PathParam("tableName") String tableName,
            @Suspended AsyncResponse asyncResponse)
    {
        CatalogSchemaTableName catalogSchemaTableName = new CatalogSchemaTableName(catalogName, new SchemaTableName(schemaName, tableName));
        cacheManager
                .getTaskInfo(catalogSchemaTableName)
                .thenApply(v -> v.orElseThrow(() -> new AccioException(NOT_FOUND, String.format("Task %s not found.", catalogSchemaTableName))))
                .whenComplete(bindAsyncResponse(asyncResponse));
    }

    @GET
    @Path("info/{catalogName}/{schemaName}")
    public void getTaskInfo(
            @PathParam("catalogName") String catalogName,
            @PathParam("schemaName") String schemaName,
            @Suspended AsyncResponse asyncResponse)
    {
        cacheManager
                .listTaskInfo(catalogName, schemaName)
                .whenComplete(bindAsyncResponse(asyncResponse));
    }
}
