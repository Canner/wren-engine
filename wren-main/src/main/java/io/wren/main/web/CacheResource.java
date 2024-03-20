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

import io.wren.base.CatalogSchemaTableName;
import io.wren.base.WrenException;
import io.wren.base.metadata.SchemaTableName;
import io.wren.cache.CacheManager;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

import static io.wren.base.metadata.StandardErrorCode.NOT_FOUND;
import static io.wren.main.web.WrenExceptionMapper.bindAsyncResponse;
import static java.util.Objects.requireNonNull;

@Path("/v1/cache")
public class CacheResource
{
    private final CacheManager cacheManager;

    @Inject
    public CacheResource(CacheManager cacheManager)
    {
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
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
                .thenApply(v -> v.orElseThrow(() -> new WrenException(NOT_FOUND, String.format("Task %s not found.", catalogSchemaTableName))))
                .whenComplete(bindAsyncResponse(asyncResponse));
    }

    @GET
    @Path("info/{catalogName}/{schemaName}")
    public void getTaskInfos(
            @PathParam("catalogName") String catalogName,
            @PathParam("schemaName") String schemaName,
            @Suspended AsyncResponse asyncResponse)
    {
        cacheManager
                .listTaskInfo(catalogName, schemaName)
                .whenComplete(bindAsyncResponse(asyncResponse));
    }

    @GET
    @Path("duckdb/settings")
    public void getDuckDBSettings(@Suspended AsyncResponse asyncResponse)
    {
        asyncResponse.resume(Response.ok(cacheManager.getDuckDBSettings()).build());
    }
}
