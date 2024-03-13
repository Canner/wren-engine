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

import io.accio.base.config.ConfigManager;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PATCH;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.accio.main.web.AccioExceptionMapper.bindAsyncResponse;
import static java.util.Objects.requireNonNull;

@Path("/v1/config")
public class ConfigResource
{
    private final ConfigManager configManager;

    @Inject
    public ConfigResource(ConfigManager configManager)
    {
        this.configManager = requireNonNull(configManager, "configManager is null");
    }

    @GET
    @Produces("application/json")
    public void getConfigs(@Suspended AsyncResponse asyncResponse)
    {
        CompletableFuture
                .supplyAsync(configManager::getConfigs)
                .whenComplete(bindAsyncResponse(asyncResponse));
    }

    @GET
    @Path("/{configName}")
    @Produces("application/json")
    public void getOneConfig(
            @PathParam("configName") String configName,
            @Suspended AsyncResponse asyncResponse)
    {
        CompletableFuture
                .supplyAsync(() -> configManager.getConfig(configName))
                .whenComplete(bindAsyncResponse(asyncResponse));
    }

    @DELETE
    @Produces("application/json")
    public void resetToDefaultConfig(@Suspended AsyncResponse asyncResponse)
    {
        CompletableFuture
                .runAsync(() -> configManager.setConfigs(List.of(), true))
                .whenComplete(bindAsyncResponse(asyncResponse));
    }

    @PATCH
    @Produces("application/json")
    public void patchConfig(
            List<ConfigManager.ConfigEntry> configEntries,
            @Suspended AsyncResponse asyncResponse)
    {
        CompletableFuture
                .runAsync(() -> configManager.setConfigs(configEntries, false))
                .whenComplete(bindAsyncResponse(asyncResponse));
    }
}
