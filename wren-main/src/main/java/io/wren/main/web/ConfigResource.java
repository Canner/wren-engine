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
import io.wren.base.config.ConfigManager;
import io.wren.base.sql.SqlConverter;
import io.wren.main.metadata.Metadata;
import io.wren.main.sql.SqlConverterManager;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

@Path("/v1/config")
public class ConfigResource
{
    private final ConfigManager configManager;
    private final SqlConverterManager sqlConverter;

    @Inject
    public ConfigResource(
            ConfigManager configManager,
            Metadata metadata,
            SqlConverter sqlConverter)

    {
        this.configManager = requireNonNull(configManager, "configManager is null");
        this.sqlConverter = (SqlConverterManager) requireNonNull(sqlConverter, "sqlConverter is null");
    }

    @GET
    @Produces("application/json")
    public void getConfigs(@Suspended AsyncResponse asyncResponse)
    {
        CompletableFuture
                .supplyAsync(configManager::getConfigs)
                .whenComplete(WrenExceptionMapper.bindAsyncResponse(asyncResponse));
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
                .whenComplete(WrenExceptionMapper.bindAsyncResponse(asyncResponse));
    }

    @DELETE
    @Produces("application/json")
    public void resetToDefaultConfig(@Suspended AsyncResponse asyncResponse)
    {
        CompletableFuture
                .runAsync(() -> configManager.setConfigs(List.of(), true))
                .whenComplete(WrenExceptionMapper.bindAsyncResponse(asyncResponse));
    }

    @PATCH
    @Produces("application/json")
    public void patchConfig(
            List<ConfigManager.ConfigEntry> configEntries,
            @Suspended AsyncResponse asyncResponse)
    {
        CompletableFuture
                .runAsync(() -> {
                    if (configManager.setConfigs(configEntries, false)) {
                        reloadConfig();
                    }
                })
                .whenComplete(WrenExceptionMapper.bindAsyncResponse(asyncResponse));
    }

    private void reloadConfig()
    {
        sqlConverter.reload();
    }
}
