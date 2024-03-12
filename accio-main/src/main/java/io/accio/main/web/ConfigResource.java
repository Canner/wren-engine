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
import io.accio.base.sql.SqlConverter;
import io.accio.cache.CacheService;
import io.accio.main.connector.CacheServiceManager;
import io.accio.main.metadata.Metadata;
import io.accio.main.metadata.MetadataManager;
import io.accio.main.sql.SqlConverterManager;

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
    private final MetadataManager metadata;
    private final SqlConverterManager sqlConverter;
    private final CacheServiceManager cacheService;

    @Inject
    public ConfigResource(
            ConfigManager configManager,
            Metadata metadata,
            SqlConverter sqlConverter,
            CacheService cacheService)

    {
        this.configManager = requireNonNull(configManager, "configManager is null");
        this.metadata = (MetadataManager) requireNonNull(metadata, "metadata is null");
        this.sqlConverter = (SqlConverterManager) requireNonNull(sqlConverter, "sqlConverter is null");
        this.cacheService = (CacheServiceManager) requireNonNull(cacheService, "cacheService is null");
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
                .runAsync(() -> {
                    if (configManager.setConfigs(configEntries, false)) {
                        reloadConfig();
                    }
                })
                .whenComplete(bindAsyncResponse(asyncResponse));
    }

    private void reloadConfig()
    {
        metadata.reload();
        sqlConverter.reload();
        cacheService.reload();
    }
}
