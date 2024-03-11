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

package io.accio.cache;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.accio.base.client.ForCache;
import io.accio.base.client.duckdb.CacheStorageConfig;
import io.accio.base.client.duckdb.DuckDBConfig;
import io.accio.base.client.duckdb.DuckDBConnectorConfig;
import io.accio.base.client.duckdb.DuckdbCacheStorageConfig;
import io.accio.base.client.duckdb.DuckdbClient;
import io.accio.base.client.duckdb.DuckdbS3StyleStorageConfig;
import io.accio.base.config.AccioConfig;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class CacheModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(DuckdbS3StyleStorageConfig.class);
        configBinder(binder).bindConfig(DuckDBConfig.class);
        binder.bind(CacheManager.class).in(Scopes.SINGLETON);
        binder.bind(DuckdbTaskManager.class).in(Scopes.SINGLETON);
        binder.bind(EventLogger.class).to(Log4jEventLogger.class).in(Scopes.SINGLETON);
        binder.bind(DuckdbClient.class).annotatedWith(ForCache.class).to(DuckdbClient.class).in(Scopes.SINGLETON);
        binder.bind(CachedTableMapping.class).to(DefaultCachedTableMapping.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, DuckDBConnectorConfig.class);
    }

    @Provides
    @Singleton
    public static CacheStorageConfig provideCacheStorageConfig(AccioConfig config, DuckdbS3StyleStorageConfig duckdbS3StyleStorageConfig)
    {
        if (config.getDataSourceType() == AccioConfig.DataSourceType.DUCKDB) {
            return new DuckdbCacheStorageConfig();
        }
        else {
            return duckdbS3StyleStorageConfig;
        }
    }
}
