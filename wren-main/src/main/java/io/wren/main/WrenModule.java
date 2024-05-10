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

package io.wren.main;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.wren.base.config.PostgresWireProtocolConfig;
import io.wren.base.config.WrenConfig;
import io.wren.cache.CacheManager;
import io.wren.cache.CacheManagerImpl;
import io.wren.cache.NoOpCacheManager;
import io.wren.main.pgcatalog.NoOpPgCatalogManager;
import io.wren.main.pgcatalog.PgCatalogManager;
import io.wren.main.pgcatalog.PgCatalogManagerImpl;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class WrenModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        PostgresWireProtocolConfig config = buildConfigObject(PostgresWireProtocolConfig.class);
        configBinder(binder).bindConfig(WrenConfig.class);
        binder.bind(WrenManager.class).in(Scopes.SINGLETON);
        binder.bind(WrenMetastore.class).in(Scopes.SINGLETON);
        if (config.isPgWireProtocolEnabled()) {
            binder.bind(CacheManager.class).to(CacheManagerImpl.class).in(Scopes.SINGLETON);
            binder.bind(PgCatalogManager.class).to(PgCatalogManagerImpl.class).in(Scopes.SINGLETON);
        }
        else {
            binder.bind(CacheManager.class).to(NoOpCacheManager.class).in(Scopes.SINGLETON);
            binder.bind(PgCatalogManager.class).to(NoOpPgCatalogManager.class).in(Scopes.SINGLETON);
        }
    }
}
