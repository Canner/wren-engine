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

package io.wren.server.module;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.wren.base.config.ConfigManager;
import io.wren.base.sql.SqlConverter;
import io.wren.cache.CacheService;
import io.wren.main.connector.CacheServiceManager;
import io.wren.main.metadata.Metadata;
import io.wren.main.metadata.MetadataManager;
import io.wren.main.pgcatalog.builder.PgFunctionBuilderManager;
import io.wren.main.sql.SqlConverterManager;

public class MainModule
        extends AbstractConfigurationAwareModule

{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(Metadata.class).to(MetadataManager.class).in(Scopes.SINGLETON);
        binder.bind(SqlConverter.class).to(SqlConverterManager.class).in(Scopes.SINGLETON);
        binder.bind(CacheService.class).to(CacheServiceManager.class).in(Scopes.SINGLETON);
        binder.bind(PgFunctionBuilderManager.class).in(Scopes.SINGLETON);
        binder.bind(ConfigManager.class).in(Scopes.SINGLETON);
    }
}
