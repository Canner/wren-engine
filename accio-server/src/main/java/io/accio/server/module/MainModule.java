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

package io.accio.server.module;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.accio.base.config.ConfigManager;
import io.accio.base.sql.SqlConverter;
import io.accio.cache.CacheService;
import io.accio.main.connector.CacheServiceManager;
import io.accio.main.metadata.Metadata;
import io.accio.main.metadata.MetadataManager;
import io.accio.main.pgcatalog.builder.PgFunctionBuilderManager;
import io.accio.main.sql.SqlConverterManager;
import io.airlift.configuration.AbstractConfigurationAwareModule;

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
