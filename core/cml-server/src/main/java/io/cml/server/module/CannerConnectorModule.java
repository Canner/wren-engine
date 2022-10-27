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

package io.cml.server.module;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.cml.connector.canner.CannerClient;
import io.cml.connector.canner.CannerConfig;
import io.cml.connector.canner.CannerMetadta;
import io.cml.connector.canner.CannerSqlConverter;
import io.cml.metadata.Metadata;
import io.cml.pgcatalog.PgCatalogCreated;
import io.cml.pgcatalog.PgCatalogManager;
import io.cml.pgcatalog.regtype.CannerPgMetadata;
import io.cml.pgcatalog.regtype.PgMetadata;
import io.cml.sql.SqlConverter;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class CannerConnectorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(Metadata.class).to(CannerMetadta.class).in(Scopes.SINGLETON);
        binder.bind(PgMetadata.class).to(CannerPgMetadata.class).in(Scopes.SINGLETON);
        binder.bind(PgCatalogManager.class).to(PgCatalogCreated.class).in(Scopes.SINGLETON);
        binder.bind(SqlConverter.class).to(CannerSqlConverter.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(CannerConfig.class);
    }

    @Provides
    @Singleton
    public static CannerClient provideCannerClient(CannerConfig cannerConfig)
    {
        return new CannerClient(cannerConfig);
    }
}
