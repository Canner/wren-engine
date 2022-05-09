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

package io.cml;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.cml.pgcatalog.PgCatalogTableManager;
import io.cml.pgcatalog.builder.BigQueryPgCatalogBuilder;
import io.cml.pgcatalog.builder.PgCatalogBuilder;
import io.cml.wireprotocol.PostgresNetty;
import io.cml.wireprotocol.postgres.ssl.SslContextProvider;
import io.cml.wireprotocol.ssl.TlsDataProvider;
import io.trino.sql.parser.SqlParser;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class PostgresWireProtocolModule
        extends AbstractConfigurationAwareModule
{
    private final TlsDataProvider tlsDataProvider;

    public PostgresWireProtocolModule(TlsDataProvider tlsDataProvider)
    {
        this.tlsDataProvider = tlsDataProvider;
    }

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(PostgresWireProtocolConfig.class);
        binder.bind(SqlParser.class).in(Scopes.SINGLETON);
        binder.bind(TlsDataProvider.class).toInstance(tlsDataProvider);
        binder.bind(SslContextProvider.class).in(Scopes.SINGLETON);
        binder.bind(PgCatalogBuilder.class).to(BigQueryPgCatalogBuilder.class).in(Scopes.SINGLETON);
        binder.bind(PgCatalogTableManager.class).in(Scopes.SINGLETON);
        binder.bind(PostgresNetty.class).toProvider(PostgresNettyProvider.class).in(Scopes.SINGLETON);
    }
}
