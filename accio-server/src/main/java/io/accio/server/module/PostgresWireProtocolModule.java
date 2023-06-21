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

package io.accio.main.server.module;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.accio.main.PostgresNettyProvider;
import io.accio.main.PostgresWireProtocolConfig;
import io.accio.main.pgcatalog.PgCatalogManager;
import io.accio.main.pgcatalog.regtype.RegObjectFactory;
import io.accio.main.wireprotocol.PgWireProtocolExtraRewriter;
import io.accio.main.wireprotocol.PostgresNetty;
import io.accio.main.wireprotocol.ssl.SslContextProvider;
import io.accio.main.wireprotocol.ssl.TlsDataProvider;
import io.accio.preaggregation.ExtraRewriter;
import io.airlift.configuration.AbstractConfigurationAwareModule;
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
        binder.bind(PgCatalogManager.class).in(Scopes.SINGLETON);
        binder.bind(RegObjectFactory.class).in((Scopes.SINGLETON));
        binder.bind(PostgresNetty.class).toProvider(PostgresNettyProvider.class).in(Scopes.SINGLETON);

        // for pre-aggregation extra rewrite
        binder.bind(ExtraRewriter.class).to(PgWireProtocolExtraRewriter.class).in(Scopes.SINGLETON);
    }
}
