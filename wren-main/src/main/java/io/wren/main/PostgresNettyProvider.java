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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.wren.base.config.ConfigManager;
import io.wren.base.config.PostgresWireProtocolConfig;
import io.wren.base.sql.SqlConverter;
import io.wren.base.wireprotocol.PgMetastore;
import io.wren.cache.CacheManager;
import io.wren.cache.CachedTableMapping;
import io.wren.main.metadata.Metadata;
import io.wren.main.pgcatalog.regtype.RegObjectFactory;
import io.wren.main.wireprotocol.PostgresNetty;
import io.wren.main.wireprotocol.auth.Authentication;
import io.wren.main.wireprotocol.ssl.SslContextProvider;
import org.elasticsearch.common.network.NetworkService;

import static java.util.Objects.requireNonNull;

public class PostgresNettyProvider
        implements Provider<PostgresNetty>
{
    private final PostgresWireProtocolConfig postgresWireProtocolConfig;
    private final ConfigManager configManager;
    private final SslContextProvider sslContextProvider;
    private final RegObjectFactory regObjectFactory;

    private final Metadata connector;
    private final SqlConverter sqlConverter;
    private final WrenMetastore wrenMetastore;
    private final CacheManager cacheManager;
    private final CachedTableMapping cachedTableMapping;
    private final Authentication authentication;
    private final PgMetastore pgMetastore;

    @Inject
    public PostgresNettyProvider(
            PostgresWireProtocolConfig postgresWireProtocolConfig,
            ConfigManager configManager,
            SslContextProvider sslContextProvider,
            RegObjectFactory regObjectFactory,
            Metadata connector,
            SqlConverter sqlConverter,
            WrenMetastore wrenMetastore,
            CacheManager cacheManager,
            CachedTableMapping cachedTableMapping,
            Authentication authentication,
            PgMetastore pgMetastore)
    {
        this.postgresWireProtocolConfig = requireNonNull(postgresWireProtocolConfig, "postgreWireProtocolConfig is null");
        this.configManager = requireNonNull(configManager, "configManager is null");
        this.sslContextProvider = requireNonNull(sslContextProvider, "sslContextProvider is null");
        this.regObjectFactory = requireNonNull(regObjectFactory, "regObjectFactory is null");
        this.connector = requireNonNull(connector, "connector is null");
        this.sqlConverter = requireNonNull(sqlConverter, "sqlConverter is null");
        this.wrenMetastore = requireNonNull(wrenMetastore, "wrenMetastore is null");
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
        this.cachedTableMapping = requireNonNull(cachedTableMapping, "cachedTableMapping is null");
        this.authentication = requireNonNull(authentication, "authentication is null");
        this.pgMetastore = requireNonNull(pgMetastore, "pgMetastore is null");
    }

    @Override
    public PostgresNetty get()
    {
        NetworkService networkService = new NetworkService(ImmutableList.of());
        PostgresNetty postgresNetty = new PostgresNetty(
                networkService,
                postgresWireProtocolConfig,
                configManager,
                sslContextProvider,
                regObjectFactory,
                connector,
                sqlConverter,
                wrenMetastore,
                cacheManager,
                cachedTableMapping,
                authentication,
                pgMetastore);
        postgresNetty.start();
        return postgresNetty;
    }
}
