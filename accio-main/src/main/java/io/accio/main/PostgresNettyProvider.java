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

package io.accio.main;

import com.google.common.collect.ImmutableList;
import io.accio.base.sql.SqlConverter;
import io.accio.cache.CacheManager;
import io.accio.cache.CachedTableMapping;
import io.accio.main.metadata.Metadata;
import io.accio.main.pgcatalog.regtype.RegObjectFactory;
import io.accio.main.wireprotocol.PostgresNetty;
import io.accio.main.wireprotocol.auth.Authentication;
import io.accio.main.wireprotocol.ssl.SslContextProvider;
import org.elasticsearch.common.network.NetworkService;

import javax.inject.Inject;
import javax.inject.Provider;

import static java.util.Objects.requireNonNull;

public class PostgresNettyProvider
        implements Provider<PostgresNetty>
{
    private final PostgresWireProtocolConfig postgresWireProtocolConfig;
    private final AccioConfig accioConfig;
    private final SslContextProvider sslContextProvider;
    private final RegObjectFactory regObjectFactory;

    private final Metadata connector;
    private final SqlConverter sqlConverter;
    private final AccioMetastore accioMetastore;
    private final CacheManager cacheManager;
    private final CachedTableMapping cachedTableMapping;
    private final Authentication authentication;

    @Inject
    public PostgresNettyProvider(
            PostgresWireProtocolConfig postgresWireProtocolConfig,
            AccioConfig accioConfig,
            SslContextProvider sslContextProvider,
            RegObjectFactory regObjectFactory,
            Metadata connector,
            SqlConverter sqlConverter,
            AccioMetastore accioMetastore,
            CacheManager cacheManager,
            CachedTableMapping cachedTableMapping,
            Authentication authentication)
    {
        this.postgresWireProtocolConfig = requireNonNull(postgresWireProtocolConfig, "postgreWireProtocolConfig is null");
        this.accioConfig = requireNonNull(accioConfig, "accioConfig is null");
        this.sslContextProvider = requireNonNull(sslContextProvider, "sslContextProvider is null");
        this.regObjectFactory = requireNonNull(regObjectFactory, "regObjectFactory is null");
        this.connector = requireNonNull(connector, "connector is null");
        this.sqlConverter = requireNonNull(sqlConverter, "sqlConverter is null");
        this.accioMetastore = requireNonNull(accioMetastore, "accioMetastore is null");
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
        this.cachedTableMapping = requireNonNull(cachedTableMapping, "cachedTableMapping is null");
        this.authentication = requireNonNull(authentication, "authentication is null");
    }

    @Override
    public PostgresNetty get()
    {
        NetworkService networkService = new NetworkService(ImmutableList.of());
        PostgresNetty postgresNetty = new PostgresNetty(
                networkService,
                postgresWireProtocolConfig,
                accioConfig,
                sslContextProvider,
                regObjectFactory,
                connector,
                sqlConverter,
                accioMetastore,
                cacheManager,
                cachedTableMapping,
                authentication);
        postgresNetty.start();
        return postgresNetty;
    }
}
