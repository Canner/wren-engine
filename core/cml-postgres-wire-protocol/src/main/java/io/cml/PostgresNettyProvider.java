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

import com.google.common.collect.ImmutableList;
import io.cml.wireprotocol.PostgresNetty;
import io.cml.wireprotocol.postgres.ssl.SslContextProvider;
import io.trino.sql.parser.SqlParser;
import org.elasticsearch.common.network.NetworkService;

import javax.inject.Inject;
import javax.inject.Provider;

import static java.util.Objects.requireNonNull;

public class PostgresNettyProvider
        implements Provider<PostgresNetty>
{
    private final SqlParser sqlParser;
    private final PostgresWireProtocolConfig postgresWireProtocolConfig;
    private final SslContextProvider sslContextProvider;

    @Inject
    public PostgresNettyProvider(
            SqlParser sqlParser,
            PostgresWireProtocolConfig postgresWireProtocolConfig,
            SslContextProvider sslContextProvider)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.postgresWireProtocolConfig = requireNonNull(postgresWireProtocolConfig, "postgreWireProtocolConfig is null");
        this.sslContextProvider = requireNonNull(sslContextProvider, "sslContextProvider is null");
    }

    @Override
    public PostgresNetty get()
    {
        NetworkService networkService = new NetworkService(ImmutableList.of());
        PostgresNetty postgresNetty = new PostgresNetty(
                sqlParser,
                networkService,
                postgresWireProtocolConfig,
                sslContextProvider);
        postgresNetty.start();
        return postgresNetty;
    }
}
