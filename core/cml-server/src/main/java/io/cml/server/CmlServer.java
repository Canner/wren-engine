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

package io.cml.server;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.cml.pgcatalog.PgCatalogManager;
import io.cml.server.module.BigQueryConnectorModule;
import io.cml.server.module.PostgresWireProtocolModule;
import io.cml.wireprotocol.ssl.EmptyTlsDataProvider;

public class CmlServer
        extends Server
{
    public static void main(String[] args)
    {
        new CmlServer().start();
    }

    @Override
    protected void configure(Injector injector)
    {
        PgCatalogManager pgCatalogManager = injector.getInstance(PgCatalogManager.class);
        pgCatalogManager.initPgCatalog();
    }

    @Override
    protected Iterable<? extends Module> getAdditionalModules()
    {
        return ImmutableList.of(
                new PostgresWireProtocolModule(new EmptyTlsDataProvider()),
                new BigQueryConnectorModule());
    }
}
