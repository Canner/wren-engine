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
import io.wren.base.client.duckdb.DuckDBConfig;
import io.wren.base.client.duckdb.DuckDBConnectorConfig;
import io.wren.main.connector.duckdb.DuckDBMetadata;
import io.wren.main.connector.duckdb.DuckDBSqlConverter;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class DuckDBConnectorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(DuckDBConnectorConfig.class);
        configBinder(binder).bindConfig(DuckDBConfig.class);
        binder.bind(DuckDBSqlConverter.class).in(Scopes.SINGLETON);
        binder.bind(DuckDBMetadata.class).in(Scopes.SINGLETON);
    }
}
