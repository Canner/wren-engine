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
import io.wren.base.config.BigQueryConfig;
import io.wren.main.connector.bigquery.BigQueryCacheService;
import io.wren.main.connector.bigquery.BigQueryMetadata;
import io.wren.main.connector.bigquery.BigQuerySqlConverter;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class BigQueryConnectorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(BigQueryConfig.class);
        binder.bind(BigQuerySqlConverter.class).in(Scopes.SINGLETON);
        binder.bind(BigQueryCacheService.class).in(Scopes.SINGLETON);
        binder.bind(BigQueryMetadata.class).in(Scopes.SINGLETON);
    }
}
