/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cml.server.module;

import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.cml.connector.bigquery.BigQueryConfig;
import io.cml.connector.bigquery.BigQueryConnector;
import io.cml.connector.bigquery.BigQueryCredentialsSupplier;
import io.cml.pgcatalog.builder.BigQueryPgCatalogTableBuilder;
import io.cml.pgcatalog.builder.BigQueryPgFunctionBuilder;
import io.cml.pgcatalog.builder.PgCatalogTableBuilder;
import io.cml.pgcatalog.builder.PgFunctionBuilder;
import io.cml.pgcatalog.regtype.BigQueryPgMetadata;
import io.cml.pgcatalog.regtype.PgMetadata;
import io.cml.spi.connector.Connector;

import java.util.Optional;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class BigQueryConnectorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(Connector.class).to(BigQueryConnector.class);
        binder.bind(PgCatalogTableBuilder.class).to(BigQueryPgCatalogTableBuilder.class).in(Scopes.SINGLETON);
        binder.bind(PgFunctionBuilder.class).to(BigQueryPgFunctionBuilder.class).in(Scopes.SINGLETON);
        binder.bind(PgMetadata.class).to(BigQueryPgMetadata.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(BigQueryConfig.class);
    }

    @Provides
    @Singleton
    public BigQuery provideBigQuery(BigQueryConfig config, HeaderProvider headerProvider, BigQueryCredentialsSupplier bigQueryCredentialsSupplier)
    {
        String billingProjectId = calculateBillingProjectId(config.getParentProjectId(), bigQueryCredentialsSupplier.getCredentials());
        BigQueryOptions.Builder options = BigQueryOptions.newBuilder()
                .setHeaderProvider(headerProvider)
                .setProjectId(billingProjectId)
                .setLocation(config.getLocation().orElse(null));
        // set credentials of provided
        bigQueryCredentialsSupplier.getCredentials().ifPresent(options::setCredentials);
        return options.build().getService();
    }

    @Provides
    @Singleton
    public BigQueryCredentialsSupplier provideBigQueryCredentialsSupplier(BigQueryConfig config)
    {
        return new BigQueryCredentialsSupplier(config.getCredentialsKey(), config.getCredentialsFile());
    }

    static String calculateBillingProjectId(Optional<String> configParentProjectId, Optional<Credentials> credentials)
    {
        // 1. Get from configuration
        if (configParentProjectId.isPresent()) {
            return configParentProjectId.get();
        }
        // 2. Get from the provided credentials, but only ServiceAccountCredentials contains the project id.
        // All other credentials types (User, AppEngine, GCE, CloudShell, etc.) take it from the environment
        if (credentials.isPresent() && credentials.get() instanceof ServiceAccountCredentials) {
            return ((ServiceAccountCredentials) credentials.get()).getProjectId();
        }
        // 3. No configuration was provided, so get the default from the environment
        return BigQueryOptions.getDefaultProjectId();
    }

    @Provides
    @Singleton
    public static HeaderProvider createHeaderProvider()
    {
        return FixedHeaderProvider.create("user-agent", "cml/1");
    }
}
