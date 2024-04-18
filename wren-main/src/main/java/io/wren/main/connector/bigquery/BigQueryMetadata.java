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

package io.wren.main.connector.bigquery;

import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.sql.tree.QualifiedName;
import io.wren.base.Column;
import io.wren.base.ConnectorRecordIterator;
import io.wren.base.Parameter;
import io.wren.base.config.BigQueryConfig;
import io.wren.base.config.ConfigManager;
import io.wren.base.config.WrenConfig;
import io.wren.base.metadata.SchemaTableName;
import io.wren.base.pgcatalog.function.DataSourceFunctionRegistry;
import io.wren.connector.StorageClient;
import io.wren.connector.bigquery.BigQueryClient;
import io.wren.connector.bigquery.BigQueryType;
import io.wren.connector.bigquery.GcsStorageClient;
import io.wren.main.metadata.Metadata;
import io.wren.main.pgcatalog.builder.BigQueryPgFunctionBuilder;
import io.wren.main.pgcatalog.builder.PgFunctionBuilder;
import org.jheaps.annotations.VisibleForTesting;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.wren.main.pgcatalog.PgCatalogUtils.PG_CATALOG_NAME;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class BigQueryMetadata
        implements Metadata
{
    private static final Logger LOG = Logger.get(BigQueryMetadata.class);

    private final DataSourceFunctionRegistry functionRegistry;

    private final Map<String, String> pgToBqFunctionNameMappings;

    private String location;
    private String pgCatalogName;

    private final ConfigManager configManager;

    private final PgFunctionBuilder pgFunctionBuilder;
    private BigQueryClient bigQueryClient;
    private StorageClient cacheStorageClient;

    @Inject
    public BigQueryMetadata(ConfigManager configManager)
    {
        this.configManager = requireNonNull(configManager, "configManager is null");
        // if data source isn't bigquery, don't init the clients.
        if (configManager.getConfig(WrenConfig.class).getDataSourceType().equals(WrenConfig.DataSourceType.BIGQUERY)) {
            try {
                this.bigQueryClient = createBigQueryClient();
                this.cacheStorageClient = createGcsStorageClient();
            }
            catch (Exception ex) {
                LOG.error(ex, "Failed to create BigQuery client. Please check your configuration.");
            }
        }
        BigQueryConfig bigQueryConfig = configManager.getConfig(BigQueryConfig.class);
        this.pgToBqFunctionNameMappings = initPgNameToBqFunctions();
        this.location = bigQueryConfig.getLocation().orElse(null);
        this.pgCatalogName = bigQueryConfig.getMetadataSchemaPrefix() + PG_CATALOG_NAME;
        this.functionRegistry = new DataSourceFunctionRegistry();
        this.pgFunctionBuilder = new BigQueryPgFunctionBuilder(this);
    }

    /**
     * @return mapping table for pg function which can be replaced by bq function.
     */
    private Map<String, String> initPgNameToBqFunctions()
    {
        // bq native function is not case-sensitive, so it is ok to this kind of SqlFunction ctor here.
        return ImmutableMap.<String, String>builder()
                .put("regexp_like", "regexp_contains")
                .build();
    }

    @Override
    public void createSchema(String name)
    {
        bigQueryClient.createSchema(DatasetInfo.newBuilder(name).setLocation(location).build());
    }

    @Override
    public void dropSchemaIfExists(String name)
    {
        bigQueryClient.deleteSchema(DatasetId.of(name));
    }

    @Override
    public QualifiedName resolveFunction(String functionName, int numArgument)
    {
        String funcNameLowerCase = functionName.toLowerCase(ENGLISH);

        if (pgToBqFunctionNameMappings.containsKey(funcNameLowerCase)) {
            return QualifiedName.of(pgToBqFunctionNameMappings.get(funcNameLowerCase));
        }

        // PgFunction is an udf defined in `pg_catalog` dataset. Add dataset prefix to invoke it in global.
        if (functionRegistry.getFunction(funcNameLowerCase, numArgument).isPresent()) {
            return QualifiedName.of(pgCatalogName, functionRegistry.getFunction(funcNameLowerCase, numArgument).get().getRemoteName());
        }

        return QualifiedName.of(functionName);
    }

    @Override
    public void directDDL(String sql)
    {
        try {
            bigQueryClient.query(sql, ImmutableList.of());
        }
        catch (Exception ex) {
            LOG.error(ex, "Failed SQL: %s", sql);
            throw ex;
        }
    }

    @Override
    public ConnectorRecordIterator directQuery(String sql, List<Parameter> parameters)
    {
        requireNonNull(sql, "sql can't be null.");
        try {
            TableResult results = bigQueryClient.query(sql, parameters);
            return BigQueryRecordIterator.of(results);
        }
        catch (BigQueryException ex) {
            LOG.error(ex);
            LOG.error("Failed SQL: %s", sql);
            throw ex;
        }
    }

    @Override
    public List<Column> describeQuery(String sql, List<Parameter> parameters)
    {
        JobStatistics.QueryStatistics queryStatistics = bigQueryClient.queryDryRun(Optional.empty(), sql, parameters);
        return queryStatistics.getSchema().getFields().stream()
                .map(field -> new Column(field.getName(), BigQueryType.toPGType(field)))
                .collect(toImmutableList());
    }

    @VisibleForTesting
    public void dropTable(SchemaTableName schemaTableName)
    {
        bigQueryClient.dropTable(schemaTableName);
    }

    @Override
    public String getDefaultCatalog()
    {
        return bigQueryClient.getProjectId();
    }

    @Override
    public boolean isPgCompatible()
    {
        return false;
    }

    @Override
    public String getPgCatalogName()
    {
        return pgCatalogName;
    }

    @Override
    public synchronized void reload()
    {
        bigQueryClient = createBigQueryClient();
        cacheStorageClient = createGcsStorageClient();
        BigQueryConfig bigQueryConfig = configManager.getConfig(BigQueryConfig.class);
        this.location = bigQueryConfig.getLocation().orElse(null);
        this.pgCatalogName = bigQueryConfig.getMetadataSchemaPrefix() + PG_CATALOG_NAME;
    }

    @Override
    public StorageClient getCacheStorageClient()
    {
        return cacheStorageClient;
    }

    @VisibleForTesting
    public BigQueryClient getBigQueryClient()
    {
        return bigQueryClient;
    }

    private BigQueryClient createBigQueryClient()
    {
        BigQueryConfig config = configManager.getConfig(BigQueryConfig.class);
        return new BigQueryClient(provideBigQuery(config));
    }

    private GcsStorageClient createGcsStorageClient()
    {
        BigQueryConfig config = configManager.getConfig(BigQueryConfig.class);
        return provideGcsStorageClient(config, FixedHeaderProvider.create("user-agent", "wren/1"), new BigQueryCredentialsSupplier(config.getCredentialsKey(), config.getCredentialsFile()));
    }

    private static BigQuery provideBigQuery(BigQueryConfig config)
    {
        HeaderProvider headerProvider = FixedHeaderProvider.create("user-agent", "wren/1");

        BigQueryCredentialsSupplier bigQueryCredentialsSupplier = new BigQueryCredentialsSupplier(config.getCredentialsKey(), config.getCredentialsFile());
        String billingProjectId = calculateBillingProjectId(config.getProjectId(), bigQueryCredentialsSupplier.getCredentials());
        BigQueryOptions.Builder options = BigQueryOptions.newBuilder()
                .setHeaderProvider(headerProvider)
                .setProjectId(billingProjectId)
                .setLocation(config.getLocation().orElse(null));
        // set credentials of provided
        bigQueryCredentialsSupplier.getCredentials().ifPresent(options::setCredentials);
        return options.build().getService();
    }

    private static String calculateBillingProjectId(Optional<String> configParentProjectId, Optional<Credentials> credentials)
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

    public static GcsStorageClient provideGcsStorageClient(BigQueryConfig config, HeaderProvider headerProvider, BigQueryCredentialsSupplier bigQueryCredentialsSupplier)
    {
        String billingProjectId = calculateBillingProjectId(config.getProjectId(), bigQueryCredentialsSupplier.getCredentials());
        StorageOptions.Builder options = StorageOptions.newBuilder()
                .setHeaderProvider(headerProvider)
                .setProjectId(billingProjectId);
        bigQueryCredentialsSupplier.getCredentials().ifPresent(options::setCredentials);
        return new GcsStorageClient(options.build().getService());
    }

    @Override
    public PgFunctionBuilder getPgFunctionBuilder()
    {
        return pgFunctionBuilder;
    }

    @Override
    public void close() {}
}
