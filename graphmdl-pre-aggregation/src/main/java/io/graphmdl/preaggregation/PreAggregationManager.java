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

package io.graphmdl.preaggregation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.graphmdl.base.CatalogSchemaTableName;
import io.graphmdl.base.ConnectorRecordIterator;
import io.graphmdl.base.GraphMDL;
import io.graphmdl.base.GraphMDLException;
import io.graphmdl.base.Parameter;
import io.graphmdl.base.SessionContext;
import io.graphmdl.base.client.duckdb.DuckdbClient;
import io.graphmdl.base.dto.Metric;
import io.graphmdl.base.sql.SqlConverter;
import io.graphmdl.sqlrewrite.GraphMDLPlanner;
import io.graphmdl.sqlrewrite.PreAggregationRewrite;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.graphmdl.base.metadata.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.execution.sql.SqlFormatterUtil.getFormattedSql;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PreAggregationManager
{
    private static final Logger LOG = Logger.get(PreAggregationManager.class);
    private static final ParsingOptions PARSE_AS_DECIMAL = new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL);
    private final ExtraRewriter extraRewriter;
    private final PreAggregationService preAggregationService;
    private final SqlParser sqlParser;
    private final SqlConverter sqlConverter;
    private final DuckdbClient duckdbClient;
    private final DuckdbStorageConfig duckdbStorageConfig;
    private final ConcurrentLinkedQueue<PathInfo> tempFileLocations = new ConcurrentLinkedQueue<>();
    private final ConcurrentMap<CatalogSchemaTableName, MetricTablePair> metricTableMapping = new ConcurrentHashMap<>();
    private final ConcurrentMap<CatalogSchemaTableName, ScheduledFuture<?>> metricScheduledFutures = new ConcurrentHashMap<>();

    private final ScheduledThreadPoolExecutor refreshExecutor = new ScheduledThreadPoolExecutor(5, daemonThreadsNamed("pre-aggregation-refresh-%s"));

    @Inject
    public PreAggregationManager(
            SqlParser sqlParser,
            SqlConverter sqlConverter,
            PreAggregationService preAggregationService,
            ExtraRewriter extraRewriter,
            DuckdbClient duckdbClient,
            DuckdbStorageConfig duckdbStorageConfig)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.sqlConverter = requireNonNull(sqlConverter, "sqlConverter is null");
        this.preAggregationService = requireNonNull(preAggregationService, "preAggregationService is null");
        this.extraRewriter = requireNonNull(extraRewriter, "extraRewriter is null");
        this.duckdbClient = requireNonNull(duckdbClient, "duckdbClient is null");
        this.duckdbStorageConfig = requireNonNull(duckdbStorageConfig, "storageConfig is null");
        refreshExecutor.setRemoveOnCancelPolicy(true);
    }

    private void scheduleGraphMDL(GraphMDL graphMDL)
    {
        graphMDL.listPreAggregatedMetrics()
                .forEach(metric ->
                        metricScheduledFutures.put(
                                new CatalogSchemaTableName(graphMDL.getCatalog(), graphMDL.getSchema(), metric.getName()),
                                refreshExecutor.scheduleWithFixedDelay(
                                        () -> doSingleMetricPreAggregation(graphMDL, metric).join(),
                                        metric.getRefreshByTime().toMillis(),
                                        metric.getRefreshByTime().toMillis(),
                                        MILLISECONDS)));
    }

    @VisibleForTesting
    public MetricTablePair getPreAggregationMetricTablePair(String catalog, String schema, String table)
    {
        return metricTableMapping.get(new CatalogSchemaTableName(catalog, schema, table));
    }

    public synchronized void importPreAggregation(GraphMDL mdl)
    {
        removePreAggregation(mdl);
        doPreAggregation(mdl).join();
        scheduleGraphMDL(mdl);
    }

    private CompletableFuture<Void> doPreAggregation(GraphMDL mdl)
    {
        List<CompletableFuture<Void>> futures = mdl.listPreAggregatedMetrics()
                .stream()
                .map(metric -> doSingleMetricPreAggregation(mdl, metric))
                .collect(toImmutableList());
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        return allFutures.whenComplete((v, e) -> {
            if (e != null) {
                LOG.error(e, "Failed to do pre-aggregation");
            }
        });
    }

    public Optional<String> convertToAggregationTable(CatalogSchemaTableName catalogSchemaTableName)
    {
        return metricTableMapping.get(catalogSchemaTableName).getTableName();
    }

    public ConnectorRecordIterator query(String sql, List<Parameter> parameters)
            throws SQLException
    {
        return DuckdbRecordIterator.of(duckdbClient.executeQuery(sql, parameters));
    }

    private CompletableFuture<Void> doSingleMetricPreAggregation(GraphMDL mdl, Metric metric)
    {
        CatalogSchemaTableName catalogSchemaTableName = new CatalogSchemaTableName(mdl.getCatalog(), mdl.getSchema(), metric.getName());
        return runAsync(() -> {
            SessionContext sessionContext = SessionContext.builder()
                    .setCatalog(mdl.getCatalog())
                    .setSchema(mdl.getSchema())
                    .build();
            String graphMDLRewritten = GraphMDLPlanner.rewrite(
                    format("select * from %s", metric.getName()),
                    sessionContext,
                    mdl);
            Statement parsedStatement = sqlParser.createStatement(graphMDLRewritten, PARSE_AS_DECIMAL);
            Statement rewrittenStatement = extraRewriter.rewrite(parsedStatement);
            String duckdbTableName = format("%s_%s", metric.getName(), randomUUID().toString().replace("-", ""));
            if (metricTableMapping.containsKey(catalogSchemaTableName)) {
                duckdbTableName = metricTableMapping.get(catalogSchemaTableName).getTableName().orElse(duckdbTableName);
            }

            createMetricPreAggregation(mdl, metric, sessionContext, rewrittenStatement, duckdbTableName);
            metricTableMapping.put(catalogSchemaTableName, new MetricTablePair(metric, duckdbTableName));
        }).exceptionally(e -> {
            if (metricTableMapping.containsKey(catalogSchemaTableName)) {
                metricTableMapping.get(catalogSchemaTableName).getTableName().ifPresent(this::dropTable);
            }
            String errMsg = format("Failed to do pre-aggregation for metric %s; caused by %s", metric.getName(), e.getMessage());
            LOG.error(e, errMsg);
            metricTableMapping.put(catalogSchemaTableName, new MetricTablePair(metric, Optional.empty(), Optional.of(errMsg)));
            return null;
        });
    }

    private void createMetricPreAggregation(
            GraphMDL mdl,
            Metric metric,
            SessionContext sessionContext,
            Statement rewrittenStatement,
            String duckdbTableName)
    {
        preAggregationService.createPreAggregation(
                        mdl.getCatalog(),
                        mdl.getSchema(),
                        metric.getName(),
                        sqlConverter.convert(getFormattedSql(rewrittenStatement, sqlParser), sessionContext))
                .ifPresent(pathInfo -> {
                    try {
                        tempFileLocations.add(pathInfo);
                        importData(pathInfo.getPath() + "/" + pathInfo.getFilePattern(), duckdbTableName);
                    }
                    finally {
                        removeTempFile(pathInfo);
                    }
                });
    }

    private void importData(String path, String tableName)
    {
        // ref: https://github.com/duckdb/duckdb/issues/1403
        StringBuilder sb = new StringBuilder("INSTALL httpfs;\n" +
                "LOAD httpfs;\n");
        sb.append(format("SET s3_endpoint='%s';\n", duckdbStorageConfig.getEndpoint()));
        duckdbStorageConfig.getAccessKey().ifPresent(accessKey -> sb.append(format("SET s3_access_key_id='%s';\n", accessKey)));
        duckdbStorageConfig.getSecretKey().ifPresent(secretKey -> sb.append(format("SET s3_secret_access_key='%s';\n", secretKey)));
        sb.append(format("SET s3_url_style='%s';\n", duckdbStorageConfig.getUrlStyle()));
        sb.append("BEGIN TRANSACTION;\n");
        sb.append(format("DROP TABLE IF EXISTS %s;\n", tableName));
        sb.append(format("CREATE TABLE \"%s\" AS SELECT * FROM read_parquet('s3://%s');", tableName, path));
        sb.append("COMMIT;\n");
        duckdbClient.executeDDL(sb.toString());
    }

    private void dropTable(String tableName)
    {
        try {
            duckdbClient.executeDDL(format("BEGIN TRANSACTION;DROP TABLE IF EXISTS %s;COMMIT;", tableName));
        }
        catch (Exception e) {
            LOG.error(e, "Failed to drop table %s", tableName);
        }
    }

    public static class MetricTablePair
    {
        private final Metric metric;
        private final Optional<String> tableName;
        private final Optional<String> errorMessage;

        private MetricTablePair(Metric metric, String tableName)
        {
            this(metric, Optional.of(tableName), Optional.empty());
        }

        private MetricTablePair(Metric metric, Optional<String> tableName, Optional<String> errorMessage)
        {
            this.metric = requireNonNull(metric, "metric is null");
            this.tableName = requireNonNull(tableName, "tableName is null");
            this.errorMessage = requireNonNull(errorMessage, "errorMessage is null");
        }

        public Metric getMetric()
        {
            return metric;
        }

        public String getRequiredTableName()
        {
            return tableName.orElseThrow(() -> new GraphMDLException(GENERIC_USER_ERROR, "Mapping table name not exists"));
        }

        public Optional<String> getTableName()
        {
            return tableName;
        }

        public Optional<String> getErrorMessage()
        {
            return errorMessage;
        }
    }

    public Optional<String> rewritePreAggregation(SessionContext sessionContext, String sql, GraphMDL graphMDL)
    {
        try {
            Statement statement = sqlParser.createStatement(sql, new ParsingOptions(AS_DECIMAL));
            return PreAggregationRewrite.rewrite(sessionContext, statement, this::convertToAggregationTable, graphMDL)
                    .map(rewrittenStatement -> getFormattedSql(rewrittenStatement, sqlParser));
        }
        catch (Exception e) {
            LOG.error(e, "Failed to rewrite pre-aggregation for statement: %s", sql);
        }
        return Optional.empty();
    }

    public void removePreAggregation(GraphMDL graphMDL)
    {
        metricScheduledFutures.keySet().stream()
                .filter(catalogSchemaTableName -> catalogSchemaTableName.getCatalogName().equals(graphMDL.getCatalog())
                        && catalogSchemaTableName.getSchemaTableName().getSchemaName().equals(graphMDL.getSchema()))
                .forEach(catalogSchemaTableName -> {
                    metricScheduledFutures.get(catalogSchemaTableName).cancel(true);
                    metricScheduledFutures.remove(catalogSchemaTableName);
                });

        metricTableMapping.entrySet().stream()
                .filter(entry -> entry.getKey().getCatalogName().equals(graphMDL.getCatalog())
                        && entry.getKey().getSchemaTableName().getSchemaName().equals(graphMDL.getSchema()))
                .forEach(entry -> {
                    entry.getValue().getTableName().ifPresent(this::dropTable);
                    metricTableMapping.remove(entry.getKey());
                });
    }

    public boolean metricScheduledFutureExists(CatalogSchemaTableName catalogSchemaTableName)
    {
        return metricScheduledFutures.containsKey(catalogSchemaTableName);
    }

    @PreDestroy
    public void stop()
    {
        refreshExecutor.shutdown();
        cleanTempFiles();
    }

    public void cleanTempFiles()
    {
        try {
            List<PathInfo> locations = ImmutableList.copyOf(tempFileLocations);
            locations.forEach(this::removeTempFile);
        }
        catch (Exception e) {
            LOG.error(e, "Failed to clean temp file");
        }
    }

    public void removeTempFile(PathInfo pathInfo)
    {
        if (tempFileLocations.contains(pathInfo)) {
            preAggregationService.deleteTarget(pathInfo);
            tempFileLocations.remove(pathInfo);
        }
    }
}
