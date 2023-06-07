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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.graphmdl.base.CatalogSchemaTableName;
import io.graphmdl.base.ConnectorRecordIterator;
import io.graphmdl.base.GraphMDL;
import io.graphmdl.base.Parameter;
import io.graphmdl.base.SessionContext;
import io.graphmdl.base.client.duckdb.DuckdbClient;
import io.graphmdl.base.dto.PreAggregationInfo;
import io.graphmdl.base.sql.SqlConverter;
import io.graphmdl.sqlrewrite.GraphMDLPlanner;
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
import static io.trino.execution.sql.SqlFormatterUtil.getFormattedSql;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
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
    private final PreAggregationStorageConfig preAggregationStorageConfig;
    private final ConcurrentLinkedQueue<PathInfo> tempFileLocations = new ConcurrentLinkedQueue<>();
    private final PreAggregationTableMapping preAggregationTableMapping;
    private final ConcurrentMap<CatalogSchemaTableName, ScheduledFuture<?>> preAggregationScheduledFutures = new ConcurrentHashMap<>();
    private final ScheduledThreadPoolExecutor refreshExecutor = new ScheduledThreadPoolExecutor(5, daemonThreadsNamed("pre-aggregation-refresh-%s"));

    @Inject
    public PreAggregationManager(
            SqlConverter sqlConverter,
            PreAggregationService preAggregationService,
            ExtraRewriter extraRewriter,
            DuckdbClient duckdbClient,
            PreAggregationStorageConfig preAggregationStorageConfig,
            PreAggregationTableMapping preAggregationTableMapping)
    {
        this.sqlParser = new SqlParser();
        this.sqlConverter = requireNonNull(sqlConverter, "sqlConverter is null");
        this.preAggregationService = requireNonNull(preAggregationService, "preAggregationService is null");
        this.extraRewriter = requireNonNull(extraRewriter, "extraRewriter is null");
        this.duckdbClient = requireNonNull(duckdbClient, "duckdbClient is null");
        this.preAggregationStorageConfig = requireNonNull(preAggregationStorageConfig, "preAggregationStorageConfig is null");
        this.preAggregationTableMapping = requireNonNull(preAggregationTableMapping, "preAggregationTableMapping is null");
        refreshExecutor.setRemoveOnCancelPolicy(true);
    }

    public synchronized void refreshPreAggregation(GraphMDL mdl)
    {
        removePreAggregation(mdl.getCatalog(), mdl.getSchema());
        doPreAggregation(mdl).join();
    }

    private CompletableFuture<Void> doPreAggregation(GraphMDL mdl)
    {
        List<CompletableFuture<Void>> futures = mdl.listPreAggregated()
                .stream()
                .map(preAggregationInfo ->
                        doSinglePreAggregation(mdl, preAggregationInfo)
                                .thenRun(() -> preAggregationScheduledFutures.put(
                                        new CatalogSchemaTableName(mdl.getCatalog(), mdl.getSchema(), preAggregationInfo.getName()),
                                        refreshExecutor.scheduleWithFixedDelay(
                                                () -> doSinglePreAggregation(mdl, preAggregationInfo).join(),
                                                preAggregationInfo.getRefreshTime().toMillis(),
                                                preAggregationInfo.getRefreshTime().toMillis(),
                                                MILLISECONDS))))
                .collect(toImmutableList());
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        return allFutures.whenComplete((v, e) -> {
            if (e != null) {
                LOG.error(e, "Failed to do pre-aggregation");
            }
        });
    }

    public ConnectorRecordIterator query(String sql, List<Parameter> parameters)
            throws SQLException
    {
        return DuckdbRecordIterator.of(duckdbClient.executeQuery(sql, parameters));
    }

    private CompletableFuture<Void> doSinglePreAggregation(GraphMDL mdl, PreAggregationInfo preAggregationInfo)
    {
        CatalogSchemaTableName catalogSchemaTableName = new CatalogSchemaTableName(mdl.getCatalog(), mdl.getSchema(), preAggregationInfo.getName());
        String duckdbTableName = format("%s_%s", preAggregationInfo.getName(), randomUUID().toString().replace("-", ""));
        long createTime = currentTimeMillis();
        return runAsync(() -> {
            SessionContext sessionContext = SessionContext.builder()
                    .setCatalog(mdl.getCatalog())
                    .setSchema(mdl.getSchema())
                    .build();
            String graphMDLRewritten = GraphMDLPlanner.rewrite(
                    format("select * from %s", preAggregationInfo.getName()),
                    sessionContext,
                    mdl);
            Statement parsedStatement = sqlParser.createStatement(graphMDLRewritten, PARSE_AS_DECIMAL);
            Statement rewrittenStatement = extraRewriter.rewrite(parsedStatement);

            createPreAggregation(mdl, preAggregationInfo, sessionContext, rewrittenStatement, duckdbTableName);
            preAggregationTableMapping.putpreAggregationTableMapping(catalogSchemaTableName, new PreAggregationTableMapping.PreAggregationInfoPair(preAggregationInfo, duckdbTableName, createTime));
        }).exceptionally(e -> {
            duckdbClient.dropTableQuietly(duckdbTableName);
            String errMsg = format("Failed to do pre-aggregation for preAggregationInfo %s; caused by %s", preAggregationInfo.getName(), e.getMessage());
            LOG.error(e, errMsg);
            preAggregationTableMapping.putpreAggregationTableMapping(catalogSchemaTableName, new PreAggregationTableMapping.PreAggregationInfoPair(preAggregationInfo, Optional.empty(), Optional.of(errMsg), createTime));
            return null;
        });
    }

    private void createPreAggregation(
            GraphMDL mdl,
            PreAggregationInfo preAggregationInfo,
            SessionContext sessionContext,
            Statement rewrittenStatement,
            String duckdbTableName)
    {
        preAggregationService.createPreAggregation(
                        mdl.getCatalog(),
                        mdl.getSchema(),
                        preAggregationInfo.getName(),
                        sqlConverter.convert(getFormattedSql(rewrittenStatement, sqlParser), sessionContext))
                .ifPresent(pathInfo -> {
                    try {
                        tempFileLocations.add(pathInfo);
                        refreshPreAggInDuckDB(pathInfo.getPath() + "/" + pathInfo.getFilePattern(), duckdbTableName);
                    }
                    finally {
                        removeTempFile(pathInfo);
                    }
                });
    }

    private void refreshPreAggInDuckDB(String path, String tableName)
    {
        duckdbClient.executeDDL(preAggregationStorageConfig.generateDuckdbParquetStatement(path, tableName));
    }

    public void removePreAggregation(String catalogName, String schemaName)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(schemaName, "schemaName is null");

        preAggregationScheduledFutures.keySet().stream()
                .filter(catalogSchemaTableName -> catalogSchemaTableName.getCatalogName().equals(catalogName)
                        && catalogSchemaTableName.getSchemaTableName().getSchemaName().equals(schemaName))
                .forEach(catalogSchemaTableName -> {
                    preAggregationScheduledFutures.get(catalogSchemaTableName).cancel(true);
                    preAggregationScheduledFutures.remove(catalogSchemaTableName);
                });

        preAggregationTableMapping.entrySet().stream()
                .filter(entry -> entry.getKey().getCatalogName().equals(catalogName)
                        && entry.getKey().getSchemaTableName().getSchemaName().equals(schemaName))
                .forEach(entry -> {
                    entry.getValue().getTableName().ifPresent(duckdbClient::dropTableQuietly);
                    preAggregationTableMapping.remove(entry.getKey());
                });
    }

    public boolean preAggregationScheduledFutureExists(CatalogSchemaTableName catalogSchemaTableName)
    {
        return preAggregationScheduledFutures.containsKey(catalogSchemaTableName);
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
