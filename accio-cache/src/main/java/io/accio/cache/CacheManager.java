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

package io.accio.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.accio.base.AccioException;
import io.accio.base.AccioMDL;
import io.accio.base.CatalogSchemaTableName;
import io.accio.base.ConnectorRecordIterator;
import io.accio.base.Parameter;
import io.accio.base.SessionContext;
import io.accio.base.client.duckdb.DuckdbClient;
import io.accio.base.dto.CacheInfo;
import io.accio.base.sql.SqlConverter;
import io.accio.cache.dto.CachedTable;
import io.accio.sqlrewrite.AccioPlanner;
import io.airlift.log.Logger;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.accio.base.CatalogSchemaTableName.catalogSchemaTableName;
import static io.accio.base.metadata.StandardErrorCode.GENERIC_USER_ERROR;
import static io.accio.cache.TaskInfo.TaskStatus.DONE;
import static io.accio.cache.TaskInfo.TaskStatus.RUNNING;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.trino.execution.sql.SqlFormatterUtil.getFormattedSql;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

public class CacheManager
{
    private static final Logger LOG = Logger.get(CacheManager.class);
    private static final ParsingOptions PARSE_AS_DECIMAL = new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL);
    private final ExtraRewriter extraRewriter;
    private final CacheService cacheService;
    private final SqlParser sqlParser;
    private final SqlConverter sqlConverter;
    private final DuckdbClient duckdbClient;
    private final CacheStorageConfig cacheStorageConfig;
    private final ConcurrentLinkedQueue<PathInfo> tempFileLocations = new ConcurrentLinkedQueue<>();
    private final CachedTableMapping cachedTableMapping;
    private final ConcurrentMap<CatalogSchemaTableName, ScheduledFuture<?>> cacheScheduledFutures = new ConcurrentHashMap<>();
    private final ScheduledThreadPoolExecutor refreshExecutor = new ScheduledThreadPoolExecutor(5, daemonThreadsNamed("cache-refresh-%s"));

    private final ExecutorService executorService = newCachedThreadPool(threadsNamed("cache-manager-%s"));
    private final ConcurrentHashMap<CatalogSchemaTableName, Task> tasks = new ConcurrentHashMap<>();

    @Inject
    public CacheManager(
            SqlConverter sqlConverter,
            CacheService cacheService,
            ExtraRewriter extraRewriter,
            DuckdbClient duckdbClient,
            CacheStorageConfig cacheStorageConfig,
            CachedTableMapping cachedTableMapping)
    {
        this.sqlParser = new SqlParser();
        this.sqlConverter = requireNonNull(sqlConverter, "sqlConverter is null");
        this.cacheService = requireNonNull(cacheService, "cacheService is null");
        this.extraRewriter = requireNonNull(extraRewriter, "extraRewriter is null");
        this.duckdbClient = requireNonNull(duckdbClient, "duckdbClient is null");
        this.cacheStorageConfig = requireNonNull(cacheStorageConfig, "cacheStorageConfig is null");
        this.cachedTableMapping = requireNonNull(cachedTableMapping, "cachedTableMapping is null");
        refreshExecutor.setRemoveOnCancelPolicy(true);
    }

    private synchronized CompletableFuture<Void> refreshCache(AccioMDL mdl, CacheInfo cacheInfo)
    {
        CatalogSchemaTableName catalogSchemaTableName = catalogSchemaTableName(mdl.getCatalog(), mdl.getSchema(), cacheInfo.getName());
        Optional<Task> taskOptional = Optional.ofNullable(tasks.get(catalogSchemaTableName));
        if (taskOptional.isPresent() && taskOptional.get().getTaskInfo().inProgress()) {
            throw new AccioException(GENERIC_USER_ERROR, format("cache is already running; catalogName: %s, schemaName: %s, tableName: %s", mdl.getCatalog(), mdl.getSchema(), cacheInfo.getName()));
        }
        removeCacheIfExist(catalogSchemaTableName);
        return doCache(mdl, cacheInfo);
    }

    private CompletableFuture<Void> handleCache(AccioMDL mdl, CacheInfo cacheInfo)
    {
        return refreshCache(mdl, cacheInfo)
                .thenRun(() -> {
                    if (cacheInfo.getRefreshTime().toMillis() > 0) {
                        cacheScheduledFutures.put(
                                new CatalogSchemaTableName(mdl.getCatalog(), mdl.getSchema(), cacheInfo.getName()),
                                refreshExecutor.scheduleWithFixedDelay(
                                        () -> createTask(mdl, cacheInfo).join(),
                                        cacheInfo.getRefreshTime().toMillis(),
                                        cacheInfo.getRefreshTime().toMillis(),
                                        MILLISECONDS));
                    }
                });
    }

    public ConnectorRecordIterator query(String sql, List<Parameter> parameters)
            throws SQLException
    {
        return DuckdbRecordIterator.of(duckdbClient, sql, parameters.stream().collect(toImmutableList()));
    }

    private CompletableFuture<Void> doCache(AccioMDL mdl, CacheInfo cacheInfo)
    {
        CatalogSchemaTableName catalogSchemaTableName = new CatalogSchemaTableName(mdl.getCatalog(), mdl.getSchema(), cacheInfo.getName());
        String duckdbTableName = format("%s_%s", cacheInfo.getName(), randomUUID().toString().replace("-", ""));
        long createTime = currentTimeMillis();
        return runAsync(() -> {
            SessionContext sessionContext = SessionContext.builder()
                    .setCatalog(mdl.getCatalog())
                    .setSchema(mdl.getSchema())
                    .build();
            String accioRewritten = AccioPlanner.rewrite(
                    format("select * from %s", cacheInfo.getName()),
                    sessionContext,
                    mdl);
            Statement parsedStatement = sqlParser.createStatement(accioRewritten, PARSE_AS_DECIMAL);
            Statement rewrittenStatement = extraRewriter.rewrite(parsedStatement);

            createCache(mdl, cacheInfo, sessionContext, rewrittenStatement, duckdbTableName);
            cachedTableMapping.putCachedTableMapping(catalogSchemaTableName, new CacheInfoPair(cacheInfo, duckdbTableName, createTime));
        }).exceptionally(e -> {
            duckdbClient.dropTableQuietly(duckdbTableName);
            String errMsg = format("Failed to do cache for cacheInfo %s; caused by %s", cacheInfo.getName(), e.getMessage());
            LOG.error(e, errMsg);
            cachedTableMapping.putCachedTableMapping(catalogSchemaTableName, new CacheInfoPair(cacheInfo, Optional.empty(), Optional.of(errMsg), createTime));
            return null;
        });
    }

    private void createCache(
            AccioMDL mdl,
            CacheInfo cacheInfo,
            SessionContext sessionContext,
            Statement rewrittenStatement,
            String duckdbTableName)
    {
        cacheService.createCache(
                        mdl.getCatalog(),
                        mdl.getSchema(),
                        cacheInfo.getName(),
                        sqlConverter.convert(getFormattedSql(rewrittenStatement, sqlParser), sessionContext))
                .ifPresent(pathInfo -> {
                    try {
                        tempFileLocations.add(pathInfo);
                        refreshCacheInDuckDB(pathInfo.getPath() + "/" + pathInfo.getFilePattern(), duckdbTableName);
                    }
                    finally {
                        removeTempFile(pathInfo);
                    }
                });
    }

    private void refreshCacheInDuckDB(String path, String tableName)
    {
        duckdbClient.executeDDL(cacheStorageConfig.generateDuckdbParquetStatement(path, tableName));
    }

    public void removeCacheIfExist(String catalogName, String schemaName)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(schemaName, "schemaName is null");

        cacheScheduledFutures.keySet().stream()
                .filter(catalogSchemaTableName -> catalogSchemaTableName.getCatalogName().equals(catalogName)
                        && catalogSchemaTableName.getSchemaTableName().getSchemaName().equals(schemaName))
                .forEach(catalogSchemaTableName -> {
                    cacheScheduledFutures.get(catalogSchemaTableName).cancel(true);
                    cacheScheduledFutures.remove(catalogSchemaTableName);
                });

        cachedTableMapping.entrySet().stream()
                .filter(entry -> entry.getKey().getCatalogName().equals(catalogName)
                        && entry.getKey().getSchemaTableName().getSchemaName().equals(schemaName))
                .forEach(entry -> {
                    entry.getValue().getTableName().ifPresent(duckdbClient::dropTableQuietly);
                    cachedTableMapping.remove(entry.getKey());
                });

        tasks.keySet().stream()
                .filter(catalogSchemaTableName -> catalogSchemaTableName.getCatalogName().equals(catalogName)
                        && catalogSchemaTableName.getSchemaTableName().getSchemaName().equals(schemaName))
                .forEach(tasks::remove);
    }

    public void removeCacheIfExist(CatalogSchemaTableName catalogSchemaTableName)
    {
        if (cacheScheduledFutures.containsKey(catalogSchemaTableName)) {
            cacheScheduledFutures.get(catalogSchemaTableName).cancel(true);
            cacheScheduledFutures.remove(catalogSchemaTableName);
        }

        Optional.ofNullable(cachedTableMapping.get(catalogSchemaTableName)).ifPresent(cacheInfoPair -> {
            cacheInfoPair.getTableName().ifPresent(duckdbClient::dropTableQuietly);
            cachedTableMapping.remove(catalogSchemaTableName);
        });

        tasks.remove(catalogSchemaTableName);
    }

    public boolean cacheScheduledFutureExists(CatalogSchemaTableName catalogSchemaTableName)
    {
        return cacheScheduledFutures.containsKey(catalogSchemaTableName);
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
            cacheService.deleteTarget(pathInfo);
            tempFileLocations.remove(pathInfo);
        }
    }

    public List<TaskInfo> createTaskUntilDone(AccioMDL mdl)
    {
        return createTask(mdl)
                .thenApply(taskInfos -> taskInfos.stream()
                        .map(taskInfo -> {
                            tasks.get(taskInfo.getCatalogSchemaTableName()).waitUntilDone();
                            return getTaskInfo(taskInfo.getCatalogSchemaTableName()).join();
                        })
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(toList()))
                .join();
    }

    public CompletableFuture<List<TaskInfo>> createTask(AccioMDL mdl)
    {
        return supplyAsync(() ->
                mdl.listCached().stream().map(cacheInfo -> createTask(mdl, cacheInfo).join()).collect(toList()));
    }

    public CompletableFuture<TaskInfo> createTask(AccioMDL mdl, CacheInfo cacheInfo)
    {
        return supplyAsync(() -> {
            CatalogSchemaTableName catalogSchemaTableName = new CatalogSchemaTableName(mdl.getCatalog(), mdl.getSchema(), cacheInfo.getName());
            TaskInfo taskInfo = new TaskInfo(mdl.getCatalog(), mdl.getSchema(), cacheInfo.getName(), RUNNING, Instant.now());
            // To fix flaky test, we pass value to tasks instead of a reference;
            Task task = new Task(TaskInfo.copyFrom(taskInfo), handleCache(mdl, cacheInfo));
            tasks.put(catalogSchemaTableName, task);
            return taskInfo;
        });
    }

    public CompletableFuture<List<TaskInfo>> listTaskInfo(String catalogName, String schemaName)
    {
        Predicate<TaskInfo> catalogNamePred = catalogName.isEmpty() ?
                (t) -> true :
                (t) -> catalogName.equals(t.getCatalogName());

        Predicate<TaskInfo> schemaNamePred = schemaName.isEmpty() ?
                (t) -> true :
                (t) -> schemaName.equals(t.getSchemaName());

        return supplyAsync(
                () -> tasks.values().stream()
                        .map(Task::getTaskInfo)
                        .filter(catalogNamePred.and(schemaNamePred))
                        .collect(toList()),
                executorService);
    }

    public CompletableFuture<Optional<TaskInfo>> getTaskInfo(CatalogSchemaTableName catalogSchemaTableName)
    {
        requireNonNull(catalogSchemaTableName);
        return supplyAsync(
                () -> Optional.ofNullable(tasks.get(catalogSchemaTableName)).map(Task::getTaskInfo),
                executorService);
    }

    @VisibleForTesting
    public void untilTaskDone(CatalogSchemaTableName name)
    {
        Optional.ofNullable(tasks.get(name)).ifPresent(Task::waitUntilDone);
    }

    private class Task
    {
        private final TaskInfo taskInfo;
        private final CompletableFuture<?> completableFuture;

        public Task(TaskInfo taskInfo, CompletableFuture<?> completableFuture)
        {
            this.taskInfo = taskInfo;
            this.completableFuture =
                    completableFuture
                            .thenRun(() -> {
                                CacheInfoPair cacheInfoPair = cachedTableMapping.getCacheInfoPair(
                                        taskInfo.getCatalogName(),
                                        taskInfo.getSchemaName(), taskInfo.getTableName());
                                taskInfo.setCachedTable(new CachedTable(
                                        cacheInfoPair.getCacheInfo().getName(),
                                        cacheInfoPair.getErrorMessage(),
                                        cacheInfoPair.getCacheInfo().getRefreshTime(),
                                        Instant.ofEpochMilli(cacheInfoPair.getCreateTime())));
                                taskInfo.setTaskStatus(DONE);
                            });
        }

        public TaskInfo getTaskInfo()
        {
            return taskInfo;
        }

        public void waitUntilDone()
        {
            completableFuture.join();
        }
    }
}
