package io.accio.cache;

import io.accio.base.AccioException;
import io.accio.base.client.AutoCloseableIterator;
import io.accio.base.client.duckdb.DuckDBConfig;
import io.accio.base.client.duckdb.DuckdbClient;

import javax.inject.Inject;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

import static io.accio.base.client.duckdb.DuckdbUtil.convertDuckDBUnits;
import static io.accio.base.metadata.StandardErrorCode.EXCEEDED_GLOBAL_MEMORY_LIMIT;
import static io.accio.base.metadata.StandardErrorCode.EXCEEDED_TIME_LIMIT;
import static io.accio.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DuckdbTaskManager
        implements Closeable
{
    private final DuckdbClient duckdbClient;
    private final ExecutorService taskExecutorService;
    private final DuckDBConfig duckDBConfig;
    private final double cacheMemoryLimit;

    @Inject
    public DuckdbTaskManager(DuckDBConfig duckDBConfig, DuckdbClient duckdbClient)
    {
        this.duckDBConfig = requireNonNull(duckDBConfig, "duckDBConfig is null");
        this.duckdbClient = requireNonNull(duckdbClient, "duckdbClient is null");
        this.taskExecutorService = newFixedThreadPool(duckDBConfig.getMaxConcurrentTasks(), threadsNamed("duckdb-task-%s"));
        this.cacheMemoryLimit = duckDBConfig.getMaxCacheTableSizeRatio() * duckDBConfig.getMemoryLimit().toBytes();
    }

    public CompletableFuture<Void> addCacheTask(Runnable runnable)
    {
        return runAsync(runnable, taskExecutorService);
    }

    public <T> T addQueryTask(Callable<T> callable)
    {
        try {
            checkMemoryLimit();
            return taskExecutorService.submit(callable).get(duckDBConfig.getMaxQueryTimeout(), SECONDS);
        }
        catch (TimeoutException e) {
            throw new AccioException(EXCEEDED_TIME_LIMIT, "Query time limit exceeded", e);
        }
        catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    // for canner use
    public void addQueryDDLTask(Runnable runnable)
    {
        try {
            checkMemoryLimit();
            taskExecutorService.submit(runnable).get(duckDBConfig.getMaxQueryTimeout(), SECONDS);
        }
        catch (TimeoutException e) {
            throw new AccioException(EXCEEDED_TIME_LIMIT, "Query time limit exceeded", e);
        }
        catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public long getMemoryUsageBytes()
    {
        try (AutoCloseableIterator<Object[]> result = duckdbClient.query("SELECT memory_usage FROM pragma_database_size()")) {
            Object[] row = result.next();
            return convertDuckDBUnits(row[0].toString()).toBytes();
        }
        catch (Exception e) {
            throw new AccioException(GENERIC_INTERNAL_ERROR, "Failed to get memory usage", e);
        }
    }

    public void checkCacheMemoryLimit()
    {
        if (getMemoryUsageBytes() >= cacheMemoryLimit) {
            throw new AccioException(EXCEEDED_GLOBAL_MEMORY_LIMIT, "Cache memory limit exceeded");
        }
    }

    private void checkMemoryLimit()
    {
        if (getMemoryUsageBytes() >= duckDBConfig.getMemoryLimit().toBytes()) {
            throw new AccioException(EXCEEDED_GLOBAL_MEMORY_LIMIT, "Duckdb memory limit exceeded");
        }
    }

    @Override
    public void close()
            throws IOException
    {
        taskExecutorService.shutdownNow();
    }
}
