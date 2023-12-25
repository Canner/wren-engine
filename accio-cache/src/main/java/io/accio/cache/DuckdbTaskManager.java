package io.accio.cache;

import io.accio.base.client.duckdb.DuckDBConfig;

import javax.inject.Inject;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class DuckdbTaskManager
        implements Closeable
{
    private final ExecutorService taskExecutorService;

    @Inject
    public DuckdbTaskManager(DuckDBConfig duckDBConfig)
    {
        this.taskExecutorService = newFixedThreadPool(duckDBConfig.getMaxConcurrentTasks(), threadsNamed("duckdb-task-%s"));
    }

    public CompletableFuture<Void> addCacheTask(Runnable runnable)
    {
        return CompletableFuture.runAsync(runnable, taskExecutorService);
    }

    public <T> T addQueryTask(Callable<T> callable)
    {
        try {
            return taskExecutorService.submit(callable).get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    // for canner use
    public void addQueryDDLTask(Runnable runnable)
    {
        taskExecutorService.submit(runnable);
    }

    @Override
    public void close()
            throws IOException
    {
        taskExecutorService.shutdownNow();
    }
}
