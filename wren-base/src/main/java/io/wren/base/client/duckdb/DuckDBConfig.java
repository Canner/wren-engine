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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.wren.base.client.duckdb;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;

public class DuckDBConfig
{
    public static final String DUCKDB_MEMORY_LIMIT = "duckdb.memory-limit";
    public static final String DUCKDB_HOME_DIRECTORY = "duckdb.home-directory";
    public static final String DUCKDB_TEMP_DIRECTORY = "duckdb.temp-directory";
    public static final String DUCKDB_MAX_CONCURRENT_TASKS = "duckdb.max-concurrent-tasks";
    public static final String DUCKDB_MAX_CACHE_QUERY_TIMEOUT = "duckdb.max-cache-query-timeout";
    public static final String DUCKDB_CACHE_TASK_RETRY_DELAY = "duckdb.cache-task-retry-delay";

    private DataSize memoryLimit = DataSize.of(Runtime.getRuntime().maxMemory() / 2, DataSize.Unit.BYTE);
    private String homeDirectory;
    private String tempDirectory = "/tmp/duck";
    private int maxConcurrentTasks = 10;
    private long maxCacheQueryTimeout = 20;
    private long cacheTaskRetryDelay = 60;

    public DataSize getMemoryLimit()
    {
        return memoryLimit;
    }

    @Config(DUCKDB_MEMORY_LIMIT)
    public void setMemoryLimit(DataSize memoryLimit)
    {
        this.memoryLimit = memoryLimit;
    }

    public String getHomeDirectory()
    {
        return homeDirectory;
    }

    @Config(DUCKDB_HOME_DIRECTORY)
    public void setHomeDirectory(String homeDirectory)
    {
        this.homeDirectory = homeDirectory;
    }

    public String getTempDirectory()
    {
        return tempDirectory;
    }

    @Config(DUCKDB_TEMP_DIRECTORY)
    public void setTempDirectory(String tempDirectory)
    {
        this.tempDirectory = tempDirectory;
    }

    public int getMaxConcurrentTasks()
    {
        return maxConcurrentTasks;
    }

    @Config(DUCKDB_MAX_CONCURRENT_TASKS)
    public void setMaxConcurrentTasks(int maxConcurrentTasks)
    {
        this.maxConcurrentTasks = maxConcurrentTasks;
    }

    public long getMaxCacheQueryTimeout()
    {
        return maxCacheQueryTimeout;
    }

    @Config(DUCKDB_MAX_CACHE_QUERY_TIMEOUT)
    public void setMaxCacheQueryTimeout(long maxCacheQueryTimeout)
    {
        this.maxCacheQueryTimeout = maxCacheQueryTimeout;
    }

    public long getCacheTaskRetryDelay()
    {
        return cacheTaskRetryDelay;
    }

    @Config(DUCKDB_CACHE_TASK_RETRY_DELAY)
    public void setCacheTaskRetryDelay(long cacheTaskRetryDelay)
    {
        this.cacheTaskRetryDelay = cacheTaskRetryDelay;
    }
}
