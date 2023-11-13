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

import io.accio.base.AccioException;
import io.accio.base.dto.CacheInfo;

import java.util.Optional;

import static io.accio.base.metadata.StandardErrorCode.GENERIC_USER_ERROR;
import static java.util.Objects.requireNonNull;

public class CacheInfoPair
{
    private final CacheInfo cacheInfo;
    private final Optional<String> tableName;
    private final Optional<String> errorMessage;
    private final long createTime;

    protected CacheInfoPair(CacheInfo cacheInfo, String tableName, long createTime)
    {
        this(cacheInfo, Optional.of(tableName), Optional.empty(), createTime);
    }

    protected CacheInfoPair(CacheInfo cacheInfo, Optional<String> tableName, Optional<String> errorMessage, long createTime)
    {
        this.cacheInfo = requireNonNull(cacheInfo, "cacheInfo is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.errorMessage = requireNonNull(errorMessage, "errorMessage is null");
        this.createTime = createTime;
    }

    public CacheInfo getCacheInfo()
    {
        return cacheInfo;
    }

    public String getRequiredTableName()
    {
        return tableName.orElseThrow(() -> new AccioException(GENERIC_USER_ERROR, "Mapping table name is refreshing or not exists"));
    }

    public Optional<String> getTableName()
    {
        return tableName;
    }

    public Optional<String> getErrorMessage()
    {
        return errorMessage;
    }

    public long getCreateTime()
    {
        return createTime;
    }
}
