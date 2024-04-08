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
package io.wren.cache;

import com.google.inject.Inject;
import io.wren.base.CatalogSchemaTableName;
import io.wren.base.wireprotocol.PgMetastore;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class DefaultCachedTableMapping
        implements CachedTableMapping
{
    private final PgMetastore pgMetastore;
    private final ConcurrentMap<CatalogSchemaTableName, CacheInfoPair> cachedTableMapping = new ConcurrentHashMap<>();

    @Inject
    public DefaultCachedTableMapping(PgMetastore pgMetastore)
    {
        this.pgMetastore = requireNonNull(pgMetastore, "duckdbClient is null");
    }

    @Override
    public void putCachedTableMapping(CatalogSchemaTableName catalogSchemaTableName, CacheInfoPair cacheInfoPair)
    {
        synchronized (cachedTableMapping) {
            if (cachedTableMapping.containsKey(catalogSchemaTableName)) {
                CacheInfoPair existedCacheInfoPair = cachedTableMapping.get(catalogSchemaTableName);
                if (existedCacheInfoPair.getCreateTime() > cacheInfoPair.getCreateTime()) {
                    cacheInfoPair.getTableName().ifPresent(pgMetastore::dropTableIfExists);
                    return;
                }
                existedCacheInfoPair.getTableName().ifPresent(pgMetastore::dropTableIfExists);
            }
            cachedTableMapping.put(catalogSchemaTableName, cacheInfoPair);
        }
    }

    @Override
    public CacheInfoPair get(CatalogSchemaTableName cachedTable)
    {
        return cachedTableMapping.get(cachedTable);
    }

    @Override
    public void remove(CatalogSchemaTableName cachedTable)
    {
        cachedTableMapping.remove(cachedTable);
    }

    @Override
    public CacheInfoPair getCacheInfoPair(String catalog, String schema, String table)
    {
        return cachedTableMapping.get(new CatalogSchemaTableName(catalog, schema, table));
    }

    @Override
    public Optional<String> convertToCachedTable(CatalogSchemaTableName catalogSchemaTableName)
    {
        return Optional.ofNullable(cachedTableMapping.get(catalogSchemaTableName))
                .flatMap(CacheInfoPair::getTableName);
    }

    @Override
    public Set<Map.Entry<CatalogSchemaTableName, CacheInfoPair>> entrySet()
    {
        return cachedTableMapping.entrySet();
    }

    @Override
    public List<CacheInfoPair> getCacheInfoPairs(String catalogName, String schemaName)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(schemaName, "schemaName is null");
        return cachedTableMapping.entrySet()
                .stream()
                .filter(entry ->
                        entry.getKey().getCatalogName().equals(catalogName)
                                && entry.getKey().getSchemaTableName().getSchemaName().equals(schemaName))
                .map(Map.Entry::getValue)
                .collect(toImmutableList());
    }
}
