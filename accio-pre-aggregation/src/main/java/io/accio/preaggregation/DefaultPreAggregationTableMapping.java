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
package io.accio.preaggregation;

import io.accio.base.CatalogSchemaTableName;
import io.accio.base.client.duckdb.DuckdbClient;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

public class DefaultPreAggregationTableMapping
        implements PreAggregationTableMapping
{
    private final DuckdbClient duckdbClient;
    private final ConcurrentMap<CatalogSchemaTableName, PreAggregationInfoPair> preAggregationTableMapping = new ConcurrentHashMap<>();

    @Inject
    public DefaultPreAggregationTableMapping(DuckdbClient duckdbClient)
    {
        this.duckdbClient = requireNonNull(duckdbClient, "duckdbClient is null");
    }

    @Override
    public void putPreAggregationTableMapping(CatalogSchemaTableName catalogSchemaTableName, PreAggregationInfoPair preAggregationInfoPair)
    {
        synchronized (preAggregationTableMapping) {
            if (preAggregationTableMapping.containsKey(catalogSchemaTableName)) {
                PreAggregationInfoPair existedPreAggregationInfoPair = preAggregationTableMapping.get(catalogSchemaTableName);
                if (existedPreAggregationInfoPair.getCreateTime() > preAggregationInfoPair.getCreateTime()) {
                    preAggregationInfoPair.getTableName().ifPresent(duckdbClient::dropTableQuietly);
                    return;
                }
                existedPreAggregationInfoPair.getTableName().ifPresent(duckdbClient::dropTableQuietly);
            }
            preAggregationTableMapping.put(catalogSchemaTableName, preAggregationInfoPair);
        }
    }

    @Override
    public PreAggregationInfoPair get(CatalogSchemaTableName preAggregationTable)
    {
        return preAggregationTableMapping.get(preAggregationTable);
    }

    @Override
    public void remove(CatalogSchemaTableName preAggregationTable)
    {
        preAggregationTableMapping.remove(preAggregationTable);
    }

    @Override
    public PreAggregationInfoPair getPreAggregationInfoPair(String catalog, String schema, String table)
    {
        return preAggregationTableMapping.get(new CatalogSchemaTableName(catalog, schema, table));
    }

    @Override
    public Optional<String> convertToAggregationTable(CatalogSchemaTableName catalogSchemaTableName)
    {
        return preAggregationTableMapping.get(catalogSchemaTableName).getTableName();
    }

    @Override
    public Set<Map.Entry<CatalogSchemaTableName, PreAggregationInfoPair>> entrySet()
    {
        return preAggregationTableMapping.entrySet();
    }
}
