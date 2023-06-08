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

import io.graphmdl.base.CatalogSchemaTableName;
import io.graphmdl.base.GraphMDLException;
import io.graphmdl.base.client.duckdb.DuckdbClient;
import io.graphmdl.base.dto.PreAggregationInfo;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.graphmdl.base.metadata.StandardErrorCode.GENERIC_USER_ERROR;
import static java.util.Objects.requireNonNull;

public class PreAggregationTableMapping
{
    private final DuckdbClient duckdbClient;
    private final ConcurrentMap<CatalogSchemaTableName, PreAggregationInfoPair> preAggregationTableMapping = new ConcurrentHashMap<>();

    @Inject
    public PreAggregationTableMapping(DuckdbClient duckdbClient)
    {
        this.duckdbClient = requireNonNull(duckdbClient, "duckdbClient is null");
    }

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

    public PreAggregationInfoPair get(CatalogSchemaTableName preAggregationTable)
    {
        return preAggregationTableMapping.get(preAggregationTable);
    }

    public void remove(CatalogSchemaTableName preAggregationTable)
    {
        preAggregationTableMapping.remove(preAggregationTable);
    }

    public PreAggregationInfoPair getPreAggregationInfoPair(String catalog, String schema, String table)
    {
        return preAggregationTableMapping.get(new CatalogSchemaTableName(catalog, schema, table));
    }

    public Optional<String> convertToAggregationTable(CatalogSchemaTableName catalogSchemaTableName)
    {
        return preAggregationTableMapping.get(catalogSchemaTableName).getTableName();
    }

    public Set<Map.Entry<CatalogSchemaTableName, PreAggregationInfoPair>> entrySet()
    {
        return preAggregationTableMapping.entrySet();
    }

    public static class PreAggregationInfoPair
    {
        private final PreAggregationInfo preAggregationInfo;
        private final Optional<String> tableName;
        private final Optional<String> errorMessage;
        private final long createTime;

        protected PreAggregationInfoPair(PreAggregationInfo preAggregationInfo, String tableName, long createTime)
        {
            this(preAggregationInfo, Optional.of(tableName), Optional.empty(), createTime);
        }

        protected PreAggregationInfoPair(PreAggregationInfo preAggregationInfo, Optional<String> tableName, Optional<String> errorMessage, long createTime)
        {
            this.preAggregationInfo = requireNonNull(preAggregationInfo, "preAggregationInfo is null");
            this.tableName = requireNonNull(tableName, "tableName is null");
            this.errorMessage = requireNonNull(errorMessage, "errorMessage is null");
            this.createTime = createTime;
        }

        public PreAggregationInfo getPreAggregationInfo()
        {
            return preAggregationInfo;
        }

        public String getRequiredTableName()
        {
            return tableName.orElseThrow(() -> new GraphMDLException(GENERIC_USER_ERROR, "Mapping table name is refreshing or not exists"));
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
}
