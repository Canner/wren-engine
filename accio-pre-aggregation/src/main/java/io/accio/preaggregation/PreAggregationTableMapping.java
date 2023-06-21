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

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface PreAggregationTableMapping
{
    void putPreAggregationTableMapping(CatalogSchemaTableName catalogSchemaTableName, PreAggregationInfoPair preAggregationInfoPair);

    PreAggregationInfoPair get(CatalogSchemaTableName preAggregationTable);

    void remove(CatalogSchemaTableName preAggregationTable);

    PreAggregationInfoPair getPreAggregationInfoPair(String catalog, String schema, String table);

    Optional<String> convertToAggregationTable(CatalogSchemaTableName catalogSchemaTableName);

    Set<Map.Entry<CatalogSchemaTableName, PreAggregationInfoPair>> entrySet();
}
