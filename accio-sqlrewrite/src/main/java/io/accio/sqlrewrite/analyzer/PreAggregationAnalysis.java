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

package io.accio.sqlrewrite.analyzer;

import io.accio.base.CatalogSchemaTableName;

import java.util.HashSet;
import java.util.Set;

public class PreAggregationAnalysis
{
    private final Set<CatalogSchemaTableName> tables = new HashSet<>();
    private final Set<CatalogSchemaTableName> preAggregationTables = new HashSet<>();

    public void addTable(CatalogSchemaTableName tableName)
    {
        tables.add(tableName);
    }

    public void addPreAggregationTables(CatalogSchemaTableName preAggregationTables)
    {
        this.preAggregationTables.add(preAggregationTables);
    }

    public boolean onlyPreAggregationTables()
    {
        return preAggregationTables.size() > 0 && tables.equals(preAggregationTables);
    }
}
