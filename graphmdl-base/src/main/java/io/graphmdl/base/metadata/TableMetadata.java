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
package io.graphmdl.base.metadata;

import com.google.common.collect.ImmutableList;
import io.graphmdl.base.type.PGType;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class TableMetadata
{
    private final SchemaTableName table;
    private final List<ColumnMetadata> columns;

    public static Builder builder(SchemaTableName tableName)
    {
        return new Builder(tableName);
    }

    public TableMetadata(SchemaTableName table, List<ColumnMetadata> columns)
    {
        this.table = requireNonNull(table, "table is null");
        this.columns = List.copyOf(requireNonNull(columns, "columns is null"));
    }

    public SchemaTableName getTable()
    {
        return table;
    }

    public List<ColumnMetadata> getColumns()
    {
        return columns;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ConnectorTableMetadata{");
        sb.append("table=").append(table);
        sb.append(", columns=").append(columns);
        sb.append('}');
        return sb.toString();
    }

    public static class Builder
    {
        private final SchemaTableName tableName;
        private final ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();

        private Builder(SchemaTableName tableName)
        {
            this.tableName = tableName;
        }

        public Builder column(String columnName, PGType<?> type)
        {
            columns.add(ColumnMetadata.builder()
                    .setName(columnName)
                    .setType(type).build());
            return this;
        }

        public Builder column(ColumnMetadata columnMetadata)
        {
            columns.add(columnMetadata);
            return this;
        }

        public TableMetadata build()
        {
            return new TableMetadata(tableName, columns.build());
        }
    }
}
