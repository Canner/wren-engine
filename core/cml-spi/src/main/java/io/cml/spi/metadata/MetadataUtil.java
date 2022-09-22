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

package io.cml.spi.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cml.spi.type.PGType;

import java.util.Optional;

public class MetadataUtil
{
    public static class TableMetadataBuilder
    {
        public static TableMetadataBuilder tableMetadataBuilder(SchemaTableName tableName)
        {
            return new TableMetadataBuilder(tableName);
        }

        private final SchemaTableName tableName;
        private final ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        private final ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        private final Optional<String> comment;

        private TableMetadataBuilder(SchemaTableName tableName)
        {
            this(tableName, Optional.empty());
        }

        private TableMetadataBuilder(SchemaTableName tableName, Optional<String> comment)
        {
            this.tableName = tableName;
            this.comment = comment;
        }

        public TableMetadataBuilder column(String columnName, PGType<?> type)
        {
            columns.add(ColumnMetadata.builder()
                    .setName(columnName)
                    .setType(type).build());
            return this;
        }

        public TableMetadataBuilder hiddenColumn(String columnName, PGType<?> type)
        {
            columns.add(ColumnMetadata.builder()
                    .setName(columnName)
                    .setType(type)
                    .setHidden(true)
                    .build());
            return this;
        }

        public TableMetadataBuilder property(String name, Object value)
        {
            properties.put(name, value);
            return this;
        }

        public TableMetadata build()
        {
            return new TableMetadata(tableName, columns.build(), properties.build(), comment);
        }
    }
}
