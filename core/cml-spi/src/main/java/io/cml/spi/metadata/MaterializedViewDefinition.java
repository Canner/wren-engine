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

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class MaterializedViewDefinition
{
    private final CatalogName catalogName;
    private final SchemaTableName schemaTableName;

    private final String originalSql;

    private final List<ColumnMetadata> columns;

    public MaterializedViewDefinition(CatalogName catalogName, SchemaTableName tableName, String originalSql, List<ColumnMetadata> columns)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.schemaTableName = requireNonNull(tableName, "schemaTableName is null");
        this.originalSql = requireNonNull(originalSql, "originalSql is null");
        this.columns = requireNonNull(columns, "columns is null");
    }

    public CatalogName getCatalogName()
    {
        return catalogName;
    }

    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    public String getOriginalSql()
    {
        return originalSql;
    }

    public List<ColumnMetadata> getColumns()
    {
        return columns;
    }

    public String toString()
    {
        return toStringHelper(this)
                .add("catalogName", catalogName)
                .add("schemaTableName", schemaTableName)
                .add("originalSql", originalSql)
                .toString();
    }
}
