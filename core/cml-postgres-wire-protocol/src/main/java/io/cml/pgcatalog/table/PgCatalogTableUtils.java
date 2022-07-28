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

package io.cml.pgcatalog.table;

import io.cml.spi.metadata.MetadataUtil;
import io.cml.spi.metadata.SchemaTableName;

import static io.cml.spi.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;

public final class PgCatalogTableUtils
{
    public static final String PG_CATALOG = "pg_catalog";
    public static final String DEFAULT_SCHEMA = "default";
    public static final String INFORMATION_SCHEMA = "information_schema";
    public static final String DEFAULT_AUTH = "cml";

    public static final String INTERNAL_LANGUAGE = "internal";

    private PgCatalogTableUtils() {}

    public static MetadataUtil.TableMetadataBuilder table(String tableName)
    {
        return table(DEFAULT_SCHEMA, tableName);
    }

    public static MetadataUtil.TableMetadataBuilder table(String schema, String tableName)
    {
        return tableMetadataBuilder(new SchemaTableName(schema, tableName));
    }
}
