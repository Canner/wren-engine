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

package io.accio.main.pgcatalog.table;

import io.accio.base.metadata.TableMetadata;

import static io.accio.base.type.VarcharType.VARCHAR;

public class KeyColumnUsage
        extends PgCatalogTable
{
    public static final String NAME = "key_column_usage";

    @Override
    protected TableMetadata createMetadata()
    {
        return PgCatalogTableUtils.table(NAME)
                .column("constraint_catalog", VARCHAR)
                .column("constraint_schema", VARCHAR)
                .column("constraint_name", VARCHAR)
                .column("table_catalog", VARCHAR)
                .column("table_schema", VARCHAR)
                .column("table_name", VARCHAR)
                .column("column_name", VARCHAR)
                .column("ordinal_position", VARCHAR)
                .column("position_in_unique_constraint", VARCHAR)
                .build();
    }
}
