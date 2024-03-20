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

package io.wren.main.pgcatalog.table;

import io.wren.base.metadata.TableMetadata;

import static io.wren.base.type.VarcharType.VARCHAR;

public class TableConstraints
        extends PgCatalogTable
{
    public static final String NAME = "table_constraints";

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
                .column("constraint_type", VARCHAR)
                .column("is_deferrable", VARCHAR)
                .column("initially_deferred", VARCHAR)
                .column("enforced", VARCHAR)
                .build();
    }
}
