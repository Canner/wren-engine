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
import static io.accio.main.pgcatalog.table.PgCatalogTableUtils.table;

public class ReferentialConstraints
        extends PgCatalogTable
{
    public static final String NAME = "referential_constraints";

    @Override
    protected TableMetadata createMetadata()
    {
        return table(NAME)
                .column("constraint_catalog", VARCHAR)
                .column("constraint_schema", VARCHAR)
                .column("constraint_name", VARCHAR)
                .column("unique_constraint_catalog", VARCHAR)
                .column("unique_constraint_schema", VARCHAR)
                .column("unique_constraint_name", VARCHAR)
                .column("match_option", VARCHAR)
                .column("update_rule", VARCHAR)
                .column("delete_rule", VARCHAR)
                .build();
    }
}
