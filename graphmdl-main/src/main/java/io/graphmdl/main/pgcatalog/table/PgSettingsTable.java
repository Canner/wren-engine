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
package io.graphmdl.main.pgcatalog.table;

import io.graphmdl.base.metadata.TableMetadata;

import static io.graphmdl.base.type.BooleanType.BOOLEAN;
import static io.graphmdl.base.type.IntegerType.INTEGER;
import static io.graphmdl.base.type.PGArray.VARCHAR_ARRAY;
import static io.graphmdl.base.type.VarcharType.VARCHAR;

/**
 * this table is unused in Cannerflow, so just an empty table
 *
 * @see <a href="https://www.postgresql.org/docs/13/view-pg-settings.html">PostgreSQL pg_settings</a>
 */
public class PgSettingsTable
        extends PgCatalogTable
{
    public static final String NAME = "pg_settings";

    @Override
    protected TableMetadata createMetadata()
    {
        return PgCatalogTableUtils.table(NAME)
                .column("name", VARCHAR)
                .column("setting", VARCHAR)
                .column("unit", VARCHAR)
                .column("category", VARCHAR)
                .column("short_desc", VARCHAR)
                .column("extra_desc", VARCHAR)
                .column("context", VARCHAR)
                .column("vartype", VARCHAR)
                .column("source", VARCHAR)
                .column("min_val", VARCHAR)
                .column("max_val", VARCHAR)
                .column("enumvals", VARCHAR_ARRAY)
                .column("boot_val", VARCHAR)
                .column("reset_val", VARCHAR)
                .column("sourcefile", VARCHAR)
                .column("sourceline", INTEGER)
                .column("pending_restart", BOOLEAN)
                .build();
    }
}
