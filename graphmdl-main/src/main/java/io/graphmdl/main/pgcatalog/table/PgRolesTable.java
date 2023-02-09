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

import io.graphmdl.spi.metadata.TableMetadata;

import static io.graphmdl.spi.type.BooleanType.BOOLEAN;
import static io.graphmdl.spi.type.IntegerType.INTEGER;
import static io.graphmdl.spi.type.PGArray.VARCHAR_ARRAY;
import static io.graphmdl.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIMEZONE;
import static io.graphmdl.spi.type.VarcharType.VARCHAR;

/**
 * this table is unused in Cannerflow, so just an empty table
 *
 * @see <a href="https://www.postgresql.org/docs/13/catalog-pg-range.html">PostgreSQL pg_roles</a>
 */
public class PgRolesTable
        extends PgCatalogTable
{
    public static final String NAME = "pg_roles";

    @Override
    protected TableMetadata createMetadata()
    {
        return PgCatalogTableUtils.table(NAME)
                .column("rolname", VARCHAR)
                .column("rolsuper", BOOLEAN)
                .column("rolinherit", BOOLEAN)
                .column("rolcreaterole", BOOLEAN)
                .column("rolcreatedb", BOOLEAN)
                .column("rolcanlogin", BOOLEAN)
                .column("rolreplication", BOOLEAN)
                .column("rolconnlimit", INTEGER)
                .column("rolpassword", VARCHAR)
                .column("rolvaliduntil", TIMESTAMP_WITH_TIMEZONE)
                .column("rolbypassrls", BOOLEAN)
                .column("rolconfig", VARCHAR_ARRAY)
                .column("oid", INTEGER)
                .build();
    }
}
