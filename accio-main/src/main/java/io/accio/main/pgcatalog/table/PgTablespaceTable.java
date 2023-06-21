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

import static io.accio.base.type.IntegerType.INTEGER;
import static io.accio.base.type.PGArray.VARCHAR_ARRAY;
import static io.accio.base.type.VarcharType.VARCHAR;
import static io.accio.main.pgcatalog.table.PgCatalogTableUtils.table;

/**
 * this table is unused in Cannerflow, so just an empty table
 *
 * @see <a href="https://postgresql.org/docs/13/catalog-pg-tablespace.html">PostgreSQL pg_tablespace</a>
 */
public class PgTablespaceTable
        extends PgCatalogTable
{
    public static final String NAME = "pg_tablespace";

    @Override
    protected TableMetadata createMetadata()
    {
        return table(NAME)
                .column("oid", INTEGER)
                .column("spcname", VARCHAR)
                .column("spcowner", INTEGER)
                .column("spcacl", VARCHAR_ARRAY)
                .column("spcoptions", VARCHAR_ARRAY)
                .build();
    }
}
