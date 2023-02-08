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
package io.graphmdl.pgcatalog.table;

import io.graphmdl.spi.metadata.TableMetadata;

import static io.graphmdl.pgcatalog.table.PgCatalogTableUtils.table;
import static io.graphmdl.spi.type.IntegerType.INTEGER;
import static io.graphmdl.spi.type.VarcharType.VARCHAR;

/**
 * this table is unused in Cannerflow, so just an empty table
 *
 * @see <a href="https://www.postgresql.org/docs/13/catalog-pg-range.html">PostgreSQL pg_range</a>
 */
public class PgRangeTable
        extends PgCatalogTable
{
    public static final String NAME = "pg_range";

    @Override
    protected TableMetadata createMetadata()
    {
        return table(NAME)
                .column("rngtypid", INTEGER)
                .column("rngsubtype", INTEGER)
                .column("rngcollation", INTEGER)
                .column("rngsubopc", INTEGER)
                .column("rngcanonical", VARCHAR)
                .column("rngsubdiff", VARCHAR)
                .build();
    }
}
