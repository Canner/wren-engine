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

import io.cml.spi.metadata.TableMetadata;

import static io.cml.pgcatalog.table.PgCatalogTableUtils.table;
import static io.cml.type.BooleanType.BOOLEAN;
import static io.cml.type.IntegerType.INTEGER;
import static io.cml.type.PGArray.INT4_ARRAY;
import static io.cml.type.PGArray.VARCHAR_ARRAY;

/**
 * this table is unused in Cannerflow, so just an empty table
 *
 * @see <a href="https://www.postgresql.org/docs/13/catalog-pg-index.html">PostgreSQL pg_index</a>
 */
public class PgIndexTable
        extends PgCatalogTable
{
    public static final String NAME = "pg_index";

    @Override
    protected TableMetadata createMetadata()
    {
        return table(NAME)
                .column("indexrelid", INTEGER)
                .column("indrelid", INTEGER)
                .column("indnatts", INTEGER)
                .column("indnkeyatts", INTEGER)
                .column("indisunique", BOOLEAN)
                .column("indisprimary", BOOLEAN)
                .column("indisexclusion", BOOLEAN)
                .column("indimmediate", BOOLEAN)
                .column("indisclustered", BOOLEAN)
                .column("indisvalid", BOOLEAN)
                .column("indcheckxmin", BOOLEAN)
                .column("indisready", BOOLEAN)
                .column("indislive", BOOLEAN)
                .column("indisreplident", BOOLEAN)
                .column("indkey", INT4_ARRAY)
                .column("indcollation", INT4_ARRAY)
                .column("indclass", INT4_ARRAY)
                .column("indoption", INT4_ARRAY)
                .column("indexprs", VARCHAR_ARRAY)
                .column("indpred", VARCHAR_ARRAY)
                .build();
    }
}
