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
import static io.cml.type.CharType.CHAR;
import static io.cml.type.IntegerType.INTEGER;
import static io.cml.type.PGArray.INT4_ARRAY;
import static io.cml.type.VarcharType.VARCHAR;

/**
 * this table is unused in Cannerflow, so just an empty table
 *
 * @see <a href="https://www.postgresql.org/docs/13/catalog-pg-constraint.html">PostgreSQL pg_constraint</a>
 */
public class PgConstraintTable
        extends PgCatalogTable
{
    public static final String NAME = "pg_constraint";

    @Override
    protected TableMetadata createMetadata()
    {
        return table(NAME)
                .column("oid", INTEGER)
                .column("conname", VARCHAR)
                .column("connamespace", INTEGER)
                .column("contype", CHAR)
                .column("condeferrable", BOOLEAN)
                .column("condeferred", BOOLEAN)
                .column("convalidated", BOOLEAN)
                .column("conrelid", INTEGER)
                .column("contypid", INTEGER)
                .column("conindid", INTEGER)
                .column("conparentid", INTEGER)
                .column("confrelid", INTEGER)
                .column("confupdtype", CHAR)
                .column("confdeltype", CHAR)
                .column("confmatchtype", CHAR)
                .column("conislocal", BOOLEAN)
                .column("coninhcount", INTEGER)
                .column("connoinherit", BOOLEAN)
                .column("conkey", INT4_ARRAY)
                .column("confkey", INT4_ARRAY)
                .column("conpfeqop", INT4_ARRAY)
                .column("conppeqop", INT4_ARRAY)
                .column("conffeqop", INT4_ARRAY)
                .column("conexclop", INT4_ARRAY)
                .column("conbin", VARCHAR)
                .build();
    }
}
