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
import static io.cml.spi.type.BooleanType.BOOLEAN;
import static io.cml.spi.type.CharType.CHAR;
import static io.cml.spi.type.DoubleType.DOUBLE;
import static io.cml.spi.type.IntegerType.INTEGER;
import static io.cml.spi.type.PGArray.CHAR_ARRAY;
import static io.cml.spi.type.PGArray.INT4_ARRAY;
import static io.cml.spi.type.PGArray.VARCHAR_ARRAY;
import static io.cml.spi.type.RegprocType.REGPROC;
import static io.cml.spi.type.VarcharType.VARCHAR;

/**
 * @see <a href="https://www.postgresql.org/docs/13/catalog-pg-proc.html">PostgreSQL pg_proc</a>
 */
public class PgProcTable
        extends PgCatalogTable
{
    public static final String NAME = "pg_proc";

    @Override
    protected TableMetadata createMetadata()
    {
        return table(NAME)
                .column("oid", INTEGER)
                .column("proname", VARCHAR)
                .column("pronamespace", INTEGER)
                .column("proowner", INTEGER)
                .column("prolang", INTEGER)
                .column("procost", DOUBLE)
                .column("prorows", DOUBLE)
                .column("provariadic", INTEGER)
                .column("prosupport", REGPROC)
                .column("prokind", CHAR)
                .column("prosecdef", BOOLEAN)
                .column("proleakproof", BOOLEAN)
                .column("proisstrict", BOOLEAN)
                .column("proretset", BOOLEAN)
                .column("provolatile", CHAR)
                .column("proparallel", CHAR)
                .column("pronargs", INTEGER)
                .column("pronargdefaults", INTEGER)
                .column("prorettype", INTEGER)
                .column("proargtypes", INT4_ARRAY)
                .column("proallargtypes", INT4_ARRAY)
                .column("proargmodes", CHAR_ARRAY)
                .column("proargnames", VARCHAR_ARRAY)
                .column("proargdefaults", VARCHAR_ARRAY)
                .column("protrftypes", INT4_ARRAY)
                .column("prosrc", VARCHAR_ARRAY)
                .column("probin", VARCHAR_ARRAY)
                .column("proconfig", VARCHAR_ARRAY)
                .column("proacl", VARCHAR_ARRAY)
                .build();
    }
}
