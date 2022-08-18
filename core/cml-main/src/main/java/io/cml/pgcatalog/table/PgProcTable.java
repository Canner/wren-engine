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

import static io.cml.pgcatalog.table.PgCatalogTableUtils.DEFAULT_AUTH;
import static io.cml.pgcatalog.table.PgCatalogTableUtils.INTERNAL_LANGUAGE;
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
                .column("oid", INTEGER, "${hash}(concat('PROC', ${functionName}))")
                .column("proname", VARCHAR, "${split}(${functionName}, '__')${firstOrdinal}")
                .column("pronamespace", INTEGER, "${hash}(${functionSchema})")
                .column("proowner", INTEGER, "${hash}('" + DEFAULT_AUTH + "')")
                .column("prolang", INTEGER, "${hash}('" + INTERNAL_LANGUAGE + "')")
                .column("procost", DOUBLE, "1")
                .column("prorows", DOUBLE, "0")
                .column("provariadic", INTEGER, "0")
                .column("prosupport", REGPROC, "null")
                .column("prokind", CHAR, "'f'")
                .column("prosecdef", BOOLEAN, "false")
                .column("proleakproof", BOOLEAN, "false")
                .column("proisstrict", BOOLEAN, "false")
                .column("proretset", BOOLEAN, "false")
                .column("provolatile", CHAR, "'i'")
                .column("proparallel", CHAR, "'u'")
                .column("pronargs", INTEGER, "0")
                .column("pronargdefaults", INTEGER, "0")
                .column("prorettype", INTEGER, "null")
                .column("proargtypes", INT4_ARRAY, "null")
                .column("proallargtypes", INT4_ARRAY, "null")
                .column("proargmodes", CHAR_ARRAY, "null")
                .column("proargnames", VARCHAR_ARRAY, "null")
                .column("proargdefaults", VARCHAR_ARRAY, "null")
                .column("protrftypes", INT4_ARRAY, "null")
                .column("prosrc", VARCHAR_ARRAY, "null")
                .column("probin", VARCHAR_ARRAY, "null")
                .column("proconfig", VARCHAR_ARRAY, "null")
                .column("proacl", VARCHAR_ARRAY, "null")
                .column("remotename", VARCHAR, "${functionName}")
                .build();
    }
}
