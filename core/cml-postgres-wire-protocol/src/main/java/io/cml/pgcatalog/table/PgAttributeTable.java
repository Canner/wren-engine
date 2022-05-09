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

import io.cml.pgcatalog.PgCatalogTableManager;
import io.cml.spi.metadata.TableMetadata;

import static io.cml.pgcatalog.table.PgCatalogTableUtils.table;
import static io.cml.type.BooleanType.BOOLEAN;
import static io.cml.type.CharType.CHAR;
import static io.cml.type.IntegerType.INTEGER;
import static io.cml.type.PGArray.VARCHAR_ARRAY;
import static io.cml.type.VarcharType.VARCHAR;

/**
 * this table is unused in Cannerflow, so just an empty table
 *
 * @see <a href="https://www.postgresql.org/docs/13/catalog-pg-attribute.html">PostgreSQL pg_attribute</a>
 */
public class PgAttributeTable
        extends PgCatalogTable
{
    public static final String NAME = "pg_attribute";

    public PgAttributeTable(PgCatalogTableManager pgCatalogTableManager)
    {
        super(pgCatalogTableManager);
    }

    @Override
    protected TableMetadata createMetadata(PgCatalogTableManager pgCatalogTableManager)
    {
        return table(NAME)
                .column("attrelid", INTEGER, "${hash}(${columnName})")
                .column("attname", VARCHAR, "${columnName}")
                .column("atttypid", INTEGER, "${typeOid}")
                .column("attstattarget", INTEGER, "0")
                .column("attlen", INTEGER, "${typeLen}")
                .column("attnum", INTEGER, "${columNum}")
                .column("attndims", INTEGER, "0")
                .column("attcacheoff", INTEGER, "-1")
                .column("atttypmod", INTEGER, "-1")
                .column("attbyval", BOOLEAN, "false")
                .column("attstorage", CHAR, "'p'")
                .column("attalign", CHAR, "'c'")
                .column("attnotnull", BOOLEAN, "false")
                .column("atthasdef", BOOLEAN, "false")
                .column("atthasmissing", BOOLEAN, "false")
                .column("attidentity", CHAR, "'a'")
                .column("attgenerated", CHAR, "'s'")
                .column("attisdropped", BOOLEAN, "false")
                .column("attislocal", BOOLEAN, "false")
                .column("attinhcount", INTEGER, "0")
                .column("attcollation", INTEGER, "0")
                .column("attacl", VARCHAR_ARRAY, "null")
                .column("attoptions", VARCHAR_ARRAY, "null")
                .column("attfdwoptions", VARCHAR_ARRAY, "null")
                .column("attmissingval", VARCHAR_ARRAY, "null")
                .build();
    }
}
