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

import static io.accio.base.type.BooleanType.BOOLEAN;
import static io.accio.base.type.CharType.CHAR;
import static io.accio.base.type.IntegerType.INTEGER;
import static io.accio.base.type.RegprocType.REGPROC;
import static io.accio.base.type.VarcharType.VARCHAR;
import static io.accio.main.pgcatalog.table.PgCatalogTableUtils.table;

/**
 * @see <a href="https://www.postgresql.org/docs/8.4/catalog-pg-type.html#CATALOG-TYPCATEGORY-TABLE">PostgreSQL pg_type</a>
 */
public class PgTypeTable
        extends PgCatalogTable
{
    public static final String NAME = "pg_type";

    @Override
    protected TableMetadata createMetadata()
    {
        return table(NAME)
                .column("oid", INTEGER)
                .column("typname", VARCHAR)
                .column("typnamespace", INTEGER)
                .column("typowner", INTEGER)
                .column("typlen", INTEGER)
                .column("typbyval", BOOLEAN)
                .column("typtype", CHAR)
                .column("typcategory", CHAR)
                .column("typisdefined", BOOLEAN)
                .column("typdelim", CHAR)
                .column("typrelid", INTEGER)
                .column("typelem", INTEGER)
                .column("typarray", INTEGER)
                .column("typinput", REGPROC)
                .column("typoutput", REGPROC)
                .column("typreceive", REGPROC)
                .column("typnotnull", BOOLEAN)
                .column("typbasetype", INTEGER)
                .column("typtypmod", INTEGER)
                .column("typndims", INTEGER)
                .column("typdefault", VARCHAR)
                .column("typispreferred", BOOLEAN)
                .column("typsend", VARCHAR)
                .column("typstorage", VARCHAR)
                .column("typdefaultbin", VARCHAR)
                .column("remotetype", VARCHAR)  // for internal pg_catalog table to map to remote database type
                .build();
    }
}
