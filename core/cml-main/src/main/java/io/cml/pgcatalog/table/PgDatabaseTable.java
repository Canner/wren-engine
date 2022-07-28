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

import static io.cml.spi.type.BooleanType.BOOLEAN;
import static io.cml.spi.type.IntegerType.INTEGER;
import static io.cml.spi.type.PGArray.VARCHAR_ARRAY;
import static io.cml.spi.type.VarcharType.VARCHAR;

/**
 * @see <a href="https://www.postgresql.org/docs/10/catalog-pg-database.html">PostgreSQL pg_database</a>
 */
public class PgDatabaseTable
        extends PgCatalogTable
{
    public static final String NAME = "pg_database";

    @Override
    protected TableMetadata createMetadata()
    {
        return PgCatalogTableUtils.table(NAME)
                .column("oid", INTEGER, "0")
                .column("datname", VARCHAR, "${catalogName}")
                .column("datdba", INTEGER, "0")
                .column("encoding", INTEGER, "6")
                .column("datcollate", VARCHAR, "'en_US.UTF-8'")
                .column("datctype", VARCHAR, "'en_US.UTF-8'")
                .column("datistemplate", BOOLEAN, "false")
                .column("datallowconn", BOOLEAN, "true")
                .column("datconnlimit", INTEGER, "-1")
                .column("datlastsysoid", INTEGER, "0")
                .column("datfrozenxid", INTEGER, "0")
                .column("datminmxid", INTEGER, "0")
                .column("dattablespace", INTEGER, "0")
                .column("datacl", VARCHAR_ARRAY, "null")
                .build();
    }
}
