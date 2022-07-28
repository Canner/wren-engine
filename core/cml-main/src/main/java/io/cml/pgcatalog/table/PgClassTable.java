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
import io.cml.spi.type.BooleanType;

import static io.cml.pgcatalog.table.PgCatalogTableUtils.table;
import static io.cml.spi.type.BooleanType.BOOLEAN;
import static io.cml.spi.type.DoubleType.DOUBLE;
import static io.cml.spi.type.IntegerType.INTEGER;
import static io.cml.spi.type.PGArray.VARCHAR_ARRAY;
import static io.cml.spi.type.VarcharType.VARCHAR;

/**
 * @see <a href="https://www.postgresql.org/docs/13/catalog-pg-class.html">PostgreSQL pg_class</a>
 */
public class PgClassTable
        extends PgCatalogTable
{
    public static final String NAME = "pg_class";

    @Override
    protected TableMetadata createMetadata()
    {
        return table(NAME)
                .column("oid", INTEGER, "${hash}(${tableName})")
                .column("relname", VARCHAR, "${tableName}")
                .column("relnamespace", INTEGER, "${hash}(${schemaName})")
                .column("reltype", INTEGER, "0")
                .column("reloftype", INTEGER, "0")
                .column("relowner", INTEGER, "0")
                .column("relam", INTEGER, "0")
                .column("relfilenode", INTEGER, "0")
                .column("reltablespace", INTEGER, "0")
                .column("relpages", INTEGER, "0")
                .column("reltuples", DOUBLE, "0")
                .column("relallvisible", INTEGER, "0")
                .column("reltoastrelid", INTEGER, "0")
                .column("relhasindex", BOOLEAN, "false")
                .column("relisshared", BOOLEAN, "false")
                .column("relpersistence", VARCHAR, "'p'")
                .column("relkind", VARCHAR, "'r'")
                .column("relnatts", INTEGER, "0")
                .column("relchecks", INTEGER, "0")
                .column("relhasrules", BOOLEAN, "false")
                .column("relhastriggers", BOOLEAN, "false")
                .column("relhassubclass", BOOLEAN, "false")
                .column("relrowsecurity", BOOLEAN, "false")
                .column("relforcerowsecurity", BOOLEAN, "false")
                .column("relispopulated", BOOLEAN, "false")
                .column("relreplident", VARCHAR, "'n'")
                .column("relispartition", BOOLEAN, "false")
                .column("relrewrite", INTEGER, "0")
                .column("relfrozenxid", INTEGER, "0")
                .column("relminmxid", INTEGER, "0")
                .column("relacl", VARCHAR_ARRAY, "null")
                .column("reloptions", VARCHAR_ARRAY, "null")
                .column("relpartbound", VARCHAR_ARRAY, "null")
                .column("relhasoids", BooleanType.BOOLEAN, "false")
                .build();
    }
}
