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

import com.google.common.collect.ImmutableMap;
import io.accio.base.metadata.TableMetadata;

import java.util.Map;

import static io.accio.base.type.BooleanType.BOOLEAN;
import static io.accio.base.type.IntegerType.INTEGER;
import static io.accio.base.type.PGArray.VARCHAR_ARRAY;
import static io.accio.base.type.VarcharType.VARCHAR;

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
                .column("oid", INTEGER)
                .column("datname", VARCHAR)
                .column("datdba", INTEGER)
                .column("encoding", INTEGER)
                .column("datcollate", VARCHAR)
                .column("datctype", VARCHAR)
                .column("datistemplate", BOOLEAN)
                .column("datallowconn", BOOLEAN)
                .column("datconnlimit", INTEGER)
                .column("datlastsysoid", INTEGER)
                .column("datfrozenxid", INTEGER)
                .column("datminmxid", INTEGER)
                .column("dattablespace", INTEGER)
                .column("datacl", VARCHAR_ARRAY)
                .build();
    }

    @Override
    protected Map<String, String> createTableContent()
    {
        return ImmutableMap.<String, String>builder()
                .put("oid", "0")
                .put("datname", "${catalogName}")
                .put("datdba", "0")
                .put("encoding", "6")
                .put("datcollate", "'en_US.UTF-8'")
                .put("datctype", "'en_US.UTF-8'")
                .put("datistemplate", "false")
                .put("datallowconn", "true")
                .put("datconnlimit", "-1")
                .put("datlastsysoid", "0")
                .put("datfrozenxid", "0")
                .put("datminmxid", "0")
                .put("dattablespace", "0")
                .put("datacl", "null")
                .build();
    }
}
