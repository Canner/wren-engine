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
package io.graphmdl.main.pgcatalog.table;

import com.google.common.collect.ImmutableMap;
import io.graphmdl.base.metadata.TableMetadata;

import java.util.Map;

import static io.graphmdl.base.type.BooleanType.BOOLEAN;
import static io.graphmdl.base.type.CharType.CHAR;
import static io.graphmdl.base.type.IntegerType.INTEGER;
import static io.graphmdl.base.type.PGArray.VARCHAR_ARRAY;
import static io.graphmdl.base.type.VarcharType.VARCHAR;
import static io.graphmdl.main.pgcatalog.table.PgCatalogTableUtils.table;

/**
 * this table is unused in Cannerflow, so just an empty table
 *
 * @see <a href="https://www.postgresql.org/docs/13/catalog-pg-attribute.html">PostgreSQL pg_attribute</a>
 */
public class PgAttributeTable
        extends PgCatalogTable
{
    public static final String NAME = "pg_attribute";

    @Override
    protected TableMetadata createMetadata()
    {
        return table(NAME)
                .column("attrelid", INTEGER)
                .column("attname", VARCHAR)
                .column("atttypid", INTEGER)
                .column("attstattarget", INTEGER)
                .column("attlen", INTEGER)
                .column("attnum", INTEGER)
                .column("attndims", INTEGER)
                .column("attcacheoff", INTEGER)
                .column("atttypmod", INTEGER)
                .column("attbyval", BOOLEAN)
                .column("attstorage", CHAR)
                .column("attalign", CHAR)
                .column("attnotnull", BOOLEAN)
                .column("atthasdef", BOOLEAN)
                .column("atthasmissing", BOOLEAN)
                .column("attidentity", CHAR)
                .column("attgenerated", CHAR)
                .column("attisdropped", BOOLEAN)
                .column("attislocal", BOOLEAN)
                .column("attinhcount", INTEGER)
                .column("attcollation", INTEGER)
                .column("attacl", VARCHAR_ARRAY)
                .column("attoptions", VARCHAR_ARRAY)
                .column("attfdwoptions", VARCHAR_ARRAY)
                .column("attmissingval", VARCHAR_ARRAY)
                .build();
    }

    @Override
    protected Map<String, String> createTableContent()
    {
        return ImmutableMap.<String, String>builder()
                .put("attrelid", "${hash}(${tableName})")
                .put("attname", "${columnName}")
                .put("atttypid", "${typeOid}")
                .put("attstattarget", "0")
                .put("attlen", "${typeLen}")
                .put("attnum", "${columNum}")
                .put("attndims", "0")
                .put("attcacheoff", "-1")
                .put("atttypmod", "-1")
                .put("attbyval", "false")
                .put("attstorage", "'p'")
                .put("attalign", "'c'")
                .put("attnotnull", "false")
                .put("atthasdef", "false")
                .put("atthasmissing", "false")
                .put("attidentity", "'a'")
                .put("attgenerated", "'s'")
                .put("attisdropped", "false")
                .put("attislocal", "false")
                .put("attinhcount", "0")
                .put("attcollation", "0")
                .put("attacl", "null")
                .put("attoptions", "null")
                .put("attfdwoptions", "null")
                .put("attmissingval", "null")
                .build();
    }
}
