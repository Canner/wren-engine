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

import static io.accio.base.type.IntegerType.INTEGER;
import static io.accio.base.type.PGArray.VARCHAR_ARRAY;
import static io.accio.base.type.VarcharType.VARCHAR;
import static io.accio.main.pgcatalog.table.PgCatalogTableUtils.table;

/**
 * @see <a href="https://www.postgresql.org/docs/13/catalog-pg-namespace.html">PostgreSQL pg_namespace</a>
 */
public class PgNamespaceTable
        extends PgCatalogTable
{
    public static final String NAME = "pg_namespace";

    @Override
    protected TableMetadata createMetadata()
    {
        return table(NAME)
                .column("oid", INTEGER)
                .column("nspname", VARCHAR)
                .column("nspowner", INTEGER)
                .column("nspacl", VARCHAR_ARRAY)
                .build();
    }

    @Override
    protected Map<String, String> createTableContent()
    {
        return ImmutableMap.<String, String>builder()
                .put("oid", "${hash}(${schemaName})")
                .put("nspname", "${schemaName}")
                .put("nspowner", "0")
                .put("nspacl", "null")
                .build();
    }
}
