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

package io.cml.pgcatalog.builder;

import com.google.common.collect.Streams;
import io.cml.pgcatalog.table.PgCatalogTable;
import io.cml.spi.type.PGTypes;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.cml.pgcatalog.table.PgCatalogTableUtils.DEFAULT_AUTH;
import static io.cml.pgcatalog.table.PgCatalogTableUtils.PG_CATALOG;
import static java.lang.String.format;

public final class PgCatalogTableBuilderUtils
{
    private PgCatalogTableBuilderUtils() {}

    public static List<Object[]> generatePgTypeRecords(PgCatalogTable pgCatalogTable, Map<Integer, String> oidToTypeMap)
    {
        return Streams.stream(PGTypes.pgTypes())
                .map(type -> {
                    Object[] record = new Object[pgCatalogTable.getTableMetadata().getColumns().size()];
                    record[0] = type.oid(); // oid
                    record[1] = type.typName(); // typname
                    record[2] = withHash(PG_CATALOG); // typnamespace
                    record[3] = withHash(DEFAULT_AUTH); // typowner
                    record[4] = type.typeLen(); // typlen
                    record[5] = true; // typbyval
                    record[6] = type.type(); // typtype
                    record[7] = type.typeCategory(); // typcategory
                    record[8] = true; // typisddefined
                    record[9] = type.typDelim(); // typdelim
                    record[10] = 0; // typrelid
                    record[11] = type.typElem(); // typelem
                    record[12] = type.typArray(); // typarray
                    record[13] = type.typInput(); // typinput
                    record[14] = type.typOutput(); // typoutput
                    record[15] = type.typReceive(); // typreceive
                    record[16] = false; // typnotnull
                    record[17] = 0; // typbasetype
                    record[18] = -1; // typtypmod
                    record[19] = 0; // typndims
                    record[20] = null; // typdefault
                    record[21] = false; // typispreferrd
                    record[22] = null; // typsend
                    record[23] = null; // typstorage
                    record[24] = null; // typdefaultbin
                    record[25] = oidToTypeMap.get(type.oid()); //remotetype
                    return record;
                }).collect(toImmutableList());
    }

    private static String withHash(String key)
    {
        return format("${hash}('%s')", key);
    }
}
