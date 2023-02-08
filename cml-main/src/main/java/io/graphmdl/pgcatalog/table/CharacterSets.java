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

package io.graphmdl.pgcatalog.table;

import com.google.common.collect.ImmutableMap;
import io.graphmdl.spi.metadata.TableMetadata;

import java.util.Map;

import static io.graphmdl.pgcatalog.table.PgCatalogTableUtils.table;
import static io.graphmdl.spi.type.VarcharType.VARCHAR;

public class CharacterSets
        extends PgCatalogTable
{
    public static final String NAME = "character_sets";

    @Override
    protected TableMetadata createMetadata()
    {
        return table(NAME)
                .column("character_set_catalog", VARCHAR)
                .column("character_set_schema", VARCHAR)
                .column("character_set_name", VARCHAR)
                .column("character_repertoire", VARCHAR)
                .column("form_of_use", VARCHAR)
                .column("default_collate_catalog", VARCHAR)
                .column("default_collate_schema", VARCHAR)
                .column("default_collate_name", VARCHAR)
                .build();
    }

    @Override
    protected Map<String, String> createTableContent()
    {
        return ImmutableMap.<String, String>builder()
                .put("character_set_catalog", "null")
                .put("character_set_schema", "null")
                .put("character_set_name", "'UTF8'")
                .put("character_repertoire", "'UCS'")
                .put("form_of_use", "'UTF8'")
                .put("default_collate_catalog", "${catalogName}")
                .put("default_collate_schema", "'default'")
                .put("default_collate_name", "'use_basic'")
                .build();
    }
}
