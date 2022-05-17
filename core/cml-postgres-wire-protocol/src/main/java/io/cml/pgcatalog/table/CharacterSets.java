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

import static io.cml.pgcatalog.table.PgCatalogTableUtils.table;
import static io.cml.type.VarcharType.VARCHAR;

public class CharacterSets
        extends PgCatalogTable
{
    public static final String NAME = "character_sets";

    @Override
    protected TableMetadata createMetadata()
    {
        return table(NAME)
                .column("character_set_catalog", VARCHAR, "null")
                .column("character_set_schema", VARCHAR, "null")
                .column("character_set_name", VARCHAR, "'UTF8'")
                .column("character_repertoire", VARCHAR, "'UCS'")
                .column("form_of_use", VARCHAR, "'UTF8'")
                .column("default_collate_catalog", VARCHAR, "${catalogName}")
                .column("default_collate_schema", VARCHAR, "'default'")
                .column("default_collate_name", VARCHAR, "'use_basic'")
                .build();
    }
}
