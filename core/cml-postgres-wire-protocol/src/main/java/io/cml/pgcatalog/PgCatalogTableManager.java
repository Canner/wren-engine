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
package io.cml.pgcatalog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.cml.pgcatalog.builder.PgCatalogBuilder;
import io.cml.pgcatalog.table.CharacterSets;
import io.cml.pgcatalog.table.KeyColumnUsage;
import io.cml.pgcatalog.table.PgAmTable;
import io.cml.pgcatalog.table.PgAttrdefTable;
import io.cml.pgcatalog.table.PgAttributeTable;
import io.cml.pgcatalog.table.PgCatalogTable;
import io.cml.pgcatalog.table.PgClassTable;
import io.cml.pgcatalog.table.PgConstraintTable;
import io.cml.pgcatalog.table.PgDatabaseTable;
import io.cml.pgcatalog.table.PgDescriptionTable;
import io.cml.pgcatalog.table.PgEnumTable;
import io.cml.pgcatalog.table.PgIndexTable;
import io.cml.pgcatalog.table.PgNamespaceTable;
import io.cml.pgcatalog.table.PgProcTable;
import io.cml.pgcatalog.table.PgRangeTable;
import io.cml.pgcatalog.table.PgRolesTable;
import io.cml.pgcatalog.table.PgSettingsTable;
import io.cml.pgcatalog.table.PgTablespaceTable;
import io.cml.pgcatalog.table.PgTypeTable;
import io.cml.pgcatalog.table.ReferentialConstraints;
import io.cml.pgcatalog.table.TableConstraints;
import io.cml.spi.connector.Connector;
import io.cml.spi.metadata.TableMetadata;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.cml.pgcatalog.PgCatalogUtils.CML_TEMP_NAME;
import static io.cml.pgcatalog.PgCatalogUtils.PG_CATALOG_NAME;
import static java.util.Objects.requireNonNull;

public class PgCatalogTableManager
{
    private final Map<String, PgCatalogTable> tables;
    private final Connector connector;
    private final PgCatalogBuilder pgCatalogBuilder;

    private final List<String> highPriorityTableName = ImmutableList.of(PgTypeTable.NAME);

    @Inject
    public PgCatalogTableManager(Connector connector, PgCatalogBuilder pgCatalogBuilder)
    {
        this.tables = initTables();
        this.connector = requireNonNull(connector, "connector is null");
        this.pgCatalogBuilder = requireNonNull(pgCatalogBuilder, "pgCatalogBuilder is null");
    }

    private Map<String, PgCatalogTable> initTables()
    {
        return ImmutableMap.<String, PgCatalogTable>builder()
                .put(PgAmTable.NAME, new PgAmTable(this))
                .put(PgAttrdefTable.NAME, new PgAttrdefTable(this))
                .put(PgAttributeTable.NAME, new PgAttributeTable(this))
                .put(PgClassTable.NAME, new PgClassTable(this))
                .put(PgConstraintTable.NAME, new PgConstraintTable(this))
                .put(PgDatabaseTable.NAME, new PgDatabaseTable(this))
                .put(PgDescriptionTable.NAME, new PgDescriptionTable(this))
                .put(PgEnumTable.NAME, new PgEnumTable(this))
                .put(PgIndexTable.NAME, new PgIndexTable(this))
                .put(PgNamespaceTable.NAME, new PgNamespaceTable(this))
                .put(PgProcTable.NAME, new PgProcTable(this))
                .put(PgRangeTable.NAME, new PgRangeTable(this))
                .put(PgRolesTable.NAME, new PgRolesTable(this))
                .put(PgSettingsTable.NAME, new PgSettingsTable(this))
                .put(PgTablespaceTable.NAME, new PgTablespaceTable(this))
                .put(PgTypeTable.NAME, new PgTypeTable(this))
                .put(CharacterSets.NAME, new CharacterSets(this))
                .put(ReferentialConstraints.NAME, new ReferentialConstraints(this))
                .put(KeyColumnUsage.NAME, new KeyColumnUsage(this))
                .put(TableConstraints.NAME, new TableConstraints(this))
                .build();
    }

    public Connector getConnector()
    {
        return this.connector;
    }

    public void initPgCatalog()
    {
        createCatalogIfNotExist(PG_CATALOG_NAME);
        if (!isPgCatalogValid()) {
            createCatalogIfNotExist(CML_TEMP_NAME);

            // Some table has dependency with the high priority table.
            // Create them first.
            for (String tableName : highPriorityTableName) {
                createPgCatalogTable(tables.get(tableName));
            }

            List<PgCatalogTable> lowPriorityTable = tables.values().stream()
                    .filter(pgCatalogTable -> !highPriorityTableName.contains(pgCatalogTable.getName()))
                    .collect(toImmutableList());

            for (PgCatalogTable pgCatalogTable : lowPriorityTable) {
                createPgCatalogTable(pgCatalogTable);
            }
        }
    }

    private void createCatalogIfNotExist(String name)
    {
        if (!connector.isSchemaExist(name)) {
            connector.createSchema(name);
        }
    }

    private boolean isPgCatalogValid()
    {
        List<TableMetadata> remoteTables = connector.listTables(PG_CATALOG_NAME);
        return remoteTables.size() == tables.values().size();
    }

    private void createPgCatalogTable(PgCatalogTable pgCatalogTable)
    {
        pgCatalogBuilder.createPgTable(pgCatalogTable);
    }
}
