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

import io.airlift.log.Logger;
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
import io.cml.spi.CmlException;
import io.cml.spi.connector.Connector;
import org.apache.commons.lang3.text.StrSubstitutor;

import java.util.Map;

import static io.cml.spi.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class PgCatalogTableBuilder
{
    private static final Logger LOG = Logger.get(PgCatalogTableBuilder.class);
    private final Connector connector;
    private final Map<String, String> replaceMap;
    private final Map<Integer, String> oidToTypeMap;
    private final StrSubstitutor strSubstitutor;

    public PgCatalogTableBuilder(Connector connector)
    {
        this.connector = requireNonNull(connector, "connector is null");
        this.replaceMap = initReplaceMap();
        this.oidToTypeMap = initOidToTypeMap();
        this.strSubstitutor = new StrSubstitutor(getReplaceMap());
    }

    public void createPgTable(PgCatalogTable pgCatalogTable)
    {
        String sql;
        switch (pgCatalogTable.getName()) {
            case PgClassTable.NAME:
                sql = createPgClass(pgCatalogTable);
                break;
            case PgTypeTable.NAME:
                sql = createPgType(pgCatalogTable);
                break;
            case PgAmTable.NAME:
                sql = createPgAmTable(pgCatalogTable);
                break;
            case PgAttributeTable.NAME:
                sql = createPgAttributeTable(pgCatalogTable);
                break;
            case PgAttrdefTable.NAME:
                sql = createPgAttrdefTable(pgCatalogTable);
                break;
            case PgConstraintTable.NAME:
                sql = createPgConstraintTable(pgCatalogTable);
                break;
            case PgDatabaseTable.NAME:
                sql = createPgDatabaseTable(pgCatalogTable);
                break;
            case PgDescriptionTable.NAME:
                sql = createPgDescriptionTable(pgCatalogTable);
                break;
            case PgEnumTable.NAME:
                sql = createPgEnumTable(pgCatalogTable);
                break;
            case PgIndexTable.NAME:
                sql = createPgIndexTable(pgCatalogTable);
                break;
            case PgNamespaceTable.NAME:
                sql = createPgNamespaceTable(pgCatalogTable);
                break;
            case PgProcTable.NAME:
                sql = createPgProcTable(pgCatalogTable);
                break;
            case PgRangeTable.NAME:
                sql = createPgRangeTable(pgCatalogTable);
                break;
            case PgRolesTable.NAME:
                sql = createPgRoleTable(pgCatalogTable);
                break;
            case PgSettingsTable.NAME:
                sql = createPgSettingsTable(pgCatalogTable);
                break;
            case PgTablespaceTable.NAME:
                sql = createPgTablespaceTable(pgCatalogTable);
                break;
            case CharacterSets.NAME:
                sql = createCharacterSets(pgCatalogTable);
                break;
            case ReferentialConstraints.NAME:
                sql = createReferentialConstraints(pgCatalogTable);
                break;
            case KeyColumnUsage.NAME:
                sql = createKeyColumnUsage(pgCatalogTable);
                break;
            case TableConstraints.NAME:
                sql = createTableConstraints(pgCatalogTable);
                break;
            default:
                throw new CmlException(GENERIC_INTERNAL_ERROR, format("Unsupported table %s", pgCatalogTable.getName()));
        }

        connector.directDDL(strSubstitutor.replace(sql));
        LOG.info("pg_catalog.%s has created or updated", pgCatalogTable.getName());
    }

    protected abstract Map<String, String> initReplaceMap();

    protected abstract Map<Integer, String> initOidToTypeMap();

    public Connector getConnector()
    {
        return connector;
    }

    public Map<String, String> getReplaceMap()
    {
        return replaceMap;
    }

    public Map<Integer, String> getOidToTypeMap()
    {
        return oidToTypeMap;
    }

    protected abstract String createPgClass(PgCatalogTable pgCatalogTable);

    protected abstract String createPgType(PgCatalogTable pgCatalogTable);

    protected abstract String createPgAmTable(PgCatalogTable pgCatalogTable);

    protected abstract String createPgAttributeTable(PgCatalogTable pgCatalogTable);

    protected abstract String createPgAttrdefTable(PgCatalogTable pgCatalogTable);

    protected abstract String createPgConstraintTable(PgCatalogTable pgCatalogTable);

    protected abstract String createPgDatabaseTable(PgCatalogTable pgCatalogTable);

    protected abstract String createPgDescriptionTable(PgCatalogTable pgCatalogTable);

    protected abstract String createPgEnumTable(PgCatalogTable pgCatalogTable);

    protected abstract String createPgIndexTable(PgCatalogTable pgCatalogTable);

    protected abstract String createPgNamespaceTable(PgCatalogTable pgCatalogTable);

    protected abstract String createPgProcTable(PgCatalogTable pgCatalogTable);

    protected abstract String createPgRangeTable(PgCatalogTable pgCatalogTable);

    protected abstract String createPgRoleTable(PgCatalogTable pgCatalogTable);

    protected abstract String createPgSettingsTable(PgCatalogTable pgCatalogTable);

    protected abstract String createPgTablespaceTable(PgCatalogTable pgCatalogTable);

    protected abstract String createCharacterSets(PgCatalogTable pgCatalogTable);

    protected abstract String createReferentialConstraints(PgCatalogTable pgCatalogTable);

    protected abstract String createKeyColumnUsage(PgCatalogTable pgCatalogTable);

    protected abstract String createTableConstraints(PgCatalogTable pgCatalogTable);
}
