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

package io.accio.main.pgcatalog.builder;

import com.google.common.collect.ImmutableMap;
import io.accio.base.AccioException;
import io.accio.main.AccioMetastore;
import io.accio.main.metadata.Metadata;
import io.accio.main.pgcatalog.table.PgCatalogTable;

import javax.inject.Inject;

import java.util.Map;

import static io.accio.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class PostgresPgCatalogTableBuilder
        extends PgCatalogTableBuilder
{
    @Inject
    public PostgresPgCatalogTableBuilder(Metadata metadata, AccioMetastore accioMetastore)
    {
        super(metadata, accioMetastore);
    }

    @Override
    protected Map<String, String> initReplaceMap()
    {
        return ImmutableMap.of();
    }

    @Override
    protected String createPgClass(PgCatalogTable pgCatalogTable)
    {
        throw new AccioException(GENERIC_INTERNAL_ERROR, "Postgres no need to invoke this method");
    }

    @Override
    protected String createPgType(PgCatalogTable pgCatalogTable)
    {
        throw new AccioException(GENERIC_INTERNAL_ERROR, "Postgres no need to invoke this method");
    }

    @Override
    protected String createPgAmTable(PgCatalogTable pgCatalogTable)
    {
        throw new AccioException(GENERIC_INTERNAL_ERROR, "Postgres no need to invoke this method");
    }

    @Override
    protected String createPgAttributeTable(PgCatalogTable pgCatalogTable)
    {
        throw new AccioException(GENERIC_INTERNAL_ERROR, "Postgres no need to invoke this method");
    }

    @Override
    protected String createPgAttrdefTable(PgCatalogTable pgCatalogTable)
    {
        throw new AccioException(GENERIC_INTERNAL_ERROR, "Postgres no need to invoke this method");
    }

    @Override
    protected String createPgConstraintTable(PgCatalogTable pgCatalogTable)
    {
        throw new AccioException(GENERIC_INTERNAL_ERROR, "Postgres no need to invoke this method");
    }

    @Override
    protected String createPgDatabaseTable(PgCatalogTable pgCatalogTable)
    {
        throw new AccioException(GENERIC_INTERNAL_ERROR, "Postgres no need to invoke this method");
    }

    @Override
    protected String createPgDescriptionTable(PgCatalogTable pgCatalogTable)
    {
        throw new AccioException(GENERIC_INTERNAL_ERROR, "Postgres no need to invoke this method");
    }

    @Override
    protected String createPgEnumTable(PgCatalogTable pgCatalogTable)
    {
        throw new AccioException(GENERIC_INTERNAL_ERROR, "Postgres no need to invoke this method");
    }

    @Override
    protected String createPgIndexTable(PgCatalogTable pgCatalogTable)
    {
        throw new AccioException(GENERIC_INTERNAL_ERROR, "Postgres no need to invoke this method");
    }

    @Override
    protected String createPgNamespaceTable(PgCatalogTable pgCatalogTable)
    {
        throw new AccioException(GENERIC_INTERNAL_ERROR, "Postgres no need to invoke this method");
    }

    @Override
    protected String createPgProcTable(PgCatalogTable pgCatalogTable)
    {
        throw new AccioException(GENERIC_INTERNAL_ERROR, "Postgres no need to invoke this method");
    }

    @Override
    protected String createPgRangeTable(PgCatalogTable pgCatalogTable)
    {
        throw new AccioException(GENERIC_INTERNAL_ERROR, "Postgres no need to invoke this method");
    }

    @Override
    protected String createPgRoleTable(PgCatalogTable pgCatalogTable)
    {
        throw new AccioException(GENERIC_INTERNAL_ERROR, "Postgres no need to invoke this method");
    }

    @Override
    protected String createPgSettingsTable(PgCatalogTable pgCatalogTable)
    {
        throw new AccioException(GENERIC_INTERNAL_ERROR, "Postgres no need to invoke this method");
    }

    @Override
    protected String createPgTablespaceTable(PgCatalogTable pgCatalogTable)
    {
        throw new AccioException(GENERIC_INTERNAL_ERROR, "Postgres no need to invoke this method");
    }

    @Override
    protected String createCharacterSets(PgCatalogTable pgCatalogTable)
    {
        throw new AccioException(GENERIC_INTERNAL_ERROR, "Postgres no need to invoke this method");
    }

    @Override
    protected String createReferentialConstraints(PgCatalogTable pgCatalogTable)
    {
        throw new AccioException(GENERIC_INTERNAL_ERROR, "Postgres no need to invoke this method");
    }

    @Override
    protected String createKeyColumnUsage(PgCatalogTable pgCatalogTable)
    {
        throw new AccioException(GENERIC_INTERNAL_ERROR, "Postgres no need to invoke this method");
    }

    @Override
    protected String createTableConstraints(PgCatalogTable pgCatalogTable)
    {
        throw new AccioException(GENERIC_INTERNAL_ERROR, "Postgres no need to invoke this method");
    }
}
