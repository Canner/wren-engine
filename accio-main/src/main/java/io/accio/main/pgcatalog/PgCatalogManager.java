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
package io.accio.main.pgcatalog;

import com.google.inject.Inject;
import io.accio.base.AccioMDL;
import io.accio.base.pgcatalog.function.DataSourceFunctionRegistry;
import io.accio.base.pgcatalog.function.PgMetastoreFunctionRegistry;
import io.accio.main.AccioMetastore;
import io.accio.main.metadata.Metadata;
import io.accio.main.pgcatalog.builder.PgFunctionBuilderManager;
import io.accio.main.pgcatalog.builder.PgMetastoreFunctionBuilder;
import io.accio.main.wireprotocol.PgMetastore;
import io.airlift.log.Logger;
import org.apache.commons.lang3.StringUtils;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class PgCatalogManager
{
    private static final Logger LOG = Logger.get(PgCatalogManager.class);

    protected final String metadataSchemaName;
    protected final String pgCatalogName;

    private final Metadata connector;
    private final DataSourceFunctionRegistry dataSourceFunctionRegistry;
    private final PgMetastoreFunctionRegistry metastoreFunctionRegistry;
    private final PgFunctionBuilderManager pgFunctionBuilderManager;
    private final PgMetastoreFunctionBuilder pgMetastoreFunctionBuilder;
    private final PgMetastore pgMetastore;
    private final AccioMetastore accioMetastore;

    @Inject
    public PgCatalogManager(
            Metadata connector,
            PgFunctionBuilderManager pgFunctionBuilderManager,
            PgMetastore pgMetastore,
            PgMetastoreFunctionBuilder pgMetastoreFunctionBuilder,
            AccioMetastore accioMetastore)
    {
        this.connector = requireNonNull(connector, "connector is null");
        this.pgFunctionBuilderManager = requireNonNull(pgFunctionBuilderManager, "pgFunctionBuilderManager is null");
        this.metadataSchemaName = requireNonNull(connector.getMetadataSchemaName());
        this.pgCatalogName = requireNonNull(connector.getPgCatalogName());
        this.dataSourceFunctionRegistry = new DataSourceFunctionRegistry();
        this.metastoreFunctionRegistry = new PgMetastoreFunctionRegistry();
        this.pgMetastore = requireNonNull(pgMetastore, "pgMetastore is null");
        this.pgMetastoreFunctionBuilder = requireNonNull(pgMetastoreFunctionBuilder, "pgMetastoreFunctionBuilder is null");
        this.accioMetastore = requireNonNull(accioMetastore, "accioMetastore is null");
    }

    public void initPgCatalog()
    {
        if (!connector.isPgCompatible()) {
            createOrReplaceSchema(pgCatalogName);
        }
        initPgFunctions();
        syncPgMetastore();
    }

    public void initPgFunctions()
    {
        metastoreFunctionRegistry.getFunctions()
                .stream()
                .filter(f -> !f.isImplemented())
                .forEach(pgMetastoreFunctionBuilder::createPgFunction);
        if (!connector.isPgCompatible()) {
            dataSourceFunctionRegistry.getFunctions()
                    .stream()
                    .filter(f -> !f.isImplemented())
                    .forEach(pgFunctionBuilderManager::createPgFunction);
        }
    }

    public void dropSchema(String name)
    {
        pgMetastore.directDDL(format("DROP SCHEMA IF EXISTS %s CASCADE;", name));
    }

    private void createOrReplaceSchema(String name)
    {
        connector.dropSchemaIfExists(name);
        connector.createSchema(name);
    }

    public boolean checkRequired()
    {
        AccioMDL mdl = accioMetastore.getAnalyzedMDL().getAccioMDL();
        if (!(pgMetastore.isSchemaExist(mdl.getSchema()))) {
            LOG.warn("PgCatalog is not initialized");
            return false;
        }
        return true;
    }

    public void syncPgMetastore()
    {
        try {
            AccioMDL mdl = accioMetastore.getAnalyzedMDL().getAccioMDL();
            StringBuilder sb = new StringBuilder();
            if (StringUtils.isNotEmpty(mdl.getSchema())) {
                sb.append(format("DROP SCHEMA IF EXISTS \"%s\" CASCADE;\n", mdl.getSchema()));
                sb.append("CREATE SCHEMA IF NOT EXISTS ").append(mdl.getSchema()).append(";\n");
            }
            mdl.listModels().forEach(model -> {
                String cols = model.getColumns().stream()
                        .filter(column -> column.getRelationship().isEmpty())
                        .map(column -> format("\"%s\" %s", column.getName(), pgMetastore.handlePgType(column.getType())))
                        .collect(joining(","));
                sb.append(format("CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" (%s);\n", mdl.getSchema(), model.getName(), cols));
            });
            mdl.listMetrics().forEach(metric -> {
                String cols = metric.getColumns().stream().map(column -> format("\"%s\" %s", column.getName(), pgMetastore.handlePgType(column.getType()))).collect(joining(","));
                sb.append(format("CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" (%s);\n", mdl.getSchema(), metric.getName(), cols));
            });
            mdl.listCumulativeMetrics().forEach(metric -> {
                String cols = format("\"%s\" %s, \"%s\" %s",
                        metric.getMeasure().getName(),
                        pgMetastore.handlePgType(metric.getMeasure().getType()),
                        metric.getWindow().getName(),
                        mdl.getColumnType(metric.getName(), metric.getWindow().getName()));
                sb.append(format("CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" (%s);\n", mdl.getSchema(), metric.getName(), cols));
            });
            String syncSql = sb.toString();
            LOG.info("Sync PG Metastore DDL:\n %s", syncSql);
            if (!syncSql.isEmpty()) {
                pgMetastore.directDDL(syncSql);
            }
        }
        catch (Exception e) {
            // won't throw exception to avoid the sever start failed.
            LOG.error(e, "Failed to sync PG Metastore");
        }
    }
}
