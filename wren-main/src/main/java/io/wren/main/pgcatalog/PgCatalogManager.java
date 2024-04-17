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

package io.wren.main.pgcatalog;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.wren.base.Column;
import io.wren.base.WrenMDL;
import io.wren.base.pgcatalog.function.DataSourceFunctionRegistry;
import io.wren.base.pgcatalog.function.PgMetastoreFunctionRegistry;
import io.wren.base.wireprotocol.PgMetastore;
import io.wren.main.PreviewService;
import io.wren.main.WrenMetastore;
import io.wren.main.metadata.Metadata;
import io.wren.main.pgcatalog.builder.PgFunctionBuilderManager;
import io.wren.main.pgcatalog.builder.PgMetastoreFunctionBuilder;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class PgCatalogManager
{
    private static final Logger LOG = Logger.get(PgCatalogManager.class);

    protected final String pgCatalogName;

    private final Metadata connector;
    private final DataSourceFunctionRegistry dataSourceFunctionRegistry;
    private final PgMetastoreFunctionRegistry metastoreFunctionRegistry;
    private final PgFunctionBuilderManager pgFunctionBuilderManager;
    private final PgMetastoreFunctionBuilder pgMetastoreFunctionBuilder;
    private final PgMetastore pgMetastore;
    private final WrenMetastore wrenMetastore;
    private final PreviewService previewService;

    @Inject
    public PgCatalogManager(
            Metadata connector,
            PgFunctionBuilderManager pgFunctionBuilderManager,
            PgMetastore pgMetastore,
            WrenMetastore wrenMetastore,
            PreviewService previewService)
    {
        this.connector = requireNonNull(connector, "connector is null");
        this.pgFunctionBuilderManager = requireNonNull(pgFunctionBuilderManager, "pgFunctionBuilderManager is null");
        this.pgCatalogName = requireNonNull(connector.getPgCatalogName());
        this.dataSourceFunctionRegistry = new DataSourceFunctionRegistry();
        this.metastoreFunctionRegistry = new PgMetastoreFunctionRegistry();
        this.pgMetastore = requireNonNull(pgMetastore, "pgMetastore is null");
        this.pgMetastoreFunctionBuilder = new PgMetastoreFunctionBuilder(pgMetastore);
        this.wrenMetastore = requireNonNull(wrenMetastore, "wrenMetastore is null");
        this.previewService = requireNonNull(previewService, "previewService is null");
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
        WrenMDL mdl = wrenMetastore.getAnalyzedMDL().getWrenMDL();
        if (!(pgMetastore.isSchemaExist(mdl.getSchema()))) {
            LOG.warn("PgCatalog is not initialized");
            return false;
        }
        return true;
    }

    public void syncPgMetastore()
    {
        try {
            WrenMDL mdl = wrenMetastore.getAnalyzedMDL().getWrenMDL();
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
            mdl.listViews().stream().map(view -> new DescribedView(view.getName(), previewService.dryRun(mdl, view.getStatement()).join())).forEach(describedView -> {
                String cols = describedView.columns.stream().map(column -> format("\"%s\" %s", column.getName(), column.getType().typName())).collect(joining(","));
                sb.append(format("CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" (%s);\n", mdl.getSchema(), describedView.name, cols));
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

    static class DescribedView
    {
        private final String name;
        private final List<Column> columns;

        public DescribedView(String name, List<Column> columns)
        {
            this.name = name;
            this.columns = columns;
        }
    }
}
