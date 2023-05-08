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

package io.graphmdl.preaggregation;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import io.graphmdl.base.CatalogSchemaTableName;
import io.graphmdl.base.ConnectorRecordIterator;
import io.graphmdl.base.GraphMDL;
import io.graphmdl.base.GraphMDLException;
import io.graphmdl.base.Parameter;
import io.graphmdl.base.SessionContext;
import io.graphmdl.base.client.duckdb.DuckdbClient;
import io.graphmdl.base.dto.Metric;
import io.graphmdl.base.sql.SqlConverter;
import io.graphmdl.sqlrewrite.GraphMDLPlanner;
import io.graphmdl.sqlrewrite.PreAggregationRewrite;
import io.trino.execution.sql.SqlFormatterUtil;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.graphmdl.base.metadata.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.supplyAsync;

public class PreAggregationManager
{
    private static final Logger LOG = Logger.get(PreAggregationManager.class);
    private static final ParsingOptions PARSE_AS_DECIMAL = new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL);
    private final ExtraRewriter extraRewriter;
    private final PreAggregationService preAggregationService;
    private final SqlParser sqlParser;
    private final SqlConverter sqlConverter;
    private final DuckdbClient duckdbClient;
    private final DuckdbStorageConfig duckdbStorageConfig;
    private final AtomicReference<Map<CatalogSchemaTableName, MetricTablePair>> metricTableMapping = new AtomicReference<>(Map.of());

    @Inject
    public PreAggregationManager(
            SqlParser sqlParser,
            SqlConverter sqlConverter,
            PreAggregationService preAggregationService,
            ExtraRewriter extraRewriter,
            DuckdbClient duckdbClient,
            DuckdbStorageConfig duckdbStorageConfig)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.sqlConverter = requireNonNull(sqlConverter, "sqlConverter is null");
        this.preAggregationService = requireNonNull(preAggregationService, "preAggregationService is null");
        this.extraRewriter = requireNonNull(extraRewriter, "extraRewriter is null");
        this.duckdbClient = requireNonNull(duckdbClient, "duckdbClient is null");
        this.duckdbStorageConfig = requireNonNull(duckdbStorageConfig, "storageConfig is null");
    }

    @VisibleForTesting
    public MetricTablePair getPreAggregationMetricTablePair(String catalog, String schema, String table)
    {
        return metricTableMapping.get().get(new CatalogSchemaTableName(catalog, schema, table));
    }

    public CompletableFuture<Void> doPreAggregation(GraphMDL mdl)
    {
        List<CompletableFuture<MetricTablePair>> futures = mdl.listMetrics()
                .stream()
                .filter(Metric::isPreAggregated)
                .map(metric -> doSingleMetricPreAggregation(mdl, metric))
                .collect(toImmutableList());
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        return allFutures.whenComplete((v, e) -> {
            if (e != null) {
                LOG.error(e, "Failed to do pre-aggregation");
            }
            else {
                Map<CatalogSchemaTableName, MetricTablePair> mapping = futures.stream()
                        .map(CompletableFuture::join)
                        .collect(toImmutableMap(
                                pair -> new CatalogSchemaTableName(mdl.getCatalog(), mdl.getSchema(), pair.getMetric().getName()),
                                pair -> pair));
                metricTableMapping.set(mapping);
            }
        });
    }

    public Optional<String> convertToAggregationTable(CatalogSchemaTableName catalogSchemaTableName)
    {
        return metricTableMapping.get().get(catalogSchemaTableName).getTableName();
    }

    public ConnectorRecordIterator query(String sql, List<Parameter> parameters)
            throws SQLException
    {
        return DuckdbRecordIterator.of(duckdbClient.executeQuery(sql, parameters));
    }

    private CompletableFuture<MetricTablePair> doSingleMetricPreAggregation(GraphMDL mdl, Metric metric)
    {
        return supplyAsync(() -> {
            SessionContext sessionContext = SessionContext.builder()
                    .setCatalog(mdl.getCatalog())
                    .setSchema(mdl.getSchema())
                    .build();
            String graphMDLRewritten = GraphMDLPlanner.rewrite(
                    format("select * from %s", metric.getName()),
                    sessionContext,
                    mdl);
            Statement parsedStatement = sqlParser.createStatement(graphMDLRewritten, PARSE_AS_DECIMAL);
            Statement rewrittenStatement = extraRewriter.rewrite(parsedStatement);
            String exportPath = preAggregationService.createPreAggregation(
                    mdl.getCatalog(),
                    mdl.getSchema(),
                    metric.getName(),
                    sqlConverter.convert(SqlFormatterUtil.getFormattedSql(rewrittenStatement, sqlParser), sessionContext));
            String duckdbTableName = format("%s_%s", metric.getName(), randomUUID());
            importData(exportPath, duckdbTableName);
            return new MetricTablePair(metric, duckdbTableName);
        }).exceptionally(e -> {
            String errMsg = format("Failed to do pre-aggregation for metric %s; caused by %s", metric.getName(), e.getMessage());
            LOG.error(e, errMsg);
            return new MetricTablePair(metric, null, errMsg);
        });
    }

    private void importData(String path, String tableName)
    {
        // ref: https://github.com/duckdb/duckdb/issues/1403
        StringBuilder sb = new StringBuilder("INSTALL httpfs;\n" +
                "LOAD httpfs;\n");
        sb.append(format("SET s3_endpoint='%s';\n", duckdbStorageConfig.getEndpoint()));
        duckdbStorageConfig.getAccessKey().ifPresent(accessKey -> sb.append(format("SET s3_access_key_id='%s';\n", accessKey)));
        duckdbStorageConfig.getSecretKey().ifPresent(secretKey -> sb.append(format("SET s3_secret_access_key='%s';\n", secretKey)));
        sb.append(format("SET s3_url_style='%s';\n", duckdbStorageConfig.getUrlStyle()));
        sb.append(format("CREATE TABLE \"%s\" AS SELECT * FROM read_parquet('s3://%s');", tableName, path));
        duckdbClient.executeDDL(sb.toString());
    }

    public static class MetricTablePair
    {
        private final Metric metric;
        private final Optional<String> tableName;
        private final Optional<String> errorMessage;

        public MetricTablePair(Metric metric, String tableName)
        {
            this(metric, tableName, null);
        }

        public MetricTablePair(Metric metric, String tableName, String errorMessage)
        {
            this.metric = metric;
            this.tableName = Optional.ofNullable(tableName);
            this.errorMessage = Optional.ofNullable(errorMessage);
        }

        public Metric getMetric()
        {
            return metric;
        }

        public String getRequiredTableName()
        {
            return tableName.orElseThrow(() -> new GraphMDLException(GENERIC_USER_ERROR, "Mapping table name not exists"));
        }

        public Optional<String> getTableName()
        {
            return tableName;
        }

        public Optional<String> getErrorMessage()
        {
            return errorMessage;
        }
    }

    public Optional<String> rewritePreAggregation(SessionContext sessionContext, String sql, GraphMDL graphMDL)
    {
        try {
            Statement statement = sqlParser.createStatement(sql, new ParsingOptions(AS_DECIMAL));
            return PreAggregationRewrite.rewrite(sessionContext, statement, this::convertToAggregationTable, graphMDL)
                    .map(rewrittenStatement -> SqlFormatterUtil.getFormattedSql(rewrittenStatement, sqlParser));
        }
        catch (Exception e) {
            LOG.error(e, "Failed to rewrite pre-aggregation for statement: %s", sql);
        }
        return Optional.empty();
    }

    @PreDestroy
    public void cleanPreAggregation()
    {
        preAggregationService.cleanPreAggregation();
    }
}
