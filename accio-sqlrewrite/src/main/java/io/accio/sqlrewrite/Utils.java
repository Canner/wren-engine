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

package io.accio.sqlrewrite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.accio.base.AccioMDL;
import io.accio.base.CatalogSchemaTableName;
import io.accio.base.SessionContext;
import io.accio.base.dto.Column;
import io.accio.base.dto.Metric;
import io.accio.base.dto.Model;
import io.accio.sqlrewrite.analyzer.Field;
import io.accio.sqlrewrite.analyzer.MetricRollupInfo;
import io.accio.sqlrewrite.analyzer.RelationType;
import io.accio.sqlrewrite.analyzer.Scope;
import io.accio.sqlrewrite.analyzer.ScopeAnalysis;
import io.accio.sqlrewrite.analyzer.ScopeAnalyzer;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.SubscriptExpression;

import java.security.SecureRandom;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.accio.base.Utils.checkArgument;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static java.lang.Character.MAX_RADIX;
import static java.lang.Math.abs;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public final class Utils
{
    public static final SqlParser SQL_PARSER = new SqlParser();
    private static final ParsingOptions PARSING_OPTIONS = new ParsingOptions(AS_DECIMAL);
    private static final SecureRandom random = new SecureRandom();
    private static final int RANDOM_SUFFIX_LENGTH = 10;

    private Utils() {}

    public static Statement parseSql(String sql)
    {
        return SQL_PARSER.createStatement(sql, PARSING_OPTIONS);
    }

    public static Query parseView(String sql)
    {
        Query query = (Query) parseSql(sql);
        // TODO: we don't support view have WITH clause yet
        checkArgument(query.getWith().isEmpty(), "view cannot have WITH clause");
        return query;
    }

    public static Expression parseExpression(String expression)
    {
        return SQL_PARSER.createExpression(expression, PARSING_OPTIONS);
    }

    public static DataType parseType(String type)
    {
        return SQL_PARSER.createType(type);
    }

    public static Query parseModelSql(Model model)
    {
        String sql = getModelSql(model);
        Statement statement = SQL_PARSER.createStatement(sql, new ParsingOptions(AS_DECIMAL));
        if (statement instanceof Query) {
            return (Query) statement;
        }
        throw new IllegalArgumentException(format("model %s is not a query, sql %s", model.getName(), sql));
    }

    public static String getModelSql(Model model)
    {
        requireNonNull(model, "model is null");
        if (model.getColumns().isEmpty()) {
            return model.getRefSql();
        }
        // In postgres, all subquery should have alias.
        return format("SELECT %s FROM (%s) t",
                model.getColumns().stream()
                        .filter(column -> column.getRelationship().isEmpty())
                        .map(Column::getSqlExpression)
                        .collect(joining(", ")),
                model.getRefSql());
    }

    public static Query parseMetricSql(Metric metric)
    {
        String sql = getMetricSql(metric);
        Statement statement = SQL_PARSER.createStatement(sql, new ParsingOptions(AS_DECIMAL));
        if (statement instanceof Query) {
            return (Query) statement;
        }
        throw new IllegalArgumentException(format("metric %s is not a query, sql %s", metric.getName(), sql));
    }

    public static Query parseMetricRollupSql(MetricRollupInfo metricRollupInfo)
    {
        String sql = getMetricRollupSql(metricRollupInfo);
        Statement statement = SQL_PARSER.createStatement(sql, new ParsingOptions(AS_DECIMAL));
        if (statement instanceof Query) {
            return (Query) statement;
        }
        throw new IllegalArgumentException(format("metric %s is not a query, sql %s", metricRollupInfo.getMetric().getName(), sql));
    }

    private static String getMetricSql(Metric metric)
    {
        requireNonNull(metric, "metric is null");
        String selectItems = Stream.concat(metric.getDimension().stream(), metric.getMeasure().stream())
                .map(Column::getSqlExpression).collect(joining(","));
        String groupByItems = IntStream.rangeClosed(1, metric.getDimension().size()).mapToObj(String::valueOf).collect(joining(","));
        return format("SELECT %s FROM %s GROUP BY %s", selectItems, metric.getBaseModel(), groupByItems);
    }

    private static String getMetricRollupSql(MetricRollupInfo metricRollupInfo)
    {
        requireNonNull(metricRollupInfo, "metricRollupInfo is null");

        Metric metric = metricRollupInfo.getMetric();
        String timeGrain = format("DATE_TRUNC('%s', %s) \"%s\"",
                metricRollupInfo.getDatePart(),
                metricRollupInfo.getTimeGrain().getRefColumn(),
                metricRollupInfo.getTimeGrain().getName());

        List<String> selectItems =
                ImmutableList.<String>builder()
                        .add(timeGrain)
                        .addAll(
                                Stream.concat(metric.getDimension().stream(), metric.getMeasure().stream())
                                        .map(Column::getSqlExpression)
                                        .collect(toList()))
                        .build();

        String groupByColumnOrdinals =
                IntStream.rangeClosed(1, selectItems.size() - metric.getMeasure().size())
                        .mapToObj(String::valueOf)
                        .collect(joining(","));

        return format("SELECT %s FROM %s GROUP BY %s",
                String.join(",", selectItems),
                metric.getBaseModel(),
                groupByColumnOrdinals);
    }

    public static String randomTableSuffix()
    {
        String randomSuffix = Long.toString(abs(random.nextLong()), MAX_RADIX);
        return randomSuffix.substring(0, min(RANDOM_SUFFIX_LENGTH, randomSuffix.length()));
    }

    public static CatalogSchemaTableName toCatalogSchemaTableName(SessionContext sessionContext, QualifiedName name)
    {
        requireNonNull(sessionContext, "sessionContext is null");
        requireNonNull(name, "name is null");
        if (name.getParts().size() > 3) {
            throw new IllegalArgumentException(format("Too many dots in table name: %s", name));
        }

        List<String> parts = Lists.reverse(name.getParts());
        String objectName = parts.get(0);
        String schemaName = (parts.size() > 1) ? parts.get(1) : sessionContext.getSchema().orElseThrow(() ->
                new IllegalArgumentException("Schema must be specified when session schema is not set"));
        String catalogName = (parts.size() > 2) ? parts.get(2) : sessionContext.getCatalog().orElseThrow(() ->
                new IllegalArgumentException("Catalog must be specified when session catalog is not set"));

        return new CatalogSchemaTableName(catalogName, schemaName, objectName);
    }

    public static QualifiedName toQualifiedName(CatalogSchemaTableName name)
    {
        requireNonNull(name, "name is null");
        return QualifiedName.of(name.getCatalogName(), name.getSchemaTableName().getSchemaName(), name.getSchemaTableName().getTableName());
    }

    public static Expression getNextPart(SubscriptExpression subscriptExpression)
    {
        Expression base = subscriptExpression.getBase();
        if (base instanceof DereferenceExpression) {
            return ((DereferenceExpression) base).getBase();
        }
        return base;
    }

    // TODO: handle accio view scope https://github.com/Canner/accio/issues/338
    public static Scope analyzeFrom(AccioMDL accioMDL, SessionContext sessionContext, Relation node, Optional<Scope> context)
    {
        ScopeAnalysis analysis = ScopeAnalyzer.analyze(accioMDL, node, sessionContext);
        List<ScopeAnalysis.Relation> usedAccioObjects = analysis.getUsedAccioObjects();
        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        accioMDL.listModels().stream()
                .filter(model -> usedAccioObjects.stream().anyMatch(relation -> relation.getName().equals(model.getName())))
                .forEach(model ->
                        model.getColumns().forEach(column -> fields.add(toField(accioMDL, model.getName(), column, usedAccioObjects))));

        accioMDL.listMetrics().stream()
                .filter(metric -> usedAccioObjects.stream().anyMatch(relation -> relation.getName().equals(metric.getName())))
                .forEach(metric -> {
                    metric.getDimension().forEach(column -> fields.add(toField(accioMDL, metric.getName(), column, usedAccioObjects)));
                    metric.getMeasure().forEach(column -> fields.add(toField(accioMDL, metric.getName(), column, usedAccioObjects)));
                });

        return Scope.builder()
                .parent(context)
                .relationType(new RelationType(fields.build()))
                .isTableScope(true)
                .build();
    }

    private static Field toField(AccioMDL accioMDL, String modelName, Column column, List<ScopeAnalysis.Relation> usedAccioObjects)
    {
        ScopeAnalysis.Relation relation = usedAccioObjects.stream()
                .filter(r -> r.getName().equals(modelName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Model not found: " + modelName));

        return Field.builder()
                .modelName(new CatalogSchemaTableName(accioMDL.getCatalog(), accioMDL.getSchema(), modelName))
                .columnName(column.getName())
                .name(column.getName())
                .relationAlias(relation.getAlias().map(QualifiedName::of).orElse(null))
                .relationship(column.getRelationship().flatMap(accioMDL::getRelationship))
                .type(column.getType())
                .build();
    }
}
