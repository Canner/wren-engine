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

package io.accio.base.sqlrewrite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.accio.base.AccioMDL;
import io.accio.base.CatalogSchemaTableName;
import io.accio.base.SessionContext;
import io.accio.base.dto.Column;
import io.accio.base.dto.CumulativeMetric;
import io.accio.base.dto.DateSpine;
import io.accio.base.dto.Metric;
import io.accio.base.dto.Model;
import io.accio.base.sqlrewrite.analyzer.Field;
import io.accio.base.sqlrewrite.analyzer.MetricRollupInfo;
import io.accio.base.sqlrewrite.analyzer.RelationType;
import io.accio.base.sqlrewrite.analyzer.Scope;
import io.accio.base.sqlrewrite.analyzer.ScopeAnalysis;
import io.accio.base.sqlrewrite.analyzer.ScopeAnalyzer;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.Statement;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.accio.base.Utils.checkArgument;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public final class Utils
{
    public static final SqlParser SQL_PARSER = new SqlParser();
    private static final ParsingOptions PARSING_OPTIONS = new ParsingOptions(AS_DECIMAL);

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

    public static Query parseQuery(String sql)
    {
        Statement statement = SQL_PARSER.createStatement(sql, new ParsingOptions(AS_DECIMAL));
        if (statement instanceof Query) {
            return (Query) statement;
        }
        throw new IllegalArgumentException("model sql is not a query");
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

    public static Query parseCumulativeMetricSql(CumulativeMetric cumulativeMetric, AccioMDL accioMDL)
    {
        String sql = getCumulativeMetricSql(cumulativeMetric, accioMDL);
        Statement statement = SQL_PARSER.createStatement(sql, new ParsingOptions(AS_DECIMAL));
        if (statement instanceof Query) {
            return (Query) statement;
        }
        throw new IllegalArgumentException(format("metric %s is not a query, sql %s", cumulativeMetric.getName(), sql));
    }

    public static String getCumulativeMetricSql(CumulativeMetric cumulativeMetric, AccioMDL accioMDL)
    {
        requireNonNull(cumulativeMetric, "cumulativeMetric is null");

        String windowType = getWindowType(cumulativeMetric, accioMDL)
                .orElseThrow(() -> new NoSuchElementException("window type not found in " + cumulativeMetric.getBaseObject()));

        String pattern =
                "select \n" +
                        "  metric_time as %s,\n" +
                        "  %s(distinct measure_field) as %s\n" +
                        "from \n" +
                        "  (\n" +
                        "    select \n" +
                        "      date_trunc('%s', d.metric_time) as metric_time,\n" +
                        "      measure_field\n" +
                        "    from \n" +
                        "      (%s) d \n" +
                        "      left join (\n" +
                        "        select \n" +
                        "          measure_field,\n" +
                        "          metric_time\n" +
                        "        from (%s) sub1\n" +
                        "        where \n" +
                        "          metric_time >= cast('%s' as %s) \n" +
                        "          and metric_time <= cast('%s' as %s)\n" +
                        "      ) sub2 on (\n" +
                        "        sub2.metric_time <= d.metric_time \n" +
                        "        and sub2.metric_time > %s\n" +
                        "      )\n" +
                        "    where \n" +
                        "      d.metric_time >= cast('%s' as %s)  \n" +
                        "      and d.metric_time <= cast('%s' as %s)   \n" +
                        "  ) sub3 \n" +
                        "group by 1\n" +
                        "order by 1\n";

        String castingDateSpine = format("select cast(metric_time as %s) as metric_time from %s", windowType, DateSpineInfo.NAME);
        String windowRange = format("d.metric_time - %s", cumulativeMetric.getWindow().getTimeUnit().getIntervalExpression());
        String selectFromModel = format("select %s as measure_field, %s as metric_time from %s",
                cumulativeMetric.getMeasure().getRefColumn(),
                cumulativeMetric.getWindow().getRefColumn(),
                cumulativeMetric.getBaseObject());

        return format(pattern,
                cumulativeMetric.getWindow().getName(),
                cumulativeMetric.getMeasure().getOperator(),
                cumulativeMetric.getMeasure().getName(),
                cumulativeMetric.getWindow().getTimeUnit().name(),
                castingDateSpine,
                selectFromModel,
                cumulativeMetric.getWindow().getStart(),
                windowType,
                cumulativeMetric.getWindow().getEnd(),
                windowType,
                windowRange,
                cumulativeMetric.getWindow().getStart(),
                windowType,
                cumulativeMetric.getWindow().getEnd(),
                windowType);
    }

    private static Optional<String> getWindowType(CumulativeMetric cumulativeMetric, AccioMDL accioMDL)
    {
        Optional<Model> baseModel = accioMDL.getModel(cumulativeMetric.getBaseObject());
        if (baseModel.isPresent()) {
            return baseModel.get().getColumns().stream()
                    .filter(column -> column.getName().equals(cumulativeMetric.getWindow().getRefColumn()))
                    .map(Column::getType)
                    .findAny();
        }

        Optional<Metric> baseMetric = accioMDL.getMetric(cumulativeMetric.getBaseObject());
        if (baseMetric.isPresent()) {
            return baseMetric.get().getColumns().stream()
                    .filter(column -> column.getName().equals(cumulativeMetric.getWindow().getRefColumn()))
                    .map(Column::getType)
                    .findAny();
        }

        Optional<CumulativeMetric> baseCumulativeMetric = accioMDL.getCumulativeMetric(cumulativeMetric.getBaseObject());
        if (baseCumulativeMetric.isPresent()) {
            if (baseCumulativeMetric.get().getWindow().getName().equals(cumulativeMetric.getWindow().getRefColumn())) {
                // TODO: potential stackoverflow issue since base object might use child object and
                //  this recursive call happen before cyclic DAG check in AccioSqlRewrite
                return getWindowType(baseCumulativeMetric.get(), accioMDL);
            }
            else {
                throw new IllegalArgumentException("CumulativeMetric measure cannot be window as it is not date/timestamp type");
            }
        }

        return Optional.empty();
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
                metric.getBaseObject(),
                groupByColumnOrdinals);
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
                .build();
    }

    public static Query createDateSpineQuery(DateSpine dateSpine)
    {
        // TODO: `GENERATE_TIMESTAMP_ARRAY` is a bigquery function. We may need to consider the SQL dialect when Accio planning.
        String sql = format("SELECT * FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP '%s', TIMESTAMP '%s', %s)) t(metric_time)", dateSpine.getStart(), dateSpine.getEnd(), dateSpine.getUnit().getIntervalExpression());
        Statement statement = SQL_PARSER.createStatement(sql, new ParsingOptions(AS_DECIMAL));
        if (statement instanceof Query) {
            return (Query) statement;
        }
        throw new IllegalArgumentException(format("Failed to parse date spine query: %s", sql));
    }
}
