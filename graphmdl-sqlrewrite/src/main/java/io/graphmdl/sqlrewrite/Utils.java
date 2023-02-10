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

package io.graphmdl.sqlrewrite;

import io.graphmdl.base.dto.Column;
import io.graphmdl.base.dto.Metric;
import io.graphmdl.base.dto.Model;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Statement;

import java.security.SecureRandom;
import java.util.stream.Stream;

import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static java.lang.Character.MAX_RADIX;
import static java.lang.Math.abs;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class Utils
{
    private static final SecureRandom random = new SecureRandom();
    private static final int RANDOM_SUFFIX_LENGTH = 10;

    public static final SqlParser SQL_PARSER = new SqlParser();

    private Utils() {}

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
        return format("SELECT %s FROM (%s)", model.getColumns().stream().map(Column::getSqlExpression).collect(joining(", ")), model.getRefSql());
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

    private static String getMetricSql(Metric metric)
    {
        requireNonNull(metric, "metric is null");
        String selectItems = Stream.concat(metric.getDimension().stream(), metric.getMeasure().stream())
                .map(Column::getSqlExpression).collect(joining(","));
        String groupByItems = metric.getDimension().stream().map(Column::getName).collect(joining(","));
        return format("SELECT %s FROM %s GROUP BY %s", selectItems, metric.getBaseModel(), groupByItems);
    }

    public static String randomTableSuffix()
    {
        String randomSuffix = Long.toString(abs(random.nextLong()), MAX_RADIX);
        return randomSuffix.substring(0, min(RANDOM_SUFFIX_LENGTH, randomSuffix.length()));
    }
}
