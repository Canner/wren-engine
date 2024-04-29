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

package io.wren.base.sqlrewrite;

import io.trino.sql.SqlFormatter;
import io.trino.sql.tree.Statement;
import io.wren.base.AnalyzedMDL;
import io.wren.base.WrenMDL;
import io.wren.base.WrenTypes;
import io.wren.base.dto.Column;
import io.wren.base.dto.Metric;
import io.wren.base.dto.Model;
import io.wren.base.dto.TimeGrain;
import io.wren.base.dto.TimeUnit;
import io.wren.base.dto.View;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static io.wren.base.sqlrewrite.GenerateViewRewrite.GENERATE_VIEW_REWRITE;
import static io.wren.base.sqlrewrite.MetricRollupRewrite.METRIC_ROLLUP_REWRITE;
import static io.wren.base.sqlrewrite.Utils.parseSql;
import static io.wren.base.sqlrewrite.WrenSqlRewrite.WREN_SQL_REWRITE;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class TestMetricViewSqlRewrite
        extends AbstractTestFramework
{
    @Language("sql")
    private static final String MODEL_CTES = "" +
            "  \"Album\" AS (\n" +
            "   SELECT\n" +
            "     \"Album\".\"id\" \"id\"\n" +
            "   , \"Album\".\"name\" \"name\"\n" +
            "   , \"Album\".\"author\" \"author\"\n" +
            "   , \"Album\".\"price\" \"price\"\n" +
            "   , \"Album\".\"publish_date\" \"publish_date\"\n" +
            "   , \"Album\".\"release_date\" \"release_date\"\n" +
            "   FROM\n" +
            "     (\n" +
            "      SELECT\n" +
            "        \"Album\".\"id\" \"id\"\n" +
            "      , \"Album\".\"name\" \"name\"\n" +
            "      , \"Album\".\"author\" \"author\"\n" +
            "      , \"Album\".\"price\" \"price\"\n" +
            "      , \"Album\".\"publish_date\" \"publish_date\"\n" +
            "      , \"Album\".\"release_date\" \"release_date\"\n" +
            "      FROM\n" +
            "        (\n" +
            "         SELECT *\n" +
            "         FROM\n" +
            "           (\n" +
            " VALUES \n" +
            "              ROW (1, 'Gusare', 'ZUTOMAYO', 2560, DATE '2023-03-29', TIMESTAMP '2023-04-27 06:06:06')\n" +
            "            , ROW (2, 'HisoHiso Banashi', 'ZUTOMAYO', 1500, DATE '2023-04-29', TIMESTAMP '2023-05-27 07:07:07')\n" +
            "            , ROW (3, 'Dakara boku wa ongaku o yameta', 'Yorushika', 2553, DATE '2023-05-29', TIMESTAMP '2023-06-27 08:08:08')\n" +
            "         )  album (id, name, author, price, publish_date, release_date)\n" +
            "      )  \"Album\"\n" +
            "   )  \"Album\"\n" +
            ")\n";

    @Language("sql")
    private static final String METRIC_CTES =
            MODEL_CTES +
                    ", \"Collection\" AS (\n" +
                    "   SELECT\n" +
                    "     \"Album\".\"author\" \"author\"\n" +
                    "   , \"Album\".\"name\" \"album_name\"\n" +
                    "   , sum(\"Album\".\"price\") \"price\"\n" +
                    "   FROM\n" +
                    "     (\n" +
                    "      SELECT *\n" +
                    "      FROM\n" +
                    "        (\n" +
                    "         SELECT *\n" +
                    "         FROM\n" +
                    "           \"Album\"\n" +
                    "      )  \"Album\"\n" +
                    "   )  \"Album\"\n" +
                    "   GROUP BY 1, 2" +
                    ") \n";

    private final WrenMDL wrenMDL;

    public TestMetricViewSqlRewrite()
    {
        wrenMDL = WrenMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(List.of(
                        Model.model("Album",
                                "select * from (values (1, 'Gusare', 'ZUTOMAYO', 2560, DATE '2023-03-29', TIMESTAMP '2023-04-27 06:06:06'), " +
                                        "(2, 'HisoHiso Banashi', 'ZUTOMAYO', 1500, DATE '2023-04-29', TIMESTAMP '2023-05-27 07:07:07'), " +
                                        "(3, 'Dakara boku wa ongaku o yameta', 'Yorushika', 2553, DATE '2023-05-29', TIMESTAMP '2023-06-27 08:08:08')) " +
                                        "album(id, name, author, price, publish_date, release_date)",
                                List.of(
                                        Column.column("id", WrenTypes.INTEGER, null, true),
                                        Column.column("name", WrenTypes.VARCHAR, null, true),
                                        Column.column("author", WrenTypes.VARCHAR, null, true),
                                        Column.column("price", WrenTypes.INTEGER, null, true),
                                        Column.column("publish_date", WrenTypes.DATE, null, true),
                                        Column.column("release_date", WrenTypes.TIMESTAMP, null, true)))))
                .setMetrics(List.of(
                        Metric.metric(
                                "Collection",
                                "Album",
                                List.of(
                                        Column.column("author", WrenTypes.VARCHAR, null, true),
                                        Column.column("album_name", WrenTypes.VARCHAR, null, true, "\"name\"")),
                                List.of(Column.column("price", WrenTypes.INTEGER, null, true, "sum(price)")),
                                List.of(
                                        TimeGrain.timeGrain("p_date", "Album.publish_date", List.of(TimeUnit.YEAR)),
                                        TimeGrain.timeGrain("r_date", "Album.release_date", List.of(TimeUnit.YEAR))))))
                .setViews(List.of(
                        View.view("UseModel", "select * from \"Album\""),
                        View.view("useView", "select * from \"useMetric\""),
                        View.view("useMetric", "select * from \"Collection\"")))
                .build());
    }

    @DataProvider
    public Object[][] metricCases()
    {
        return new Object[][] {
                {"select author, price from Collection",
                        "WITH\n" +
                                METRIC_CTES +
                                "SELECT\n" +
                                "  author\n" +
                                ", price\n" +
                                "FROM\n" +
                                "  Collection"},
                {
                        "WITH c AS (SELECT * FROM Collection)\n" +
                                "SELECT * from c",
                        "WITH\n" +
                                METRIC_CTES +
                                ", c AS (SELECT * FROM Collection)\n" +
                                "SELECT * from c"
                },
                {
                        "SELECT author, price FROM roll_up(Collection, p_date, YEAR)",
                        "WITH\n" +
                                MODEL_CTES +
                                "SELECT\n" +
                                "  author\n" +
                                ", price\n" +
                                "FROM\n" +
                                "  (\n" +
                                "   SELECT\n" +
                                "     DATE_TRUNC('YEAR', Album.publish_date) \"p_date\"\n" +
                                "   , \"author\" \"author\"\n" +
                                "   , \"name\" \"album_name\"\n" +
                                "   , sum(price) \"price\"\n" +
                                "   FROM\n" +
                                "     \"Album\"\n" +
                                "   GROUP BY 1, 2, 3\n" +
                                ")  Collection"
                },
                {
                        "SELECT author, price FROM roll_up(wren.test.Collection, p_date, DAY)",
                        "WITH\n" +
                                MODEL_CTES +
                                "SELECT\n" +
                                "  author\n" +
                                ", price\n" +
                                "FROM\n" +
                                "  (\n" +
                                "   SELECT\n" +
                                "     DATE_TRUNC('DAY', Album.publish_date) \"p_date\"\n" +
                                "   , \"author\" \"author\"\n" +
                                "   , \"name\" \"album_name\"\n" +
                                "   , sum(price) \"price\"\n" +
                                "   FROM\n" +
                                "     \"Album\"\n" +
                                "   GROUP BY 1, 2, 3\n" +
                                ")  Collection"
                },
                {
                        "SELECT author, price FROM UseModel",
                        "WITH\n" + MODEL_CTES +
                                ", \"UseModel\" AS (\n" +
                                "   SELECT *\n" +
                                "   FROM\n" +
                                "     \"Album\"\n" +
                                ") \n" +
                                "SELECT\n" +
                                "  author\n" +
                                ", price\n" +
                                "FROM\n" +
                                "  UseModel"
                },
                {
                        "SELECT album_name, price FROM useMetric",
                        "WITH\n" + MODEL_CTES +
                                ", \"Collection\" AS (\n" +
                                "   SELECT\n" +
                                "     \"Album\".\"author\" \"author\"\n" +
                                "   , \"Album\".\"name\" \"album_name\"\n" +
                                "   , sum(\"Album\".\"price\") \"price\"\n" +
                                "   FROM\n" +
                                "     (\n" +
                                "      SELECT *\n" +
                                "      FROM\n" +
                                "        (\n" +
                                "         SELECT *\n" +
                                "         FROM\n" +
                                "           \"Album\"\n" +
                                "      )  \"Album\"\n" +
                                "   )  \"Album\"\n" +
                                "   GROUP BY 1, 2" +
                                ") \n" +
                                ", \"useMetric\" AS (\n" +
                                "   SELECT *\n" +
                                "   FROM\n" +
                                "     \"Collection\"\n" +
                                ") \n" +
                                "SELECT\n" +
                                "  album_name\n" +
                                ", price\n" +
                                "FROM\n" +
                                "  useMetric"
                },
                {
                        "SELECT album_name, price FROM useView",
                        "WITH\n" + MODEL_CTES +
                                ", \"Collection\" AS (\n" +
                                "   SELECT\n" +
                                "     \"Album\".\"author\" \"author\"\n" +
                                "   , \"Album\".\"name\" \"album_name\"\n" +
                                "   , sum(\"Album\".\"price\") \"price\"\n" +
                                "   FROM\n" +
                                "     (\n" +
                                "      SELECT *\n" +
                                "      FROM\n" +
                                "        (\n" +
                                "         SELECT *\n" +
                                "         FROM\n" +
                                "           \"Album\"\n" +
                                "      )  \"Album\"\n" +
                                "   )  \"Album\"\n" +
                                "   GROUP BY 1, 2\n" +
                                ") \n" +
                                ", \"useMetric\" AS (\n" +
                                "   SELECT *\n" +
                                "   FROM\n" +
                                "     \"Collection\"\n" +
                                ") \n" +
                                ", \"useView\" AS (\n" +
                                "   SELECT *\n" +
                                "   FROM\n" +
                                "     \"useMetric\"\n" +
                                ") \n" +
                                "SELECT\n" +
                                "  album_name\n" +
                                ", price\n" +
                                "FROM\n" +
                                "  useView"
                }
        };
    }

    @Test(dataProvider = "metricCases")
    public void testMetricSqlRewrite(String original, String expected)
    {
        Statement expectedState = parseSql(expected);
        String actualSql = rewrite(original);
        assertThat(actualSql).isEqualTo(SqlFormatter.formatSql(expectedState));
        assertThatNoException()
                .describedAs(format("actual sql: %s is invalid", actualSql))
                .isThrownBy(() -> query(actualSql));
    }

    private String rewrite(String sql)
    {
        return rewrite(sql, wrenMDL);
    }

    private String rewrite(String sql, WrenMDL wrenMDL)
    {
        return WrenPlanner.rewrite(sql, DEFAULT_SESSION_CONTEXT, new AnalyzedMDL(wrenMDL, null), List.of(GENERATE_VIEW_REWRITE, METRIC_ROLLUP_REWRITE, WREN_SQL_REWRITE));
    }
}
