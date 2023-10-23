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

import io.accio.base.AccioMDL;
import io.accio.base.dto.Column;
import io.accio.testing.AbstractTestFramework;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.tree.Statement;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static io.accio.base.AccioTypes.DATE;
import static io.accio.base.AccioTypes.INTEGER;
import static io.accio.base.AccioTypes.TIMESTAMP;
import static io.accio.base.AccioTypes.VARCHAR;
import static io.accio.base.dto.Column.column;
import static io.accio.base.dto.Metric.metric;
import static io.accio.base.dto.Model.model;
import static io.accio.base.dto.TimeGrain.TimeUnit.YEAR;
import static io.accio.base.dto.TimeGrain.timeGrain;
import static io.accio.base.dto.View.view;
import static io.accio.sqlrewrite.AccioSqlRewrite.ACCIO_SQL_REWRITE;
import static io.accio.sqlrewrite.MetricViewSqlRewrite.METRIC_VIEW_SQL_REWRITE;
import static io.accio.sqlrewrite.Utils.SQL_PARSER;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class TestMetricViewSqlRewrite
        extends AbstractTestFramework
{
    @Language("sql")
    private static final String MODEL_CTES =
            "Album AS (\n" +
                    "   SELECT\n" +
                    "     \"id\"\n" +
                    "   , \"name\"\n" +
                    "   , \"author\"\n" +
                    "   , \"price\"\n" +
                    "   , \"publish_date\"\n" +
                    "   , \"release_date\"\n" +
                    "   FROM\n" +
                    "     (\n" +
                    "      SELECT *\n" +
                    "      FROM\n" +
                    "        (\n" +
                    " VALUES \n" +
                    "           ROW (1, 'Gusare', 'ZUTOMAYO', 2560, DATE '2023-03-29', TIMESTAMP '2023-04-27 06:06:06')\n" +
                    "         , ROW (2, 'HisoHiso Banashi', 'ZUTOMAYO', 1500, DATE '2023-04-29', TIMESTAMP '2023-05-27 07:07:07')\n" +
                    "         , ROW (3, 'Dakara boku wa ongaku o yameta', 'Yorushika', 2553, DATE '2023-05-29', TIMESTAMP '2023-06-27 08:08:08')\n" +
                    "      )  album (id, name, author, price, publish_date, release_date)\n" +
                    "   ) t\n" +
                    ") \n";

    @Language("sql")
    private static final String METRIC_CTES =
            MODEL_CTES +
                    ", Collection AS (\n" +
                    "   SELECT\n" +
                    "     \"author\"\n" +
                    "   , Album.name \"album_name\"\n" +
                    "   , sum(Album.price) \"price\"\n" +
                    "   FROM\n" +
                    "     Album\n" +
                    "   GROUP BY 1, 2\n" +
                    ") \n";

    private final AccioMDL accioMDL;
    private final AccioMDL invalidAccioMDL;

    public TestMetricViewSqlRewrite()
    {
        accioMDL = AccioMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(List.of(
                        model("Album",
                                "select * from (values (1, 'Gusare', 'ZUTOMAYO', 2560, DATE '2023-03-29', TIMESTAMP '2023-04-27 06:06:06'), " +
                                        "(2, 'HisoHiso Banashi', 'ZUTOMAYO', 1500, DATE '2023-04-29', TIMESTAMP '2023-05-27 07:07:07'), " +
                                        "(3, 'Dakara boku wa ongaku o yameta', 'Yorushika', 2553, DATE '2023-05-29', TIMESTAMP '2023-06-27 08:08:08')) " +
                                        "album(id, name, author, price, publish_date, release_date)",
                                List.of(
                                        column("id", INTEGER, null, true),
                                        column("name", VARCHAR, null, true),
                                        column("author", VARCHAR, null, true),
                                        column("price", INTEGER, null, true),
                                        column("publish_date", DATE, null, true),
                                        column("release_date", TIMESTAMP, null, true)))))
                .setMetrics(List.of(
                        metric(
                                "Collection",
                                "Album",
                                List.of(
                                        column("author", VARCHAR, null, true),
                                        column("album_name", VARCHAR, null, true, "Album.name")),
                                List.of(Column.column("price", INTEGER, null, true, "sum(Album.price)")),
                                List.of(
                                        timeGrain("p_date", "Album.publish_date", List.of(YEAR)),
                                        timeGrain("r_date", "Album.release_date", List.of(YEAR))))))
                .setViews(List.of(
                        view("UseModel", "select * from Album"),
                        view("useMetric", "select * from Collection"),
                        view("useView", "select * from useMetric")))
                .build());

        invalidAccioMDL = AccioMDL.fromManifest(withDefaultCatalogSchema()
                .setViews(List.of(
                        view("withView", "with a as (select 1,2,3) select * from a")))
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
                                ", Collection AS (\n" +
                                "   SELECT\n" +
                                "     DATE_TRUNC('YEAR', Album.publish_date) \"p_date\"\n" +
                                "   , \"author\"\n" +
                                "   , Album.name \"album_name\"\n" +
                                "   , sum(Album.price) \"price\"\n" +
                                "   FROM\n" +
                                "     Album\n" +
                                "   GROUP BY 1, 2, 3\n" +
                                ") \n" +
                                "SELECT\n" +
                                "  author\n" +
                                ", price\n" +
                                "FROM\n" +
                                "  Collection"
                },
                {
                        "SELECT author, price FROM roll_up(accio.test.Collection, p_date, DAY)",
                        "WITH\n" +
                                MODEL_CTES +
                                ", Collection AS (\n" +
                                "   SELECT\n" +
                                "     DATE_TRUNC('DAY', Album.publish_date) \"p_date\"\n" +
                                "   , \"author\"\n" +
                                "   , Album.name \"album_name\"\n" +
                                "   , sum(Album.price) \"price\"\n" +
                                "   FROM\n" +
                                "     Album\n" +
                                "   GROUP BY 1, 2, 3\n" +
                                ") \n" +
                                "SELECT\n" +
                                "  author\n" +
                                ", price\n" +
                                "FROM\n" +
                                "  Collection"
                },
                {
                        "SELECT author, price FROM UseModel",
                        "WITH\n" + MODEL_CTES +
                                ", UseModel AS (\n" +
                                "   SELECT *\n" +
                                "   FROM\n" +
                                "     Album\n" +
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
                                ", Collection AS (\n" +
                                "   SELECT\n" +
                                "     \"author\"\n" +
                                "   , Album.name \"album_name\"\n" +
                                "   , sum(Album.price) \"price\"\n" +
                                "   FROM\n" +
                                "     Album\n" +
                                "   GROUP BY 1, 2\n" +
                                ") \n" +
                                ", useMetric AS (\n" +
                                "   SELECT *\n" +
                                "   FROM\n" +
                                "     Collection\n" +
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
                                ", Collection AS (\n" +
                                "   SELECT\n" +
                                "     \"author\"\n" +
                                "   , Album.name \"album_name\"\n" +
                                "   , sum(Album.price) \"price\"\n" +
                                "   FROM\n" +
                                "     Album\n" +
                                "   GROUP BY 1, 2\n" +
                                ") \n" +
                                ", useMetric AS (\n" +
                                "   SELECT *\n" +
                                "   FROM\n" +
                                "     Collection\n" +
                                ") \n" +
                                ", useView AS (\n" +
                                "   SELECT *\n" +
                                "   FROM\n" +
                                "     useMetric\n" +
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
        Statement expectedState = SQL_PARSER.createStatement(expected, new ParsingOptions(AS_DECIMAL));
        String actualSql = rewrite(original);
        assertThat(actualSql).isEqualTo(SqlFormatter.formatSql(expectedState));
        assertThatNoException()
                .describedAs(format("actual sql: %s is invalid", actualSql))
                .isThrownBy(() -> query(actualSql));
    }

    @Test
    public void testInvalidAccioMDL()
    {
        String sql = "select * from withView";
        assertThatThrownBy(() -> rewrite(sql, invalidAccioMDL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("view cannot have WITH clause");
    }

    private String rewrite(String sql)
    {
        return rewrite(sql, accioMDL);
    }

    private String rewrite(String sql, AccioMDL accioMDL)
    {
        return AccioPlanner.rewrite(sql, DEFAULT_SESSION_CONTEXT, accioMDL, List.of(METRIC_VIEW_SQL_REWRITE, ACCIO_SQL_REWRITE));
    }
}
