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

package io.graphmdl;

import io.graphmdl.base.GraphML;
import io.graphmdl.base.dto.JoinType;
import io.graphmdl.testing.AbstractTestFramework;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.tree.Statement;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static io.graphmdl.Utils.SQL_PARSER;
import static io.graphmdl.base.GraphMLTypes.INTEGER;
import static io.graphmdl.base.GraphMLTypes.VARCHAR;
import static io.graphmdl.base.dto.Column.column;
import static io.graphmdl.base.dto.Column.relationshipColumn;
import static io.graphmdl.base.dto.Manifest.manifest;
import static io.graphmdl.base.dto.Metric.metric;
import static io.graphmdl.base.dto.Model.model;
import static io.graphmdl.base.dto.Relationship.relationship;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class TestAllRulesRewrite
        extends AbstractTestFramework
{
    private final GraphML graphML;

    public TestAllRulesRewrite()
    {
        graphML = GraphML.fromManifest(manifest(
                List.of(model("Album",
                                "select * from (values (1, 'Gusare', 1, 2560), " +
                                        "(2, 'HisoHiso Banashi', 1, 1500), " +
                                        "(3, 'Dakara boku wa ongaku o yameta', 2, 2553)) " +
                                        "Album(id, name, bandId, price)",
                                List.of(
                                        column("id", INTEGER, null, true),
                                        column("name", VARCHAR, null, true),
                                        relationshipColumn("band", "Band", "AlbumBand"),
                                        column("price", INTEGER, null, true))),
                        model("Band",
                                "select * from (values (1, 'ZUTOMAYO'), " +
                                        "(2, 'Yorushika')) " +
                                        "Band(id, name)",
                                List.of(
                                        column("id", INTEGER, null, true),
                                        column("name", VARCHAR, null, true)))),
                List.of(relationship("AlbumBand", List.of("Album", "Band"), JoinType.MANY_TO_ONE, "Album.bandId = Band.id")),
                List.of(),
                List.of(metric(
                        "Collection",
                        "Album",
                        // TODO: if dimension is a relationship type
                        List.of(column("band", VARCHAR, null, true, "Album.band.name")),
                        List.of(column("price", INTEGER, null, true, "sum(Album.price)"))))));
    }

    @DataProvider
    public Object[][] graphMLUsedCases()
    {
        return new Object[][] {
                {"select name, price from Album", "WITH\n" +
                        "  Album AS (\n" +
                        "   SELECT\n" +
                        "     id\n" +
                        "   , name\n" +
                        "   , 'relationship<AlbumBand>' band\n" +
                        "   , price\n" +
                        "   FROM\n" +
                        "     (\n" +
                        "      SELECT *\n" +
                        "      FROM\n" +
                        "        (\n" +
                        " VALUES \n" +
                        "           ROW (1, 'Gusare', 1, 2560)\n" +
                        "         , ROW (2, 'HisoHiso Banashi', 1, 1500)\n" +
                        "         , ROW (3, 'Dakara boku wa ongaku o yameta', 2, 2553)\n" +
                        "      )  Album (id, name, bandId, price)\n" +
                        "   ) \n" +
                        ") \n" +
                        "SELECT\n" +
                        "  name\n" +
                        ", price\n" +
                        "FROM\n" +
                        "  Album"},
                // TODO: access the relationship column in the baseModel of metric
                // {"select band, price from Collection", ""},
        };
    }

    @Test(dataProvider = "graphMLUsedCases")
    public void testGraphMLRewrite(String original, String expected)
    {
        Statement expectedState = SQL_PARSER.createStatement(expected, new ParsingOptions(AS_DECIMAL));
        String actualSql = rewrite(original);
        assertThat(actualSql).isEqualTo(SqlFormatter.formatSql(expectedState));
        assertThatNoException()
                .describedAs(format("actual sql: %s is invalid", actualSql))
                .isThrownBy(() -> query(actualSql));
    }

    @DataProvider
    public Object[][] noRewriteCase()
    {
        return new Object[][] {
                {"select 1, 2, 3"},
                {"select id, name from normalTable"},
                {"with normalCte as (select id, name from normalTable) select id, name from normalCte"}
        };
    }

    @Test(dataProvider = "noRewriteCase")
    public void testGraphMLNoRewrite(String original)
    {
        Statement expectedState = SQL_PARSER.createStatement(original, new ParsingOptions(AS_DECIMAL));
        assertThat(rewrite(original)).isEqualTo(SqlFormatter.formatSql(expectedState));
    }

    private String rewrite(String sql)
    {
        return GraphMLPlanner.rewrite(sql, graphML);
    }
}
