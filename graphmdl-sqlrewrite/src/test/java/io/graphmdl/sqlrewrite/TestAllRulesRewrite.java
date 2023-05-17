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

import io.graphmdl.base.GraphMDL;
import io.graphmdl.base.dto.JoinType;
import io.graphmdl.testing.AbstractTestFramework;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.tree.Statement;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static io.graphmdl.base.GraphMDLTypes.INTEGER;
import static io.graphmdl.base.GraphMDLTypes.VARCHAR;
import static io.graphmdl.base.dto.Column.column;
import static io.graphmdl.base.dto.Column.relationshipColumn;
import static io.graphmdl.base.dto.Metric.metric;
import static io.graphmdl.base.dto.Model.model;
import static io.graphmdl.base.dto.Relationship.relationship;
import static io.graphmdl.sqlrewrite.Utils.SQL_PARSER;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAllRulesRewrite
        extends AbstractTestFramework
{
    private final GraphMDL graphMDL;

    public TestAllRulesRewrite()
    {
        graphMDL = GraphMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(List.of(
                        model("Album",
                                "select * from (values (1, 'Gusare', 1, 2560), " +
                                        "(2, 'HisoHiso Banashi', 1, 1500), " +
                                        "(3, 'Dakara boku wa ongaku o yameta', 2, 2553)) " +
                                        "Album(id, name, bandId, price)",
                                List.of(
                                        column("id", INTEGER, null, true),
                                        column("name", VARCHAR, null, true),
                                        relationshipColumn("band", "Band", "AlbumBand"),
                                        column("price", INTEGER, null, true),
                                        column("bandId", INTEGER, null, true)),
                                "id"),
                        model("Band",
                                "select * from (values (1, 'ZUTOMAYO'), " +
                                        "(2, 'Yorushika')) " +
                                        "Band(id, name)",
                                List.of(
                                        column("id", INTEGER, null, true),
                                        column("name", VARCHAR, null, true)),
                                "id")))
                .setRelationships(List.of(relationship("AlbumBand", List.of("Album", "Band"), JoinType.MANY_TO_ONE, "Album.bandId = Band.id")))
                .setMetrics(List.of(
                        metric(
                                "Collection",
                                "Album",
                                List.of(column("band", VARCHAR, null, true, "Album.band.name")),
                                List.of(column("price", INTEGER, null, true, "sum(Album.price)")),
                                List.of()),
                        metric(
                                "CollectionA",
                                "Album",
                                List.of(column("band", VARCHAR, null, true, null)),
                                List.of(column("price", INTEGER, null, true, "sum(Album.price)")),
                                List.of())))
                .build());
    }

    @DataProvider
    public Object[][] graphMDLUsedCases()
    {
        return new Object[][] {
                {"select name, price from Album",
                        "values('Gusare', 2560), ('HisoHiso Banashi', 1500), ('Dakara boku wa ongaku o yameta', 2553)"},
                {"SELECT name, price FROM graphmdl.test.Album",
                        "values('Gusare', 2560), ('HisoHiso Banashi', 1500), ('Dakara boku wa ongaku o yameta', 2553)"},
                {"select band.name, count(*) from Album group by band", "values ('ZUTOMAYO', cast(2 as long)), ('Yorushika', cast(1 as long))"},
                {"select band, price from Collection order by price", "values ('Yorushika', cast(2553 as long)), ('ZUTOMAYO', cast(4060 as long))"},
                {"select band, price from CollectionA order by price", "values ('relationship<AlbumBand>', cast(2553 as long)), ('relationship<AlbumBand>', cast(4060 as long))"},
                {"select band from Album", "values ('relationship<AlbumBand>'), ('relationship<AlbumBand>'), ('relationship<AlbumBand>')"}};
    }

    @Test(dataProvider = "graphMDLUsedCases")
    public void testGraphMDLRewrite(String original, String expected)
    {
        String actualSql = rewrite(original);
        assertQuery(actualSql, expected);
    }

    private void assertQuery(String acutal, String expected)
    {
        assertThat(query(acutal)).isEqualTo(query(expected));
    }

    @DataProvider
    public Object[][] noRewriteCase()
    {
        return new Object[][] {
                {"select 1, 2, 3"},
                {"select id, name from normalTable"},
                {"with normalCte as (select id, name from normalTable) select id, name from normalCte"},
                {"SELECT graphmdl.test.Album.id FROM catalog.schema.Album"},
        };
    }

    @Test(dataProvider = "noRewriteCase")
    public void testGraphMDLNoRewrite(String original)
    {
        Statement expectedState = SQL_PARSER.createStatement(original, new ParsingOptions(AS_DECIMAL));
        assertThat(rewrite(original)).isEqualTo(SqlFormatter.formatSql(expectedState));
    }

    private String rewrite(String sql)
    {
        return GraphMDLPlanner.rewrite(sql, DEFAULT_SESSION_CONTEXT, graphMDL);
    }
}
