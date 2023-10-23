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
import io.accio.base.dto.JoinType;
import io.accio.testing.AbstractTestFramework;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.tree.Statement;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static io.accio.base.AccioTypes.INTEGER;
import static io.accio.base.AccioTypes.VARCHAR;
import static io.accio.base.dto.Column.column;
import static io.accio.base.dto.Column.relationshipColumn;
import static io.accio.base.dto.EnumDefinition.enumDefinition;
import static io.accio.base.dto.EnumValue.enumValue;
import static io.accio.base.dto.Metric.metric;
import static io.accio.base.dto.Model.model;
import static io.accio.base.dto.Relationship.relationship;
import static io.accio.base.dto.View.view;
import static io.accio.sqlrewrite.Utils.SQL_PARSER;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAllRulesRewrite
        extends AbstractTestFramework
{
    private final AccioMDL accioMDL;

    public TestAllRulesRewrite()
    {
        accioMDL = AccioMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(List.of(
                        model("Album",
                                "select * from (values (1, 'Gusare', 1, 2560, 'I', 'IN_STOCK'), " +
                                        "(2, 'HisoHiso Banashi', 1, 1500, 'O', 'OUT_OF_STOCK'), " +
                                        "(3, 'Dakara boku wa ongaku o yameta', 2, 2553, 'I', 'IN_STOCK')) " +
                                        "Album(id, name, bandId, price, status, statusA)",
                                List.of(
                                        column("id", INTEGER, null, true),
                                        column("name", VARCHAR, null, true),
                                        relationshipColumn("band", "Band", "AlbumBand"),
                                        column("price", INTEGER, null, true),
                                        column("bandId", INTEGER, null, true),
                                        column("status", "Inventory", null, true),
                                        column("statusA", "InventoryA", null, true),
                                        relationshipColumn("orders", "Order", "AlbumOrder")),
                                "id"),
                        model("Band",
                                "select * from (values (1, 'ZUTOMAYO'), " +
                                        "(2, 'Yorushika')) " +
                                        "Band(id, name)",
                                List.of(
                                        column("id", INTEGER, null, true),
                                        column("name", VARCHAR, null, true),
                                        relationshipColumn("albums", "Album", "AlbumBand")),
                                "id"),
                        model("Order", "select * from (values (1, 1), (2, 1), (3, 2), (4, 3)) Orders(orderkey, albumId)",
                                List.of(
                                        column("orderkey", INTEGER, null, true),
                                        column("albumId", INTEGER, null, true)),
                                "orderkey")))
                .setRelationships(List.of(
                        relationship("AlbumBand", List.of("Album", "Band"), JoinType.MANY_TO_ONE, "Album.bandId = Band.id"),
                        relationship("AlbumOrder", List.of("Album", "Order"), JoinType.ONE_TO_MANY,
                                // It's hard to quote the reserve word in the user-defined join condition.
                                // We should ask users to quote the identifier by themselves if it's a reserve word.
                                "Album.id = \"Order\".albumId")))
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
                                // TODO: we don't support to output a relationship column in a metric.
                                //  It just can be a group by key but can't be accessed with other relationship operation. e.g. `band.name`.
                                List.of(column("band", VARCHAR, null, true, null)),
                                List.of(column("price", INTEGER, null, true, "sum(Album.price)")),
                                List.of())))
                .setEnumDefinitions(List.of(
                        enumDefinition("Inventory", List.of(enumValue("IN_STOCK", "I"), enumValue("OUT_OF_STOCK", "O"))),
                        enumDefinition("InventoryA", List.of(enumValue("IN_STOCK"), enumValue("OUT_OF_STOCK")))))
                .setViews(List.of(
                        view("UseModel", "select * from Album"),
                        view("useMetric", "select band, price from Collection")))
                .build());
    }

    @DataProvider
    public Object[][] accioUsedCases()
    {
        return new Object[][] {
                {"select name, price from Album",
                        "values('Gusare', 2560), ('HisoHiso Banashi', 1500), ('Dakara boku wa ongaku o yameta', 2553)"},
                {"SELECT name, price FROM accio.test.Album",
                        "values('Gusare', 2560), ('HisoHiso Banashi', 1500), ('Dakara boku wa ongaku o yameta', 2553)"},
                {"select band, price from useMetric", "values  ('Yorushika', cast(2553 as long)), ('ZUTOMAYO', cast(4060 as long))"},
                {"select * from \"Order\"", "values (1, 1), (2, 1), (3, 2), (4, 3)"},
        };
    }

    @Test(dataProvider = "accioUsedCases")
    public void testAccioRewrite(String original, String expected)
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
                {"SELECT accio.test.Album.id FROM catalog.schema.Album"},
        };
    }

    @Test(dataProvider = "noRewriteCase")
    public void testAccioNoRewrite(String original)
    {
        Statement expectedState = SQL_PARSER.createStatement(original, new ParsingOptions(AS_DECIMAL));
        assertThat(rewrite(original)).isEqualTo(SqlFormatter.formatSql(expectedState));
    }

    private String rewrite(String sql)
    {
        return AccioPlanner.rewrite(sql, DEFAULT_SESSION_CONTEXT, accioMDL);
    }
}
