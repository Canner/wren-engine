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

import io.trino.sql.tree.Statement;
import io.wren.base.AnalyzedMDL;
import io.wren.base.WrenMDL;
import io.wren.base.WrenTypes;
import io.wren.base.dto.Column;
import io.wren.base.dto.EnumDefinition;
import io.wren.base.dto.EnumValue;
import io.wren.base.dto.JoinType;
import io.wren.base.dto.Metric;
import io.wren.base.dto.Model;
import io.wren.base.dto.Relationship;
import io.wren.base.dto.View;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.sql.SqlFormatter.formatSql;
import static io.wren.base.sqlrewrite.Utils.parseSql;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAllRulesRewrite
        extends AbstractTestFramework
{
    private final WrenMDL wrenMDL;

    public TestAllRulesRewrite()
    {
        wrenMDL = WrenMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(List.of(
                        Model.model("Album",
                                "select * from (values (1, 'Gusare', 1, 2560, 'I', 'IN_STOCK'), " +
                                        "(2, 'HisoHiso Banashi', 1, 1500, 'O', 'OUT_OF_STOCK'), " +
                                        "(3, 'Dakara boku wa ongaku o yameta', 2, 2553, 'I', 'IN_STOCK')) " +
                                        "Album(id, name, bandId, price, status, statusA)",
                                List.of(
                                        Column.column("id", WrenTypes.INTEGER, null, true),
                                        Column.column("name", WrenTypes.VARCHAR, null, true),
                                        Column.relationshipColumn("band", "Band", "AlbumBand"),
                                        Column.column("price", WrenTypes.INTEGER, null, true),
                                        Column.column("bandId", WrenTypes.INTEGER, null, true),
                                        Column.calculatedColumn("bandName", WrenTypes.VARCHAR, "band.name"),
                                        Column.column("status", "Inventory", null, true),
                                        Column.column("statusA", "InventoryA", null, true),
                                        Column.relationshipColumn("orders", "Order", "AlbumOrder")),
                                "id"),
                        Model.model("Band",
                                "select * from (values (1, 'ZUTOMAYO'), " +
                                        "(2, 'Yorushika')) " +
                                        "Band(id, name)",
                                List.of(
                                        Column.column("id", WrenTypes.INTEGER, null, true),
                                        Column.column("name", WrenTypes.VARCHAR, null, true),
                                        Column.relationshipColumn("albums", "Album", "AlbumBand")),
                                "id"),
                        Model.model("Order", "select * from (values (1, 1), (2, 1), (3, 2), (4, 3)) Orders(orderkey, albumId)",
                                List.of(
                                        Column.column("orderkey", WrenTypes.INTEGER, null, true),
                                        Column.column("albumId", WrenTypes.INTEGER, null, true)),
                                "orderkey")))
                .setRelationships(List.of(
                        Relationship.relationship("AlbumBand", List.of("Album", "Band"), JoinType.MANY_TO_ONE, "Album.bandId = Band.id"),
                        Relationship.relationship("AlbumOrder", List.of("Album", "Order"), JoinType.ONE_TO_MANY,
                                // It's hard to quote the reserve word in the user-defined join condition.
                                // We should ask users to quote the identifier by themselves if it's a reserve word.
                                "Album.id = \"Order\".albumId")))
                .setMetrics(List.of(
                        Metric.metric(
                                "Collection",
                                "Album",
                                List.of(Column.column("band", WrenTypes.VARCHAR, null, true, "bandName")),
                                List.of(Column.column("price", WrenTypes.INTEGER, null, true, "sum(Album.price)")),
                                List.of()),
                        Metric.metric(
                                "CollectionA",
                                "Album",
                                // TODO: we don't support to output a relationship column in a metric.
                                //  It just can be a group by key but can't be accessed with other relationship operation. e.g. `band.name`.
                                List.of(Column.column("band", WrenTypes.VARCHAR, null, true, null)),
                                List.of(Column.column("price", WrenTypes.INTEGER, null, true, "sum(Album.price)")),
                                List.of())))
                .setEnumDefinitions(List.of(
                        EnumDefinition.enumDefinition("Inventory", List.of(EnumValue.enumValue("IN_STOCK", "I"), EnumValue.enumValue("OUT_OF_STOCK", "O"))),
                        EnumDefinition.enumDefinition("InventoryA", List.of(EnumValue.enumValue("IN_STOCK"), EnumValue.enumValue("OUT_OF_STOCK")))))
                .setViews(List.of(
                        View.view("UseModel", "select * from Album"),
                        View.view("useMetric", "select band, price from Collection")))
                .build());
    }

    @DataProvider
    public Object[][] wrenUsedCases()
    {
        return new Object[][] {
                {"select name, price from Album",
                        "values('Gusare', 2560), ('HisoHiso Banashi', 1500), ('Dakara boku wa ongaku o yameta', 2553)"},
                {"SELECT name, price FROM wren.test.Album",
                        "values('Gusare', 2560), ('HisoHiso Banashi', 1500), ('Dakara boku wa ongaku o yameta', 2553)"},
                {"select band, cast(price as integer) from useMetric order by band", "values  ('Yorushika', 2553), ('ZUTOMAYO', 4060)"},
                {"select * from \"Order\"", "values (1, 1), (2, 1), (3, 2), (4, 3)"},
                {"select name, price from Album where id in (select albumId from \"Order\")",
                        "values('Gusare', 2560), ('HisoHiso Banashi', 1500), ('Dakara boku wa ongaku o yameta', 2553)"},
                {"select name, price from Album where id not in (select albumId from \"Order\")",
                        "values(1, 1) limit 0"},
                {"select * from (select name ,price from Album where bandId = 1 union select name, price from Album where bandId = 2) order by price",
                        "values('HisoHiso Banashi', 1500), ('Dakara boku wa ongaku o yameta', 2553), ('Gusare', 2560)"},
                {"select * from (select name ,price from Album where bandId = 1 except select name, price from Album where bandId = 2) order by price",
                        "values('HisoHiso Banashi', 1500), ('Gusare', 2560)"},
                {"select * from (select name ,price from Album where bandId = 1 intersect select name, price from Album where bandId = 2) order by price",
                        "values(1, 1) limit 0"}
        };
    }

    @Test(dataProvider = "wrenUsedCases")
    public void testWrenRewrite(String original, String expected)
    {
        String actualSql = rewrite(original);
        assertQuery(actualSql, expected);
    }

    private void assertQuery(String actual, String expected)
    {
        assertThat(query(actual)).isEqualTo(query(expected));
    }

    @DataProvider
    public Object[][] noRewriteCase()
    {
        return new Object[][] {
                {"select 1, 2, 3"},
                {"select id, name from normalTable"},
                {"with normalCte as (select id, name from normalTable) select id, name from normalCte"},
                {"SELECT Album.id FROM catalog.schema.Album"},
        };
    }

    @Test(dataProvider = "noRewriteCase")
    public void testWrenNoRewrite(String original)
    {
        Statement expectedState = parseSql(original);
        assertThat(rewrite(original)).isEqualTo(formatSql(expectedState));
    }

    // TODO: The scope of QuerySpecification is wrong. Enable it after fixing the scope.
    @Test(enabled = false)
    public void testSetOperationColumnNoMatch()
    {
        assertThatThrownBy(() -> rewrite("select name, price from Album union select price from Album"))
                .hasMessageFindingMatch("query has different number of fields: expected 2, found 1");
    }

    private String rewrite(String sql)
    {
        return WrenPlanner.rewrite(sql, DEFAULT_SESSION_CONTEXT, new AnalyzedMDL(wrenMDL, null));
    }
}
