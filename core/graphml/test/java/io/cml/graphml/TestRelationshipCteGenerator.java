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

package io.cml.graphml;

import io.cml.graphml.base.GraphML;
import io.cml.graphml.base.GraphMLTypes;
import io.cml.graphml.base.dto.JoinType;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.With;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.cml.graphml.RelationshipCteGenerator.RsItem;
import static io.cml.graphml.RelationshipCteGenerator.RsItem.rsItem;
import static io.cml.graphml.base.dto.Column.column;
import static io.cml.graphml.base.dto.Manifest.manifest;
import static io.cml.graphml.base.dto.Model.model;
import static io.cml.graphml.base.dto.Relationship.relationship;
import static io.trino.sql.QueryUtil.selectAll;
import static io.trino.sql.QueryUtil.simpleQuery;
import static io.trino.sql.QueryUtil.unaliasedName;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;

public class TestRelationshipCteGenerator
{
    @Test
    public void testGenerate()
    {
        GraphML graphML = GraphML.fromManifest(manifest(
                List.of(model("Book",
                                "select * from (values (1, 'book1', 1), (2, 'book2', 2), (3, 'book3', 3) book(id, name, authorId)",
                                List.of(
                                        column("id", GraphMLTypes.INTEGER, null, true),
                                        column("name", GraphMLTypes.VARCHAR, null, true),
                                        column("authorId", GraphMLTypes.INTEGER, null, true))),
                        model("User",
                                "select * from (values (1, 'user1'), (2, 'user2'), (3, 'user3')) user(id, name))",
                                List.of(
                                        column("id", GraphMLTypes.INTEGER, null, true),
                                        column("name", GraphMLTypes.VARCHAR, null, true)))),
                List.of(relationship("BookUser", List.of("Book", "User"), JoinType.ONE_TO_ONE, "book.authorId  = user.id")),
                List.of()));

        RelationshipCteGenerator generator = new RelationshipCteGenerator(graphML);
        Map<String, List<RsItem>> relationShipRefs = new HashMap<>();
        relationShipRefs.put("author", List.of(rsItem("BookUser", RsItem.Type.RS)));
        relationShipRefs.put("author.book", List.of(rsItem("author", RsItem.Type.CTE), rsItem("BookUser", RsItem.Type.REVERSE_RS)));
        relationShipRefs.put("author.book.author", List.of(rsItem("author.book", RsItem.Type.CTE), rsItem("BookUser", RsItem.Type.RS)));
        relationShipRefs.forEach(generator::register);
        Query query = new Query(
                Optional.of(new With(false, new ArrayList<>(generator.getRegisteredCte().values()))),
                simpleQuery(selectAll(List.of(unaliasedName("author.book.author")))).getQueryBody(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        System.out.println(SqlFormatter.formatSql(query));
    }

    @Test
    public void testRsRewrite()
    {
        GraphML graphML = GraphML.fromManifest(manifest(
                List.of(model("Book",
                                "select * from (values (1, 'book1', 1), (2, 'book2', 2), (3, 'book3', 3)) book(id, name, authorId)",
                                List.of(
                                        column("id", GraphMLTypes.INTEGER, null, true),
                                        column("name", GraphMLTypes.VARCHAR, null, true),
                                        column("author", "User", null, true))),
                        model("User",
                                "select * from (values (1, 'user1'), (2, 'user2'), (3, 'user3')) user(id, name))",
                                List.of(
                                        column("id", GraphMLTypes.INTEGER, null, true),
                                        column("name", GraphMLTypes.VARCHAR, null, true),
                                        column("book", "Book", null, true)))),
                List.of(relationship("BookUser", List.of("Book", "User"), JoinType.ONE_TO_ONE, "book.authorId  = user.id")),
                List.of()));

        RelationshipCteGenerator generator = new RelationshipCteGenerator(graphML);
        Map<String, List<RelationshipCteGenerator.RsItem>> relationShipRefs = ImmutableMap.<String, List<RelationshipCteGenerator.RsItem>>builder()
                .put("author", List.of(rsItem("BookUser", RelationshipCteGenerator.RsItem.Type.RS)))
                .put("author.book", List.of(rsItem("author", RelationshipCteGenerator.RsItem.Type.CTE), rsItem("BookUser", RelationshipCteGenerator.RsItem.Type.REVERSE_RS)))
                .put("author.book.author", List.of(rsItem("author.book", RelationshipCteGenerator.RsItem.Type.CTE), rsItem("BookUser", RelationshipCteGenerator.RsItem.Type.RS)))
                .build();
        relationShipRefs.forEach(generator::register);

        SqlParser SQL_PARSER = new SqlParser();
        String sql = "select author.book.author.name from Book";
        Statement statement = SQL_PARSER.createStatement(sql, new ParsingOptions(AS_DECIMAL));
        Analyzer.Analysis analysis = Analyzer.analyze(statement, generator);

        Node result = RelationshipRewrite.RELATIONSHIP_REWRITE.apply(statement, analysis, graphML);
        System.out.printf(SqlFormatter.formatSql(result));
    }
}
