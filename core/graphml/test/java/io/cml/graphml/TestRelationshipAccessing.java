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

import io.cml.graphml.analyzer.Analysis;
import io.cml.graphml.analyzer.StatementAnalyzer;
import io.cml.graphml.base.GraphML;
import io.cml.graphml.base.GraphMLTypes;
import io.cml.graphml.base.dto.JoinType;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Statement;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.cml.graphml.base.dto.Column.column;
import static io.cml.graphml.base.dto.Manifest.manifest;
import static io.cml.graphml.base.dto.Model.model;
import static io.cml.graphml.base.dto.Relationship.relationship;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestRelationshipAccessing
{
    private final GraphML graphML;
    private final RelationshipCteGenerator generator;

    private static final String EXPECTED_WITH_QUERIES = "WITH\n" +
            "  ${rs1} (id, name, book) AS (\n" +
            "   SELECT\n" +
            "     r.id\n" +
            "   , r.name\n" +
            "   , r.book\n" +
            "   FROM\n" +
            "     (Book l\n" +
            "   LEFT JOIN User r ON (l.authorId = r.id))\n" +
            ") \n" +
            ", ${rs2} (id, name, author) AS (\n" +
            "   SELECT\n" +
            "     r.id\n" +
            "   , r.name\n" +
            "   , r.author\n" +
            "   FROM\n" +
            "     (${rs1} l\n" +
            "   LEFT JOIN Book r ON (l.id = r.authorId))\n" +
            ") \n" +
            ", ${rs3} (id, name, book) AS (\n" +
            "   SELECT\n" +
            "     r.id\n" +
            "   , r.name\n" +
            "   , r.book\n" +
            "   FROM\n" +
            "     (${rs2} l\n" +
            "   LEFT JOIN User r ON (l.authorId = r.id))\n" +
            ") \n";

    public TestRelationshipAccessing()
    {
        graphML = GraphML.fromManifest(manifest(
                List.of(model("Book",
                                "select * from (values (1, 'book1', 1), (2, 'book2', 2), (3, 'book3', 3)) book(id, name, authorId)",
                                List.of(
                                        column("id", GraphMLTypes.INTEGER, null, true),
                                        column("name", GraphMLTypes.VARCHAR, null, true),
                                        column("author", "User", "BookUser", true))),
                        model("User",
                                "select * from (values (1, 'user1'), (2, 'user2'), (3, 'user3')) user(id, name))",
                                List.of(
                                        column("id", GraphMLTypes.INTEGER, null, true),
                                        column("name", GraphMLTypes.VARCHAR, null, true),
                                        column("book", "Book", "BookUser", true)))),
                List.of(relationship("BookUser", List.of("Book", "User"), JoinType.ONE_TO_ONE, "book.authorId  = user.id")),
                List.of()));
    }

    @DataProvider
    public Object[][] relationshipAccessCases()
    {
        return new Object[][] {
                // TODO: enable this test
//                {"select c1.s1.Book.author.book.author.name,\n" +
//                        "s1.Book.author.book.author.name,\n" +
//                        "Book.author.book.author.name\n" +
//                        "from c1.s1.Book",
//                        EXPECTED_WITH_QUERIES +
//                                "SELECT ${rs3}.name, ${rs3}.name, ${rs3}.name\n" +
//                                "FROM\n" +
//                                "  c1.s1.Book\n" +
//                                ", ${rs1}\n" +
//                                ", ${rs2}\n" +
//                                ", ${rs3}\n"},
                {"select author.book.author.name,\n" +
                        "author.book.name,\n" +
                        "author.name\n" +
                        "from Book",
                        EXPECTED_WITH_QUERIES +
                                "SELECT ${rs3}.name, ${rs2}.name, ${rs1}.name\n" +
                                "FROM\n" +
                                "  Book\n" +
                                ", ${rs1}\n" +
                                ", ${rs2}\n" +
                                ", ${rs3}\n"},
                {"select name from Book where author.book.author.name = 'jax'",
                        EXPECTED_WITH_QUERIES +
                                "SELECT name\n" +
                                "FROM\n" +
                                "  Book\n" +
                                ", ${rs1}\n" +
                                ", ${rs2}\n" +
                                ", ${rs3}\n" +
                                "WHERE\n" +
                                "  ${rs3}.name = 'jax'"},
                {"select name, author.book.author.name from Book group by author.book.author.name having author.book.name = 'destiny'",
                        EXPECTED_WITH_QUERIES +
                                "SELECT name, ${rs3}.name\n" +
                                "FROM\n" +
                                "  Book\n" +
                                ", ${rs1}\n" +
                                ", ${rs2}\n" +
                                ", ${rs3}\n" +
                                "GROUP BY\n" +
                                "  ${rs3}.name\n" +
                                "HAVING\n" +
                                "  ${rs2}.name = 'destiny'"},
                {"select name, author.book.author.name from Book order by author.book.author.name",
                        EXPECTED_WITH_QUERIES +
                                "SELECT name, ${rs3}.name\n" +
                                "FROM\n" +
                                "  Book\n" +
                                ", ${rs1}\n" +
                                ", ${rs2}\n" +
                                ", ${rs3}\n" +
                                "ORDER BY\n" +
                                "  ${rs3}.name"},
        };
    }

    @Test(dataProvider = "relationshipAccessCases")
    public void testRelationshipAccessingRewrite(String original, String expected)
    {
        SqlParser SQL_PARSER = new SqlParser();
        Statement statement = SQL_PARSER.createStatement(original, new ParsingOptions(AS_DECIMAL));
        RelationshipCteGenerator generator = new RelationshipCteGenerator(graphML);
        Analysis analysis = StatementAnalyzer.analyze(statement, graphML, generator);

        assertThat(generator.getRegisteredCte().size()).isEqualTo(3);

        Map<String, String> replaceMap = new HashMap<>();
        replaceMap.put("rs1", generator.getNameMapping().computeIfAbsent("Book.author", ignored -> {
            throw new AssertionError("Book.author name mapping not found");
        }));
        replaceMap.put("rs2", generator.getNameMapping().computeIfAbsent("Book.author.book", ignored -> {
            throw new AssertionError("Book.author.book cte name mapping not found");
        }));
        replaceMap.put("rs3", generator.getNameMapping().computeIfAbsent("Book.author.book.author", ignored -> {
            throw new AssertionError("Book.author.book.author cte name mapping not found");
        }));

        Node result = RelationshipRewrite.RELATIONSHIP_REWRITE.apply(statement, analysis, graphML);
        Statement expectedResult = SQL_PARSER.createStatement(new StrSubstitutor(replaceMap).replace(expected), new ParsingOptions(AS_DECIMAL));
        assertThat(SqlFormatter.formatSql(result)).isEqualTo(SqlFormatter.formatSql(expectedResult));
    }
}
