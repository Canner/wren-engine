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
import io.graphmdl.base.GraphMDLTypes;
import io.graphmdl.base.dto.JoinType;
import io.graphmdl.base.dto.Model;
import io.graphmdl.base.dto.Relationship;
import io.graphmdl.testing.AbstractTestFramework;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import org.testng.annotations.Test;

import java.util.List;

import static io.graphmdl.base.dto.Column.column;
import static io.graphmdl.base.dto.Relationship.SortKey.sortKey;
import static io.graphmdl.base.dto.Relationship.relationship;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestGroupByKeyRewrite
        extends AbstractTestFramework
{
    private final GraphMDL oneToManyGraphMDL;
    private static final SqlParser SQL_PARSER = new SqlParser();

    public TestGroupByKeyRewrite()
    {
        oneToManyGraphMDL = GraphMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(List.of(
                        Model.model("Book",
                                "select * from (values (1, 'book1', 1), (2, 'book2', 2), (3, 'book3', 1)) Book(bookId, name, authorId)",
                                List.of(
                                        column("bookId", GraphMDLTypes.INTEGER, null, true),
                                        column("name", GraphMDLTypes.VARCHAR, null, true),
                                        column("author", "People", "PeopleBook", true),
                                        column("author_reverse", "People", "BookPeople", true),
                                        column("authorId", GraphMDLTypes.INTEGER, null, true)),
                                "bookId"),
                        Model.model("People",
                                "select * from (values (1, 'user1'), (2, 'user2')) People(userId, name)",
                                List.of(
                                        column("userId", GraphMDLTypes.INTEGER, null, true),
                                        column("name", GraphMDLTypes.VARCHAR, null, true),
                                        column("books", "Book", "PeopleBook", true),
                                        column("sorted_books", "Book", "PeopleBookOrderByName", true)),
                                "userId")))
                .setRelationships(List.of(
                        relationship("PeopleBook", List.of("People", "Book"), JoinType.ONE_TO_MANY, "People.userId = Book.authorId"),
                        relationship("BookPeople", List.of("Book", "People"), JoinType.MANY_TO_ONE, "Book.authorId = People.userId"),
                        relationship("PeopleBookOrderByName", List.of("People", "Book"), JoinType.ONE_TO_MANY, "People.userId = Book.authorId",
                                List.of(sortKey("name", Relationship.SortKey.Ordering.ASC), sortKey("bookId", Relationship.SortKey.Ordering.DESC)))))
                .build());
    }

    @Test
    public void testBasic()
    {
        String sql = "select count(*) from Book group by author";
        String expected = "select count(*) from Book group by author.userId";
        assertThat(rewrite(sql)).isEqualTo(parse(expected));
    }

    @Test
    public void testGroupByIndex()
    {
        String sql = "select author, count(*) from Book group by 1";
        String expected = "select author, count(*) from Book group by author.userId";
        assertThat(rewrite(sql)).isEqualTo(parse(expected));
    }

    private Statement rewrite(String sql)
    {
        return GroupByKeyRewrite.GROUP_BY_KEY_REWRITE.apply(parse(sql), DEFAULT_SESSION_CONTEXT, oneToManyGraphMDL);
    }

    private Statement parse(String sql)
    {
        return SQL_PARSER.createStatement(sql, new ParsingOptions());
    }
}
