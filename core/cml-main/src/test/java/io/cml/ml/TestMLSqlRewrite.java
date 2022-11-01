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

package io.cml.ml;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMLSqlRewrite
{
    @Test
    public void testGraphMLStore()
            throws JsonProcessingException
    {
        GraphMLStore graphMLStore = new GraphMLStore();
        Optional<GraphML.Model> model = graphMLStore.getModel("Book");
        assertThat(model.isPresent()).isTrue();
        assertThat(model.get().name).isEqualTo("Book");
    }

    @Test
    public void testModel()
            throws JsonProcessingException
    {
        GraphMLStore graphMLStore = new GraphMLStore("" +
                "{\n" +
                "    \"models\": [\n" +
                "        {\n" +
                "            \"name\": \"User\",\n" +
                "            \"refSql\": \"SELECT * FROM User\",\n" +
                "            \"columns\": [\n" +
                "                {\n" +
                "                    \"name\": \"username\",\n" +
                "                    \"type\": \"STRING\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"name\": \"email\",\n" +
                "                    \"type\": \"STRING\"\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}");

        SqlParser sqlParser = new SqlParser();
        Statement stmt = sqlParser.createStatement("SELECT * FROM Book", new ParsingOptions(AS_DECIMAL));
        Statement rewrittenStmt = MLSqlRewrite.rewrite(stmt, graphMLStore);
        String rewrittenSql = SqlFormatter.formatSql(rewrittenStmt);
        assertThat(rewrittenSql).isEqualTo(
                "SELECT author\n" +
                "FROM\n" +
                "  (\n" +
                "   SELECT *\n" +
                "   FROM\n" +
                "     Book\n" +
                ") \n");
    }

//    @Test
//    public void testRewrite()
//    {
//        SqlParser sqlParser = new SqlParser();
//        Statement statement = sqlParser.createStatement("SELECT * FROM foo", new ParsingOptions(AS_DECIMAL));
//        Statement rewrite = MLSqlRewrite.rewrite(statement, new GraphMLStore() {
//            @Override
//            public List<String> listModels()
//            {
//                return GraphMLStore.super.listModels();
//            }
//
//            @Override
//            public String getModelSql(String name)
//            {
//                return GraphMLStore.super.getModelSql(name);
//            }
//        });
//
//        String rewrittenSql = SqlFormatter.formatSql(rewrite);
//        assertThat(rewrittenSql).isEqualTo("SELECT *\n" +
//                "FROM\n" +
//                "  SELECT col_1\n" +
//                "FROM\n" +
//                "  bar\n\n"); // TODO: remove the extra \n
//    }
}
