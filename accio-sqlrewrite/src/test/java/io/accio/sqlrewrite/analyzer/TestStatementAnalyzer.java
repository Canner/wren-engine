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

package io.accio.sqlrewrite.analyzer;

import io.accio.base.CatalogSchemaTableName;
import io.accio.base.SessionContext;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import org.testng.annotations.Test;

import static io.accio.base.AccioMDL.EMPTY;
import static io.accio.sqlrewrite.analyzer.StatementAnalyzer.analyze;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStatementAnalyzer
{
    public static final SqlParser sqlParser = new SqlParser();

    @Test
    public void testValues()
    {
        SessionContext sessionContext = SessionContext.builder().build();
        analyze(sqlParser.createStatement("VALUES(1, 'a')", new ParsingOptions(AS_DECIMAL)), sessionContext, EMPTY);
        analyze(sqlParser.createStatement("SELECT * FROM (VALUES(1, 'a'))", new ParsingOptions(AS_DECIMAL)), sessionContext, EMPTY);
    }

    @Test
    public void testGetTableWithoutWithTable()
    {
        SessionContext sessionContext = SessionContext.builder().setCatalog("test").setSchema("test").build();
        Analysis analysis = analyze(
                sqlParser.createStatement("WITH a AS (SELECT * FROM People) SELECT * FROM a", new ParsingOptions(AS_DECIMAL)),
                sessionContext,
                EMPTY);

        assertThat(analysis.getTables()).containsExactly(new CatalogSchemaTableName("test", "test", "People"));
    }
}
