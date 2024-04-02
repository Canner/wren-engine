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

package io.wren.sqlglot.converter;

import io.wren.base.SessionContext;
import io.wren.sqlglot.TestingSQLGlotServer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.wren.sqlglot.glot.SQLGlot.Dialect.BIGQUERY;
import static io.wren.sqlglot.glot.SQLGlot.Dialect.DUCKDB;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSQLGlotConverter
{
    private static final SessionContext DEFAULT_SESSION_CONTEXT = SessionContext.builder().build();
    private TestingSQLGlotServer testingSQLGlotServer;

    @BeforeClass
    public void setup()
    {
        testingSQLGlotServer = new TestingSQLGlotServer();
    }

    @AfterMethod
    public void close()
    {
        testingSQLGlotServer.close();
    }

    @Test
    public void testGenerateArray()
    {
        SQLGlotConverter sqlGlotConverter = SQLGlotConverter.builder()
                .setReadDialect(BIGQUERY)
                .setWriteDialect(DUCKDB)
                .build();

        assertConvert(sqlGlotConverter, "SELECT generate_array(1, 10)", "SELECT GENERATE_SERIES(1, 10)");
    }

    @Test
    public void testSubstring()
    {
        SQLGlotConverter sqlGlotConverter = SQLGlotConverter.builder()
                .setWriteDialect(BIGQUERY)
                .build();

        assertConvert(sqlGlotConverter, "SELECT substring('Thomas' from 2 for 3)", "SELECT SUBSTRING('Thomas', 2, 3)");
    }

    @Test
    public void testUnnest()
    {
        SQLGlotConverter sqlGlotConverter = SQLGlotConverter.builder()
                .setWriteDialect(BIGQUERY)
                .build();

        assertConvert(sqlGlotConverter, "SELECT a.id FROM UNNEST(ARRAY[1]) as a(id)", "SELECT id FROM UNNEST([1]) AS id");
    }

    @Test
    public void testArray()
    {
        SQLGlotConverter sqlGlotConverter = SQLGlotConverter.builder()
                .setWriteDialect(DUCKDB)
                .build();

        assertConvert(sqlGlotConverter, "SELECT ARRAY[1,2,3][1]", "SELECT ([1, 2, 3])[1]");
    }

    private static void assertConvert(SQLGlotConverter sqlGlotConverter, String sql, String expected)
    {
        assertThat(sqlGlotConverter.convert(sql, DEFAULT_SESSION_CONTEXT)).isEqualTo(expected);
    }
}
