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

package io.wren.testing.sqlglot;

import io.wren.base.SessionContext;
import io.wren.sql.converter.SQLGlotConverter;
import io.wren.sql.glot.SQLGlot;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSQLGlot
{
    SQLGlotConverter sqlGlotConverter;

    @BeforeClass
    public void setup()
    {
        sqlGlotConverter = new SQLGlotConverter();
    }

    @Test
    public void testGenerateArray()
    {
        SessionContext sessionContext = SessionContext.builder()
                .setReadDialect(SQLGlot.Dialect.BIGQUERY.getDialect())
                .setWriteDialect(SQLGlot.Dialect.DUCKDB.getDialect())
                .build();
        assertThat(sqlGlotConverter.convert("SELECT generate_array(1, 10)", sessionContext))
                .isEqualTo("SELECT GENERATE_SERIES(1, 10)");
    }

    @Test
    public void testSubstring()
    {
        SessionContext sessionContext = SessionContext.builder()
                .setReadDialect(SQLGlot.Dialect.POSTGRES.getDialect())
                .setWriteDialect(SQLGlot.Dialect.BIGQUERY.getDialect())
                .build();
        assertThat(sqlGlotConverter.convert("SELECT substring('Thomas' from 2 for 3)", sessionContext))
                .isEqualTo("SELECT SUBSTRING('Thomas', 2, 3)");
    }

    @Test
    public void testUnnest()
    {
        SessionContext sessionContext = SessionContext.builder()
                .setReadDialect(SQLGlot.Dialect.POSTGRES.getDialect())
                .setWriteDialect(SQLGlot.Dialect.BIGQUERY.getDialect())
                .build();
        assertThat(sqlGlotConverter.convert("SELECT a.id FROM UNNEST(ARRAY[1]) as a(id)", sessionContext))
                .isEqualTo("SELECT id FROM UNNEST([1]) AS id");
    }

    @Test
    public void testArray()
    {
        SessionContext sessionContext = SessionContext.builder()
                .setReadDialect(SQLGlot.Dialect.TRINO.getDialect())
                .setWriteDialect(SQLGlot.Dialect.DUCKDB.getDialect())
                .build();
        assertThat(sqlGlotConverter.convert("SELECT ARRAY[1,2,3][1]", sessionContext))
                .isEqualTo("SELECT array_value(1, 2, 3)[1]");
    }
}
