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

package io.trino.sql;

import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSqlFormatter
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testFormatJoin()
    {
        String sql = "SELECT * FROM a JOIN b ON a.x = b.y";
        String formattedSql = SqlFormatter.formatSql(SQL_PARSER.createStatement(sql, new ParsingOptions()));
        assertEquals("""
                SELECT *
                FROM
                  a
                INNER JOIN b ON (a.x = b.y)
                """, formattedSql);

        sql = "SELECT * FROM a JOIN b ON a.x = b.y JOIN c ON a.x = c.z";
        formattedSql = SqlFormatter.formatSql(SQL_PARSER.createStatement(sql, new ParsingOptions()));
        assertEquals("""
                SELECT *
                FROM
                  a
                INNER JOIN b ON (a.x = b.y)
                INNER JOIN c ON (a.x = c.z)
                """, formattedSql);
    }

    @Test
    public void testFormatAliasJoin()
    {
        String sql = "SELECT * FROM (a JOIN b ON a.x = b.y) t1";
        String formattedSql = SqlFormatter.formatSql(SQL_PARSER.createStatement(sql, new ParsingOptions()));
        assertEquals("""
                SELECT *
                FROM
                  ( a
                   INNER JOIN b ON (a.x = b.y)) t1
                """, formattedSql);
        sql = "SELECT * FROM ((a JOIN b ON a.x = b.y) t1 join c on t1.x = c.y)";
        formattedSql = SqlFormatter.formatSql(SQL_PARSER.createStatement(sql, new ParsingOptions()));
        assertEquals("""
                SELECT *
                FROM
                  ( a
                   INNER JOIN b ON (a.x = b.y)) t1
                INNER JOIN c ON (t1.x = c.y)
                """, formattedSql);
    }
}
