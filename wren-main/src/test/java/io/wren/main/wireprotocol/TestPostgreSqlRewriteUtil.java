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

package io.wren.main.wireprotocol;

import io.wren.main.wireprotocol.patterns.PostgreSqlRewriteUtil;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestPostgreSqlRewriteUtil
{
    @DataProvider(name = "sql")
    public Object[][] createSQL()
    {
        return new Object[][] {
                new Object[] {"SELECT 1", "SELECT 1"},
                new Object[] {"SHOW TRANSACTION_ISOLATION", "SELECT * FROM (VALUES(ROW('read uncommitted'))) RESPONSE(transaction_isolation)"},
                new Object[] {"show transaction_isolation", "SELECT * FROM (VALUES(ROW('read uncommitted'))) RESPONSE(transaction_isolation)"},
                new Object[] {"   SHOW TRANSACTION_ISOLATION", "SELECT * FROM (VALUES(ROW('read uncommitted'))) RESPONSE(transaction_isolation)"},
                new Object[] {"SHOW   TRANSACTION_ISOLATION", "SELECT * FROM (VALUES(ROW('read uncommitted'))) RESPONSE(transaction_isolation)"},
                new Object[] {"SHOW TRANSACTION ISOLATION LEVEL", "SELECT * FROM (VALUES(ROW('read uncommitted'))) RESPONSE(transaction_isolation)"},
                new Object[] {"SET search_path TO my_schema, public", "SET SESSION search_path = 'my_schema', 'public'"},
                new Object[] {"set search_path to my_schema, public", "SET SESSION search_path = 'my_schema', 'public'"},
                new Object[] {"set search_path to 'my_schema, public'", "SET SESSION search_path = 'my_schema, public'"},
                new Object[] {"set search_path to 'my,schema', public", "SET SESSION search_path = 'my,schema', 'public'"},
                new Object[] {"set search_path to 'my,schema', pu,blic", "SET SESSION search_path = 'my,schema', 'pu', 'blic'"},
                new Object[] {"set search_path to 'my, schema', pu,blic", "SET SESSION search_path = 'my, schema', 'pu', 'blic'"},
                new Object[] {"set search_path to ' myschema ', pu,blic", "SET SESSION search_path = ' myschema ', 'pu', 'blic'"},
                new Object[] {"SET datestyle TO postgres, dmy", "SET SESSION datestyle = 'postgres', 'dmy'"},
                new Object[] {"SET SESSION datestyle TO postgres, dmy", "SET SESSION datestyle = 'postgres', 'dmy'"},
                new Object[] {"SET LOCAL datestyle TO postgres, dmy", "SET LOCAL datestyle TO postgres, dmy"},
                new Object[] {"SET TIME ZONE 'PST8PDT'", "SET TIME ZONE 'PST8PDT'"},
                new Object[] {"SET TIME ZONE 'Europe/Rome'", "SET TIME ZONE 'Europe/Rome'"},
                new Object[] {"SHOW max_identifier_length", "SELECT 63 AS max_identifier_length"},
                new Object[] {"show max_identifier_length", "SELECT 63 AS max_identifier_length"},
                new Object[] {"DEALLOCATE testprepared", "DEALLOCATE PREPARE testprepared"},
                new Object[] {"deallocate testprepared", "DEALLOCATE PREPARE testprepared"},
                new Object[] {"DEALLOCATE all", "DEALLOCATE PREPARE all"},
                new Object[] {"SELECT ARRAY(SELECT 123)", "SELECT ARRAY[(SELECT 123)]"},
                new Object[] {"select array(select 123)", "select array[(select 123)]"},
                new Object[] {"SELECT CAST(ARRAY ( SELECT 123 ) as ARRAY(INTEGER))", "SELECT CAST(ARRAY [( SELECT 123 )] as ARRAY(INTEGER))"},
                new Object[] {"SELECT ARRAY(SELECT 123), ARRAY(SELECT 123)", "SELECT ARRAY[(SELECT 123)], ARRAY[(SELECT 123)]"},
                new Object[] {"SELECT ARRAY(SELECT ARRAY(SELECT 123))", "SELECT ARRAY[(SELECT ARRAY[(SELECT 123)])]"},
                new Object[] {"SELECT oid, stxrelid::pg_catalog.regclass, stxnamespace::pg_catalog.regnamespace AS nsp, stxname,\n" +
                        "  (SELECT pg_catalog.string_agg(pg_catalog.quote_ident(attname),', ')\n" +
                        "   FROM pg_catalog.unnest(stxkeys) s(attnum)\n" +
                        "   JOIN pg_catalog.pg_attribute a ON (stxrelid = a.attrelid AND\n" +
                        "        a.attnum = s.attnum AND NOT attisdropped)) AS columns,\n" +
                        "  'd' = any(stxkind) AS ndist_enabled,\n" +
                        "  'f' = any(stxkind) AS deps_enabled,\n" +
                        "  'm' = any(stxkind) AS mcv_enabled,\n" +
                        "  stxstattarget\n" +
                        "FROM pg_catalog.pg_statistic_ext stat\n" +
                        "WHERE stxrelid = '1046995830'\n" +
                        "ORDER BY 1",
                        // replace `stxkeys` as `array[]`
                        "SELECT oid, stxrelid::pg_catalog.regclass, stxnamespace::pg_catalog.regnamespace AS nsp, stxname,\n" +
                                "  (SELECT pg_catalog.string_agg(pg_catalog.quote_ident(attname),', ')\n" +
                                "   FROM pg_catalog.unnest(array[]) s(attnum)\n" +
                                "   JOIN pg_catalog.pg_attribute a ON (stxrelid = a.attrelid AND\n" +
                                "        a.attnum = s.attnum AND NOT attisdropped)) AS columns,\n" +
                                "  'd' = any(stxkind) AS ndist_enabled,\n" +
                                "  'f' = any(stxkind) AS deps_enabled,\n" +
                                "  'm' = any(stxkind) AS mcv_enabled,\n" +
                                "  stxstattarget\n" +
                                "FROM pg_catalog.pg_statistic_ext stat\n" +
                                "WHERE stxrelid = '1046995830'\n" +
                                "ORDER BY 1"
                },
                new Object[] {"SHOW DATESTYLE", "SELECT 'ISO' AS DateStyle"},
                new Object[] {"show datestyle", "SELECT 'ISO' AS DateStyle"},
                new Object[] {"show DateStyle", "SELECT 'ISO' AS DateStyle"},
                new Object[] {"select  array(select target from pg_extension_update_paths(extname) where source = extversion and path is not null)", "select  array[array[]]"},
                new Object[] {"SHOW standard_conforming_strings", "SELECT 'on' AS standard_conforming_strings"},
                new Object[] {"show standard_conforming_strings", "SELECT 'on' AS standard_conforming_strings"},
                new Object[] {"   show   STANDARD_CONFORMING_STRINGS", "SELECT 'on' AS standard_conforming_strings"},
                new Object[] {"SET datestyle = 'ISO'", "SET SESSION datestyle = 'ISO'"},
                new Object[] {"SET datestyle = ISO", "SET SESSION datestyle = 'ISO'"},
                new Object[] {"SET datestyle = iso", "SET SESSION datestyle = 'iso'"},
                new Object[] {"SET extra_float_digits = 3", "SET SESSION extra_float_digits = 3"},
                new Object[] {"SET refresh_cache = true", "SET SESSION refresh_cache = true"},
                new Object[] {" with table_privileges as (\n" +
                        "select\n" +
                        "  NULL as role,\n" +
                        "  t.schemaname as schema,\n" +
                        "  t.tablename as table,\n" +
                        "  pg_catalog.has_table_privilege(current_user, concat('\"', t.schemaname, '\"', '.', '\"', t.tablename, '\"'), 'SELECT') as select,\n" +
                        "  pg_catalog.has_table_privilege(current_user, concat('\"', t.schemaname, '\"', '.', '\"', t.tablename, '\"'), 'UPDATE') as update,\n" +
                        "  pg_catalog.has_table_privilege(current_user, concat('\"', t.schemaname, '\"', '.', '\"', t.tablename, '\"'), 'INSERT') as insert,\n" +
                        "  pg_catalog.has_table_privilege(current_user, concat('\"', t.schemaname, '\"', '.', '\"', t.tablename, '\"'), 'DELETE') as delete\n" +
                        "from pg_catalog.pg_tables t\n" +
                        "where t.schemaname !~ '^pg_'\n" +
                        "  and t.schemaname <> 'information_schema'\n" +
                        "  and pg_catalog.has_schema_privilege(current_user, t.schemaname, 'USAGE')\n" +
                        ")\n" +
                        "select t.*\n" +
                        "from table_privileges t\n" +
                        "where t.select or t.update or t.insert or t.delete",
                        "with table_privileges as (\n" +
                                "select\n" +
                                "  NULL as \"role\",\n" +
                                "  t.schemaname as \"schema\",\n" +
                                "  t.tablename as \"table\",\n" +
                                "  pg_catalog.has_table_privilege(concat('\"', t.schemaname, '\"', '.', '\"', t.tablename, '\"'), 'SELECT') as \"select\",\n" +
                                "  pg_catalog.has_table_privilege(concat('\"', t.schemaname, '\"', '.', '\"', t.tablename, '\"'), 'UPDATE') as \"update\",\n" +
                                "  pg_catalog.has_table_privilege(concat('\"', t.schemaname, '\"', '.', '\"', t.tablename, '\"'), 'INSERT') as \"insert\",\n" +
                                "  pg_catalog.has_table_privilege(concat('\"', t.schemaname, '\"', '.', '\"', t.tablename, '\"'), 'DELETE') as \"delete\"\n" +
                                "from pg_catalog.pg_tables t\n" +
                                "where t.schemaname !~ '^pg_'\n" +
                                "  and t.schemaname <> 'information_schema'\n" +
                                "  and pg_catalog.has_schema_privilege(t.schemaname, 'USAGE')\n" +
                                ")\n" +
                                "select t.*\n" +
                                "from table_privileges t\n" +
                                "where t.\"select\" or t.\"update\" or t.\"insert\" or t.\"delete\""},
                new Object[] {"show timezone", "SELECT 'UTC' AS TimeZone"},
                new Object[] {"show TIMEZONE", "SELECT 'UTC' AS TimeZone"},
                new Object[] {"SELECT n.nspname,p.proname,p.prorettype,p.proargtypes, t.typtype,t.typrelid,  p.proargnames, p.proargmodes, p.proallargtypes, p.oid  " +
                        "FROM pg_catalog.pg_proc p, pg_catalog.pg_namespace n, pg_catalog.pg_type t  WHERE p.pronamespace=n.oid AND p.prorettype=t.oid  ORDER BY n.nspname, " +
                        "p.proname, p.oid::text;",
                        "SELECT 1 limit 0"},
                new Object[] {"with table_privileges as (\n" +
                        " select\n" +
                        "   NULL as role,\n" +
                        "   t.schemaname as schema,\n" +
                        "   t.objectname as table,\n" +
                        "   pg_catalog.has_table_privilege(current_user, '\"' || t.schemaname || '\"' || '.' || '\"' || t.objectname || '\"',  'UPDATE') as update,\n" +
                        "   pg_catalog.has_table_privilege(current_user, '\"' || t.schemaname || '\"' || '.' || '\"' || t.objectname || '\"',  'SELECT') as select,\n" +
                        "   pg_catalog.has_table_privilege(current_user, '\"' || t.schemaname || '\"' || '.' || '\"' || t.objectname || '\"',  'INSERT') as insert,\n" +
                        "   pg_catalog.has_table_privilege(current_user, '\"' || t.schemaname || '\"' || '.' || '\"' || t.objectname || '\"',  'DELETE') as delete\n" +
                        " from (\n" +
                        "   select schemaname, tablename as objectname from pg_catalog.pg_tables\n" +
                        "   union\n" +
                        "   select schemaname, viewname as objectname from pg_catalog.pg_views\n" +
                        "   union\n" +
                        "   select schemaname, matviewname as objectname from pg_catalog.pg_matviews\n" +
                        " ) t\n" +
                        " where t.schemaname !~ '^pg_'\n" +
                        "   and t.schemaname <> 'information_schema'\n" +
                        "   and pg_catalog.has_schema_privilege(current_user, t.schemaname, 'USAGE')\n" +
                        ")\n" +
                        "select t.*\n" +
                        "from table_privileges t",
                        "with table_privileges as (\n" +
                                "select\n" +
                                "  NULL as \"role\",\n" +
                                "  t.schemaname as \"schema\",\n" +
                                "  t.tablename as \"table\",\n" +
                                "  pg_catalog.has_table_privilege(concat('\"', t.schemaname, '\"', '.', '\"', t.tablename, '\"'), 'SELECT') as \"select\",\n" +
                                "  pg_catalog.has_table_privilege(concat('\"', t.schemaname, '\"', '.', '\"', t.tablename, '\"'), 'UPDATE') as \"update\",\n" +
                                "  pg_catalog.has_table_privilege(concat('\"', t.schemaname, '\"', '.', '\"', t.tablename, '\"'), 'INSERT') as \"insert\",\n" +
                                "  pg_catalog.has_table_privilege(concat('\"', t.schemaname, '\"', '.', '\"', t.tablename, '\"'), 'DELETE') as \"delete\"\n" +
                                "from pg_catalog.pg_tables t\n" +
                                "where t.schemaname !~ '^pg_'\n" +
                                "  and t.schemaname <> 'information_schema'\n" +
                                "  and pg_catalog.has_schema_privilege(t.schemaname, 'USAGE')\n" +
                                ")\n" +
                                "select t.*\n" +
                                "from table_privileges t\n" +
                                "where t.\"select\" or t.\"update\" or t.\"insert\" or t.\"delete\""},
        };
    }

    @Test(dataProvider = "sql")
    public void testWrite(String sql, String expected)
    {
        String actual = PostgreSqlRewriteUtil.rewrite(sql);
        assertEquals(actual, expected);
    }
}
