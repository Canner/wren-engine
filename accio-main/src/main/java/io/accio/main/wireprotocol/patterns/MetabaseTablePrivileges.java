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

package io.accio.main.wireprotocol.patterns;

import java.util.regex.Pattern;

/**
 * DuckDB doesn't support ahs_table_privilege() function with user arguments now.
 * Metabase submit a query with invalid syntax. It make the sql rewrite won't work.
 * Thus, rewrite this query through hard code for now.
 */
public class MetabaseTablePrivileges
        extends QueryPattern
{
    static final QueryPattern INSTANCE = new MetabaseTablePrivileges();

    public MetabaseTablePrivileges()
    {
        super(Pattern.compile("with table_privileges as \\(\n" +
                        "select\n" +
                        " *NULL as role,\n" +
                        " *t\\.schemaname as schema,\n" +
                        " *t\\.tablename as table,\n" +
                        " *pg_catalog\\.has_table_privilege\\(current_user, concat\\('\"', t\\.schemaname, '\"', '\\.', '\"', t\\.tablename, '\"'\\), 'SELECT'\\) as select,\n" +
                        " *pg_catalog\\.has_table_privilege\\(current_user, concat\\('\"', t\\.schemaname, '\"', '\\.', '\"', t\\.tablename, '\"'\\), 'UPDATE'\\) as update,\n" +
                        " *pg_catalog\\.has_table_privilege\\(current_user, concat\\('\"', t\\.schemaname, '\"', '\\.', '\"', t\\.tablename, '\"'\\), 'INSERT'\\) as insert,\n" +
                        " *pg_catalog\\.has_table_privilege\\(current_user, concat\\('\"', t\\.schemaname, '\"', '\\.', '\"', t\\.tablename, '\"'\\), 'DELETE'\\) as delete\n" +
                        "from pg_catalog\\.pg_tables t\n" +
                        "where t\\.schemaname !~ '\\^pg_'\n" +
                        " *and t\\.schemaname <> 'information_schema'\n" +
                        " *and pg_catalog\\.has_schema_privilege\\(current_user, t\\.schemaname, 'USAGE'\\)\n" +
                        "\\)\n" +
                        "select t\\.\\*\n" +
                        "from table_privileges t\n" +
                        "where t\\.select or t\\.update or t\\.insert or t\\.delete",
                Pattern.CASE_INSENSITIVE));
    }

    @Override
    protected String rewrite(String statement)
    {
        return "with table_privileges as (\n" +
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
                "where t.\"select\" or t.\"update\" or t.\"insert\" or t.\"delete\"";
    }
}
