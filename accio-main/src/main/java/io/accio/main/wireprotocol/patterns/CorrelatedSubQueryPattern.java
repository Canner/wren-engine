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

import static java.util.regex.Pattern.CASE_INSENSITIVE;

public class CorrelatedSubQueryPattern
        extends QueryPattern
{
    static final CorrelatedSubQueryPattern INSTANCE = new CorrelatedSubQueryPattern();

    private CorrelatedSubQueryPattern()
    {
        // This is for '\d [tablename]' command in psql client.
        // Trino don't support correlated sub-query, so we mock `stxkeys` column in pg_statistic_ext, and give an empty array as value.
        super(Pattern.compile("SELECT oid, stxrelid::pg_catalog.regclass, stxnamespace::pg_catalog.regnamespace AS nsp, stxname,\n" +
                        " *\\(SELECT pg_catalog.string_agg\\(pg_catalog.quote_ident\\(attname\\),', '\\)\n" +
                        " *FROM pg_catalog.unnest\\(stxkeys\\) s\\(attnum\\)\n" +
                        " *JOIN pg_catalog.pg_attribute a ON \\(stxrelid = a.attrelid AND\n" +
                        " *a.attnum = s.attnum AND NOT attisdropped\\)\\) AS columns,\n" +
                        " *'d' = any\\(stxkind\\) AS ndist_enabled,\n" +
                        " *'f' = any\\(stxkind\\) AS deps_enabled,\n" +
                        " *'m' = any\\(stxkind\\) AS mcv_enabled,\n" +
                        " *stxstattarget\n" +
                        " *FROM pg_catalog.pg_statistic_ext stat\n" +
                        " *WHERE stxrelid = '\\d*'\n" +
                        " *ORDER BY 1",
                CASE_INSENSITIVE));
    }

    @Override
    protected String rewrite(String statement)
    {
        return statement.replaceAll("stxkeys", "array[]");
    }
}
