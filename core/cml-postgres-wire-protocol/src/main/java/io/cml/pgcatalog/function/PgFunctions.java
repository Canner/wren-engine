/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cml.pgcatalog.function;

import com.google.common.collect.ImmutableList;

import static io.cml.pgcatalog.function.PgFunction.Argument.argument;
import static io.cml.pgcatalog.function.PgFunction.Language.SQL;
import static io.cml.pgcatalog.function.PgFunction.builder;
import static io.cml.type.BigIntType.BIGINT;
import static io.cml.type.IntegerType.INTEGER;
import static io.cml.type.VarcharType.VARCHAR;

public final class PgFunctions
{
    private static final String EMPTY_STATEMENT = "SELECT null LIMIT 0";

    private PgFunctions() {}

    public static final PgFunction CURRENT_DATABASE = builder()
            .setName("current_database")
            .setLanguage(SQL)
            .setDefinition("SELECT DISTINCT catalog_name FROM INFORMATION_SCHEMA.SCHEMATA")
            .build();

    public static final PgFunction PG_RELATION_SIZE__INT___BIGINT = builder()
            .setName("pg_relation_size")
            .setLanguage(SQL)
            .setDefinition(EMPTY_STATEMENT)
            .setArguments(ImmutableList.of(argument("relOid", INTEGER)))
            .setReturnType(BIGINT)
            .build();

    public static final PgFunction PG_RELATION_SIZE__INT_VARCHAR___BIGINT = builder()
            .setName("pg_relation_size")
            .setLanguage(SQL)
            .setDefinition(EMPTY_STATEMENT)
            .setArguments(ImmutableList.of(argument("relOid", INTEGER), argument("text", VARCHAR)))
            .setReturnType(BIGINT)
            .build();
}
