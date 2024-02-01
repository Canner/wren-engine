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

package io.accio.main.pgcatalog.function;

import com.google.common.collect.ImmutableList;
import io.accio.base.type.RecordType;

import java.util.List;

import static io.accio.base.type.BigIntType.BIGINT;
import static io.accio.base.type.BooleanType.BOOLEAN;
import static io.accio.base.type.IntegerType.INTEGER;
import static io.accio.base.type.PGArray.INT4_ARRAY;
import static io.accio.base.type.PGArray.VARCHAR_ARRAY;
import static io.accio.base.type.VarcharType.VARCHAR;
import static io.accio.main.pgcatalog.function.PgFunction.Argument.argument;
import static io.accio.main.pgcatalog.function.PgFunction.Language.SQL;
import static io.accio.main.pgcatalog.function.PgFunction.builder;

public class DuckDBFunctions
{
    private DuckDBFunctions() {}

    private static final String NULL = "NULL";

    public static final PgFunction CURRENT_DATABASE = builder()
            .setName("current_database")
            .setLanguage(SQL)
            .setImplemented(true)
            .build();

    public static final PgFunction CURRENT_SCHEMAS = builder()
            .setName("current_schemas")
            .setLanguage(SQL)
            .setArguments(ImmutableList.of(argument("includeImplicit", BOOLEAN)))
            .setImplemented(true)
            .build();

    public static final PgFunction PG_RELATION_SIZE__INT_VARCHAR___BIGINT = builder()
            .setName("pg_relation_size")
            .setLanguage(SQL)
            .setDefinition(NULL)
            .setArguments(ImmutableList.of(argument("relOid", INTEGER), argument("text", VARCHAR)))
            .setReturnType(BIGINT)
            .build();

    // It's an overloading of PG_RELATION_SIZE__INT_VARCHAR___BIGINT, no need to implement it.
    public static final PgFunction PG_RELATION_SIZE__INT___BIGINT = builder()
            .setName("pg_relation_size")
            .setLanguage(SQL)
            .setArguments(ImmutableList.of(argument("relOid", INTEGER)))
            .setReturnType(BIGINT)
            .setImplemented(true)
            .build();

    public static final PgFunction ARRAY_IN = builder()
            .setName("array_in")
            .setLanguage(SQL)
            .setDefinition(NULL)
            .setArguments(ImmutableList.of(argument("ignored", VARCHAR)))
            .setReturnType(VARCHAR_ARRAY)
            .build();

    public static final PgFunction ARRAY_OUT = builder()
            .setName("array_out")
            .setLanguage(SQL)
            .setDefinition(NULL)
            .setArguments(ImmutableList.of(argument("ignored", VARCHAR_ARRAY)))
            .setReturnType(VARCHAR)
            .build();

    public static final PgFunction ARRAY_RECV = builder()
            .setName("array_recv")
            .setLanguage(SQL)
            .setDefinition(NULL)
            .setArguments(ImmutableList.of(argument("ignored", VARCHAR)))
            .setReturnType(VARCHAR_ARRAY)
            .build();

    public static final PgFunction ARRAY_UPPER = builder()
            .setName("array_upper")
            .setLanguage(SQL)
            .setDefinition("CASE WHEN dim = 1 THEN array_length(input) ELSE NULL END")
            .setArguments(ImmutableList.of(argument("input", VARCHAR_ARRAY), argument("dim", BIGINT)))
            .setReturnType(INTEGER)
            .build();

    public static final PgFunction PG_GET_EXPR = builder()
            .setName("pg_get_expr")
            .setLanguage(SQL)
            .setDefinition("''")
            .setArguments(List.of(argument("pg_node", VARCHAR), argument("relation", INTEGER)))
            .setReturnType(VARCHAR)
            .setImplemented(true)
            .build();

    public static final PgFunction PG_GET_EXPR_PRETTY = builder()
            .setName("pg_get_expr")
            .setLanguage(SQL)
            .setDefinition("SELECT ''")
            .setArguments(List.of(argument("pg_node", VARCHAR), argument("relation", INTEGER), argument("pretty", BOOLEAN)))
            .setReturnType(VARCHAR)
            .setImplemented(true)
            .build();

    public static final PgFunction FORMAT_TYPE = builder()
            .setName("format_type")
            .setLanguage(SQL)
            .setDefinition("(select format_pg_type(t.type_name) from duckdb_types() t where t.type_name=lower(tname)) || case when typemod>0 then concat('(', typemod//1000, ',', typemod%1000, ')') else '' end")
            .setArguments(List.of(argument("tname", VARCHAR), argument("typemod", INTEGER)))
            .setReturnType(VARCHAR)
            .build();

    public static final PgFunction PG_GET_FUNCTION_RESULT = builder()
            .setName("pg_get_function_result")
            .setLanguage(SQL)
            .setDefinition("SELECT prorettype FROM pg_proc WHERE oid = func")
            .setArguments(List.of(argument("func", INTEGER)))
            .setReturnType(VARCHAR)
            .build();

    // TODO This is a mock function, need to be implemented
    public static final PgFunction PG_EXPANDARRAY = builder()
            .setName("_pg_expandarray")
            .setLanguage(SQL)
            .setDefinition("CASE WHEN (array_length(int_arr) > 0) THEN cast((int_arr[0], 1) as row(x int, n int)) ELSE NULL END")
            .setArguments(List.of(argument("int_arr", INT4_ARRAY)))
            .setReturnType(new RecordType(List.of(BIGINT, BIGINT)))
            .build();

    public static final PgFunction REGEXP_LIKE = builder()
            .setName("regexp_like")
            .setLanguage(SQL)
            .setDefinition("SELECT regexp_matches(arg1, arg2)")
            .setArguments(List.of(argument("arg1", VARCHAR), argument("arg2", VARCHAR)))
            .setReturnType(BOOLEAN)
            .build();

    public static final PgFunction GENERATE_ARRAY = builder()
            .setName("generate_array")
            .setLanguage(SQL)
            .setDefinition("generate_series(start, stop)")
            .setArguments(List.of(argument("start", BIGINT), argument("stop", BIGINT)))
            .setReturnType(INT4_ARRAY)
            .build();
}
