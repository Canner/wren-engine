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

package io.wren.base.pgcatalog.function;

import com.google.common.collect.ImmutableList;
import io.wren.base.type.BigIntType;
import io.wren.base.type.IntegerType;
import io.wren.base.type.PGArray;
import io.wren.base.type.RecordType;
import io.wren.base.type.VarcharType;

import java.util.List;

import static io.wren.base.metadata.Function.Argument.argument;
import static io.wren.base.type.BooleanType.BOOLEAN;

public class DuckDBFunctions
{
    private DuckDBFunctions() {}

    private static final String NULL = "NULL";

    public static final PgFunction CURRENT_DATABASE = PgFunction.builder()
            .setName("current_database")
            .setLanguage(PgFunction.Language.SQL)
            .setImplemented(true)
            .build();

    public static final PgFunction CURRENT_SCHEMAS = PgFunction.builder()
            .setName("current_schemas")
            .setLanguage(PgFunction.Language.SQL)
            .setArguments(ImmutableList.of(argument("includeImplicit", BOOLEAN)))
            .setImplemented(true)
            .build();

    public static final PgFunction PG_RELATION_SIZE__INT_VARCHAR___BIGINT = PgFunction.builder()
            .setName("pg_relation_size")
            .setLanguage(PgFunction.Language.SQL)
            .setDefinition(NULL)
            .setArguments(ImmutableList.of(argument("relOid", IntegerType.INTEGER), argument("text", VarcharType.VARCHAR)))
            .setReturnType(BigIntType.BIGINT)
            .build();

    // It's an overloading of PG_RELATION_SIZE__INT_VARCHAR___BIGINT, no need to implement it.
    public static final PgFunction PG_RELATION_SIZE__INT___BIGINT = PgFunction.builder()
            .setName("pg_relation_size")
            .setLanguage(PgFunction.Language.SQL)
            .setArguments(ImmutableList.of(argument("relOid", IntegerType.INTEGER)))
            .setReturnType(BigIntType.BIGINT)
            .setImplemented(true)
            .build();

    public static final PgFunction ARRAY_IN = PgFunction.builder()
            .setName("array_in")
            .setLanguage(PgFunction.Language.SQL)
            .setDefinition(NULL)
            .setArguments(ImmutableList.of(argument("ignored", VarcharType.VARCHAR)))
            .setReturnType(PGArray.VARCHAR_ARRAY)
            .build();

    public static final PgFunction ARRAY_OUT = PgFunction.builder()
            .setName("array_out")
            .setLanguage(PgFunction.Language.SQL)
            .setDefinition(NULL)
            .setArguments(ImmutableList.of(argument("ignored", PGArray.VARCHAR_ARRAY)))
            .setReturnType(VarcharType.VARCHAR)
            .build();

    public static final PgFunction ARRAY_RECV = PgFunction.builder()
            .setName("array_recv")
            .setLanguage(PgFunction.Language.SQL)
            .setDefinition(NULL)
            .setArguments(ImmutableList.of(argument("ignored", VarcharType.VARCHAR)))
            .setReturnType(PGArray.VARCHAR_ARRAY)
            .build();

    public static final PgFunction ARRAY_UPPER = PgFunction.builder()
            .setName("array_upper")
            .setLanguage(PgFunction.Language.SQL)
            .setDefinition("CASE WHEN dim = 1 THEN array_length(input) ELSE NULL END")
            .setArguments(ImmutableList.of(argument("input", PGArray.VARCHAR_ARRAY), argument("dim", BigIntType.BIGINT)))
            .setReturnType(IntegerType.INTEGER)
            .build();

    public static final PgFunction PG_GET_EXPR = PgFunction.builder()
            .setName("pg_get_expr")
            .setLanguage(PgFunction.Language.SQL)
            .setDefinition("''")
            .setArguments(List.of(argument("pg_node", VarcharType.VARCHAR), argument("relation", IntegerType.INTEGER)))
            .setReturnType(VarcharType.VARCHAR)
            .setImplemented(true)
            .build();

    public static final PgFunction PG_GET_EXPR_PRETTY = PgFunction.builder()
            .setName("pg_get_expr")
            .setLanguage(PgFunction.Language.SQL)
            .setDefinition("SELECT ''")
            .setArguments(List.of(argument("pg_node", VarcharType.VARCHAR), argument("relation", IntegerType.INTEGER), argument("pretty", BOOLEAN)))
            .setReturnType(VarcharType.VARCHAR)
            .setImplemented(true)
            .build();

    public static final PgFunction PG_GET_FUNCTION_RESULT = PgFunction.builder()
            .setName("pg_get_function_result")
            .setLanguage(PgFunction.Language.SQL)
            .setDefinition("SELECT prorettype FROM pg_proc WHERE oid = func")
            .setArguments(List.of(argument("func", IntegerType.INTEGER)))
            .setReturnType(VarcharType.VARCHAR)
            .build();

    // TODO This is a mock function, need to be implemented
    public static final PgFunction PG_EXPANDARRAY = PgFunction.builder()
            .setName("_pg_expandarray")
            .setLanguage(PgFunction.Language.SQL)
            .setDefinition("CASE WHEN (array_length(int_arr) > 0) THEN cast((int_arr[0], 1) as row(x int, n int)) ELSE NULL END")
            .setArguments(List.of(argument("int_arr", PGArray.INT4_ARRAY)))
            .setReturnType(new RecordType(List.of(BigIntType.BIGINT, BigIntType.BIGINT)))
            .build();

    public static final PgFunction REGEXP_LIKE = PgFunction.builder()
            .setName("regexp_like")
            .setLanguage(PgFunction.Language.SQL)
            .setDefinition("SELECT regexp_matches(arg1, arg2)")
            .setArguments(List.of(argument("arg1", VarcharType.VARCHAR), argument("arg2", VarcharType.VARCHAR)))
            .setReturnType(BOOLEAN)
            .build();

    public static final PgFunction GENERATE_ARRAY = PgFunction.builder()
            .setName("generate_array")
            .setLanguage(PgFunction.Language.SQL)
            .setDefinition("generate_series(start, stop)")
            .setArguments(List.of(argument("start", BigIntType.BIGINT), argument("stop", BigIntType.BIGINT)))
            .setReturnType(PGArray.INT4_ARRAY)
            .build();
}
