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

package io.cml.pgcatalog.function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cml.spi.CmlException;

import javax.annotation.concurrent.ThreadSafe;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.cml.pgcatalog.function.PgFunctions.ARRAY_IN;
import static io.cml.pgcatalog.function.PgFunctions.ARRAY_OUT;
import static io.cml.pgcatalog.function.PgFunctions.ARRAY_RECV;
import static io.cml.pgcatalog.function.PgFunctions.ARRAY_UPPER;
import static io.cml.pgcatalog.function.PgFunctions.CURRENT_DATABASE;
import static io.cml.pgcatalog.function.PgFunctions.CURRENT_SCHEMAS;
import static io.cml.pgcatalog.function.PgFunctions.PG_RELATION_SIZE__INT_VARCHAR___BIGINT;
import static io.cml.pgcatalog.function.PgFunctions.PG_RELATION_SIZE__INT___BIGINT;
import static io.cml.spi.metadata.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;

@ThreadSafe
public final class PgFunctionRegistry
{
    private final List<PgFunction> pgFunctions;
    private final Map<String, PgFunction> simpleNameToFunction = new HashMap<>();

    private final Map<String, PgFunction> remoteNameToFunction;

    public PgFunctionRegistry()
    {
        pgFunctions = ImmutableList.<PgFunction>builder()
                .add(CURRENT_DATABASE)
                .add(CURRENT_SCHEMAS)
                .add(PG_RELATION_SIZE__INT___BIGINT)
                .add(PG_RELATION_SIZE__INT_VARCHAR___BIGINT)
                .add(ARRAY_IN)
                .add(ARRAY_OUT)
                .add(ARRAY_RECV)
                .add(ARRAY_UPPER)
                .build();

        ImmutableMap.Builder<String, PgFunction> remoteNameBuilder = ImmutableMap.builder();
        pgFunctions.forEach(pgFunction -> {
            // use HashMap to handle multiple same key entriesnn
            simpleNameToFunction.put(pgFunction.getName(), pgFunction);
            remoteNameBuilder.put(pgFunction.getRemoteName(), pgFunction);
        });
        remoteNameToFunction = remoteNameBuilder.build();
    }

    public List<PgFunction> getPgFunctions()
    {
        return pgFunctions;
    }

    public PgFunction getPgFunction(String name)
    {
        PgFunction candidate = simpleNameToFunction.get(name);
        if (candidate == null) {
            candidate = remoteNameToFunction.get(name);
        }
        return Optional.ofNullable(candidate)
                .orElseThrow(() -> new CmlException(NOT_FOUND, format("%s is undefined", name)));
    }
}
