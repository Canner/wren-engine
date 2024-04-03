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

package io.wren.base.metadata;

import com.google.common.collect.ImmutableList;
import io.wren.base.type.DateType;
import io.wren.base.type.VarcharType;

import static io.wren.base.metadata.Function.Argument.argument;

public class BasicFunctions
{
    private BasicFunctions() {}

    public static final Function DATE_TRUNC = Function.builder()
            .setName("date_trunc")
            .setArguments(ImmutableList.of(argument("field", VarcharType.VARCHAR), argument("source", DateType.DATE)))
            .setReturnType(DateType.DATE)
            .build();
}
