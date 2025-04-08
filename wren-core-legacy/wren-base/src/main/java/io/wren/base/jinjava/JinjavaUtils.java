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

package io.wren.base.jinjava;

import io.wren.base.dto.Macro;
import io.wren.base.macro.Parameter;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class JinjavaUtils
{
    private JinjavaUtils() {}

    public static String getMacroTag(Macro macro)
    {
        StringBuilder builder = new StringBuilder();
        String paramString = macro.getParameters().stream().map(Parameter::getName).collect(joining(","));
        builder.append(format("{%% macro %s(%s) -%%}", macro.getName(), paramString));
        builder.append(macro.getBody());
        builder.append("{%- endmacro -%}");
        return builder.toString();
    }
}
