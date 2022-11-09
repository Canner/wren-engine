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

package io.cml.graphml;

import io.cml.graphml.dto.Column;
import io.cml.graphml.dto.Model;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class Utils
{
    private Utils() {}

    public static String getModelSql(Model model)
    {
        requireNonNull(model, "model is null");
        if (model.getColumns().isEmpty()) {
            return model.getRefSql();
        }
        return format("SELECT %s FROM (%s)", model.getColumns().stream().map(Column::getName).collect(joining(", ")), model.getRefSql());
    }
}
