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
package io.wren.base.sqlrewrite.analyzer;

import io.trino.sql.tree.Node;
import io.wren.base.ErrorCodeSupplier;
import io.wren.base.Location;
import io.wren.base.WrenException;

import java.util.Optional;

import static java.lang.String.format;

public final class SemanticExceptions
{
    private SemanticExceptions() {}

    public static WrenException semanticException(ErrorCodeSupplier code, Node node, String format, Object... args)
    {
        return semanticException(code, node, null, format, args);
    }

    public static WrenException semanticException(ErrorCodeSupplier code, Node node, Throwable cause, String format, Object... args)
    {
        throw new WrenException(code, extractLocation(node), format(format, args), cause);
    }

    public static Optional<Location> extractLocation(Node node)
    {
        return node.getLocation()
                .map(location -> new Location(location.getLineNumber(), location.getColumnNumber()));
    }
}
