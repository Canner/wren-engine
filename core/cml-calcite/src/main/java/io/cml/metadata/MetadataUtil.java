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

package io.cml.metadata;

import com.google.common.collect.Lists;
import io.cml.spi.CmlException;
import io.cml.sql.QualifiedObjectName;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;

import java.util.List;

import static io.cml.spi.metadata.StandardErrorCode.SYNTAX_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class MetadataUtil
{
    private MetadataUtil() {}

    public static QualifiedObjectName createQualifiedObjectName(Node node, QualifiedName name)
    {
        requireNonNull(name, "name is null");
        if (name.getParts().size() > 3) {
            throw new CmlException(SYNTAX_ERROR, format("Too many dots in table name: %s", name));
        }

        List<String> parts = Lists.reverse(name.getParts());
        String objectName = parts.get(0);
        String schemaName = (parts.size() > 1) ? parts.get(1) : "";
        String catalogName = (parts.size() > 2) ? parts.get(2) : "";

        return new QualifiedObjectName(catalogName, schemaName, objectName);
    }
}
