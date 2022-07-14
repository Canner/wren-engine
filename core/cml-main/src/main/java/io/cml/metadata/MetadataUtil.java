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

import io.cml.spi.CmlException;
import io.cml.sql.QualifiedObjectName;
import io.trino.sql.tree.QualifiedName;

import java.util.List;
import java.util.Optional;

import static io.cml.spi.metadata.StandardErrorCode.MISSING_CATALOG_NAME;
import static io.cml.spi.metadata.StandardErrorCode.SYNTAX_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class MetadataUtil
{
    private MetadataUtil() {}

    public static QualifiedObjectName createQualifiedObjectName(QualifiedName name, String defaultCatalog, String defaultSchema)
    {
        requireNonNull(name, "name is null");
        List<String> parts = name.getParts();
        if (name.getParts().size() == 3) {
            return new QualifiedObjectName(parts.get(0), parts.get(1), parts.get(2));
        }
        else if (parts.size() == 2) {
            return new QualifiedObjectName(
                    Optional.ofNullable(defaultCatalog).orElseThrow(() ->
                            new CmlException(MISSING_CATALOG_NAME, "Default catalog must be specified")),
                    parts.get(0),
                    parts.get(1));
        }
        else if (parts.size() == 1) {
            return new QualifiedObjectName(
                    Optional.ofNullable(defaultCatalog).orElseThrow(() ->
                            new CmlException(MISSING_CATALOG_NAME, "Default catalog must be specified")),
                    Optional.ofNullable(defaultSchema).orElseThrow(() ->
                            new CmlException(MISSING_CATALOG_NAME, "Default schema must be specified")),
                    parts.get(0));
        }

        throw new CmlException(SYNTAX_ERROR, format("Too many dots in table name: %s", name));
    }
}
