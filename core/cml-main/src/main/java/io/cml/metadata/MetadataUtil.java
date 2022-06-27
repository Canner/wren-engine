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
import io.trino.sql.tree.QualifiedName;

import java.util.List;
import java.util.Optional;

import static io.cml.spi.metadata.StandardErrorCode.MISSING_CATALOG_NAME;
import static io.cml.spi.metadata.StandardErrorCode.MISSING_SCHEMA_NAME;
import static io.cml.spi.metadata.StandardErrorCode.SYNTAX_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class MetadataUtil
{
    private MetadataUtil() {}

    public static QualifiedObjectName createQualifiedObjectName(QualifiedName name)
    {
        requireNonNull(name, "name is null");
        if (name.getParts().size() > 3) {
            throw new CmlException(SYNTAX_ERROR, format("Too many dots in table name: %s", name));
        }

        List<String> parts = Lists.reverse(name.getParts());
        String objectName = parts.get(0);
        // TODO: handle bigquery with optional project id, optional schema name
        String schemaName = Optional.ofNullable(parts.get(1)).orElseThrow(() ->
                new CmlException(MISSING_SCHEMA_NAME, "Schema must be specified when session schema is not set"));
        String catalogName = Optional.ofNullable(parts.get(2)).orElseThrow(() ->
                new CmlException(MISSING_CATALOG_NAME, "Catalog must be specified when session catalog is not set"));

        return new QualifiedObjectName(catalogName, schemaName, objectName);
    }
}
