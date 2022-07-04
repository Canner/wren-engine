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
package io.cml.sql;

import com.fasterxml.jackson.annotation.JsonValue;
import io.cml.spi.metadata.SchemaTableName;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;
import java.util.function.Function;

@Immutable
public class QualifiedObjectName
{
    private final String catalogName;
    private final String schemaName;
    private final String objectName;

    public QualifiedObjectName(String catalogName, String schemaName, String objectName)
    {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.objectName = objectName;
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getObjectName()
    {
        return objectName;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        QualifiedObjectName o = (QualifiedObjectName) obj;
        return Objects.equals(catalogName, o.catalogName) &&
                Objects.equals(schemaName, o.schemaName) &&
                Objects.equals(objectName, o.objectName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, schemaName, objectName);
    }

    @JsonValue
    @Override
    public String toString()
    {
        return catalogName + '.' + schemaName + '.' + objectName;
    }

    public static Function<SchemaTableName, QualifiedObjectName> convertFromSchemaTableName(String catalogName)
    {
        return input -> new QualifiedObjectName(catalogName, input.getSchemaName(), input.getTableName());
    }
}
