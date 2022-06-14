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

import io.cml.spi.function.OperatorType;
import io.cml.spi.type.PGType;
import io.cml.sql.QualifiedObjectName;
import io.trino.sql.tree.QualifiedName;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.cml.calcite.CmlSchemaUtil.Dialect;

public interface Metadata
{
    /**
     * Return table schema definition for the specified table handle.
     * Table schema definition is a set of information
     * required by semantic analyzer to analyze the query.
     *
     * @throws RuntimeException if table handle is no longer valid
     * @see {@link #getTableMetadata(Session, TableHandle)}
     */
    TableSchema getTableSchema(TableHandle tableHandle);

    Optional<TableHandle> getTableHandle(QualifiedObjectName tableName);

    Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle);

    ResolvedFunction resolveFunction(QualifiedName name, List<PGType> parameterTypes);

    ResolvedFunction resolveOperator(OperatorType operatorType, List<PGType> parameterTypes);

    PGType fromSqlType(String type);

    boolean isAggregationFunction(QualifiedName name);

    default Dialect getDialect()
    {
        return Dialect.CALCITE;
    }
}
