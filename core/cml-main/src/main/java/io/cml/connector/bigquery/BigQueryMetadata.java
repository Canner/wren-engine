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

package io.cml.connector.bigquery;

import io.cml.metadata.ColumnHandle;
import io.cml.metadata.Metadata;
import io.cml.metadata.ResolvedFunction;
import io.cml.metadata.TableHandle;
import io.cml.metadata.TableSchema;
import io.cml.spi.connector.Connector;
import io.cml.spi.function.OperatorType;
import io.cml.spi.type.PGType;
import io.cml.sql.QualifiedObjectName;
import io.trino.sql.tree.QualifiedName;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class BigQueryMetadata
        implements Metadata
{
    private final Connector connector;

    @Inject
    public BigQueryMetadata(Connector connector)
    {
        this.connector = connector;
    }

    @Override
    public TableSchema getTableSchema(TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TableHandle> getTableHandle(QualifiedObjectName tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResolvedFunction resolveFunction(QualifiedName name, List<PGType> parameterTypes)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResolvedFunction resolveOperator(OperatorType operatorType, List<PGType> parameterTypes)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PGType fromSqlType(String type)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isAggregationFunction(QualifiedName name)
    {
        throw new UnsupportedOperationException();
    }
}
