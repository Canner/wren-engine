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

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Table;
import io.cml.metadata.ColumnHandle;
import io.cml.metadata.ColumnSchema;
import io.cml.metadata.ConnectorTableSchema;
import io.cml.metadata.Metadata;
import io.cml.metadata.ResolvedFunction;
import io.cml.metadata.TableHandle;
import io.cml.metadata.TableSchema;
import io.cml.spi.function.OperatorType;
import io.cml.spi.metadata.CatalogName;
import io.cml.spi.metadata.SchemaTableName;
import io.cml.spi.type.DateType;
import io.cml.spi.type.IntegerType;
import io.cml.spi.type.PGType;
import io.cml.sql.QualifiedObjectName;
import io.trino.sql.tree.QualifiedName;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.cml.calcite.CmlSchemaUtil.Dialect;
import static io.cml.connector.bigquery.BigQueryType.toPGType;
import static java.util.Objects.requireNonNull;

public class BigQueryMetadata
        implements Metadata
{
    private final BigQueryClient bigQueryClient;

    @Inject
    public BigQueryMetadata(BigQueryClient bigQueryClient)
    {
        this.bigQueryClient = requireNonNull(bigQueryClient, "bigQueryClient is null");
    }

    @Override
    public TableSchema getTableSchema(TableHandle tableHandle)
    {
        Table table = bigQueryClient.getTable(tableHandle);
        return new TableSchema(
                tableHandle.getCatalogName(),
                new ConnectorTableSchema(
                        tableHandle.getSchemaTableName(),
                        table.getDefinition().getSchema().getFields().stream()
                                .map(field ->
                                        ColumnSchema.builder()
                                                .setName(field.getName())
                                                .setType(toPGType(field.getType().name()))
                                                .build())
                                .collect(toImmutableList())));
    }

    @Override
    public Optional<TableHandle> getTableHandle(QualifiedObjectName tableName)
    {
        return Optional.of(
                new TableHandle(
                        new CatalogName(tableName.getCatalogName()),
                        new SchemaTableName(tableName.getSchemaName(), tableName.getObjectName())));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        return bigQueryClient.getTable(tableHandle)
                .getDefinition()
                .getSchema()
                .getFields()
                .stream()
                .collect(toImmutableMap(
                        Field::getName,
                        field -> new BigQueryColumnHandle(field.getName(), toPGType(field.getType().name()))));
    }

    @Override
    public ResolvedFunction resolveFunction(QualifiedName name, List<PGType> parameterTypes)
    {
        return new ResolvedFunction(name.toString(), IntegerType.INTEGER, parameterTypes);
    }

    @Override
    public ResolvedFunction resolveOperator(OperatorType operatorType, List<PGType> parameterTypes)
    {
        return new ResolvedFunction(operatorType.name(), IntegerType.INTEGER, parameterTypes);
    }

    @Override
    public PGType fromSqlType(String type)
    {
        if ("date".equals(type)) {
            return DateType.DATE;
        }
        throw new IllegalArgumentException();
    }

    @Override
    public boolean isAggregationFunction(QualifiedName name)
    {
        switch (name.toString()) {
            case "sum":
            case "avg":
            case "count":
                return true;
        }
        return false;
    }

    @Override
    public Dialect getDialect()
    {
        return Dialect.BIGQUERY;
    }
}
