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

package io.cml.tpch;

import com.google.common.collect.ImmutableMap;
import io.cml.calcite.CmlTable;
import io.cml.metadata.CatalogName;
import io.cml.metadata.ColumnHandle;
import io.cml.metadata.ColumnSchema;
import io.cml.metadata.ConnectorTableSchema;
import io.cml.metadata.Metadata;
import io.cml.metadata.ResolvedFunction;
import io.cml.metadata.TableHandle;
import io.cml.metadata.TableSchema;
import io.cml.spi.function.OperatorType;
import io.cml.spi.metadata.SchemaTableName;
import io.cml.spi.type.DateType;
import io.cml.spi.type.DoubleType;
import io.cml.spi.type.IntegerType;
import io.cml.spi.type.PGType;
import io.cml.spi.type.VarcharType;
import io.cml.sql.QualifiedObjectName;
import io.trino.sql.tree.QualifiedName;

import java.sql.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class TpchMetadata
        implements Metadata
{
    private final Map<String, CmlTable> tpchTableMap;

    public TpchMetadata(Map<String, CmlTable> tpchTableMap)
    {
        this.tpchTableMap = tpchTableMap;
    }

    @Override
    public TableSchema getTableSchema(TableHandle tableHandle)
    {
        TpchTable table = TpchTable.valueOf(tableHandle.toString().toUpperCase(Locale.ROOT));
        return new TableSchema(new CatalogName("tpch"),
                new ConnectorTableSchema(new SchemaTableName("tiny", table.name().toLowerCase(Locale.ROOT)),
                        table.columns.stream().map(this::toColumnSchema).collect(toImmutableList())));
    }

    @Override
    public Optional<TableHandle> getTableHandle(QualifiedObjectName tableName)
    {
        return Optional.ofNullable(tpchTableMap.get(tableName.getObjectName()))
                .map(TableHandle::new);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        for (TpchTable.Column column : TpchTable.valueOf(tableHandle.toString().toUpperCase(Locale.ROOT)).columns) {
            builder.put(column.name, new TpchColumnHandle(column.name, toPgType(column.type)));
        }
        return builder.build();
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

    private ColumnSchema toColumnSchema(TpchTable.Column column)
    {
        return ColumnSchema.builder().setName(column.name).setType(toPgType(column.type)).build();
    }

    private PGType toPgType(Class<?> type)
    {
        if (Integer.class.equals(type)) {
            return IntegerType.INTEGER;
        }
        else if (Double.class.equals(type)) {
            return DoubleType.DOUBLE;
        }
        else if (String.class.equals(type)) {
            return VarcharType.VARCHAR;
        }
        else if (Date.class.equals(type)) {
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
}
