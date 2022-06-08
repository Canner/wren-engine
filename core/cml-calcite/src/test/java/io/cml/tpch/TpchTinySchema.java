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
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Locale;
import java.util.Map;

public class TpchTinySchema
        extends AbstractSchema
{
    @Override
    protected Map<String, Table> getTableMap()
    {
        ImmutableMap.Builder<String, Table> tableMapBuilder = ImmutableMap.builder();
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

        for (TpchTable table : TpchTable.values()) {
            RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
            for (TpchTable.Column column : table.columns) {
                RelDataType type = typeFactory.createJavaType(column.type);
                builder.add(column.name, type.getSqlTypeName()).nullable(true);
            }
            tableMapBuilder.put(table.name().toLowerCase(Locale.ROOT), new CmlTable(table.name().toLowerCase(Locale.ROOT), builder.build()));
        }
        return tableMapBuilder.build();
    }
}
