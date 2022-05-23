/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cml.sql.calcite;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Map;

public class CmlSchema
        extends AbstractSchema
{
    @Override
    protected Map<String, Table> getTableMap()
    {
        CmlTable table = new CmlTable(new JavaTypeFactoryImpl().createSqlType(SqlTypeName.INTEGER));
        return ImmutableMap.of("orders", table);
    }
}
