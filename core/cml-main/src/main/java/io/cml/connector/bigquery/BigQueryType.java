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

import io.cml.spi.type.BooleanType;
import io.cml.spi.type.DoubleType;
import io.cml.spi.type.IntegerType;
import io.cml.spi.type.PGType;
import io.cml.spi.type.VarcharType;

public final class BigQueryType
{
    private BigQueryType() {}

    public static PGType<?> toPGType(String bigQueryType)
    {
        switch (bigQueryType) {
            case "BOOLEAN":
                return BooleanType.BOOLEAN;
            case "INTEGER":
                return IntegerType.INTEGER;
            case "STRING":
                return VarcharType.VARCHAR;
            case "FLOAT":
                return DoubleType.DOUBLE;
            default:
                return VarcharType.VARCHAR;
        }
    }
}
