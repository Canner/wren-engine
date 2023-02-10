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

package io.graphmdl.connector.jdbc;

import io.graphmdl.base.GraphMDLTypes;

import java.sql.JDBCType;

public final class JdbcTypeMapping
{
    private JdbcTypeMapping() {}

    public static String toGraphMDLType(JDBCType jdbcType)
    {
        switch (jdbcType) {
            case BIGINT:
                return GraphMDLTypes.BIGINT;
            case INTEGER:
                return GraphMDLTypes.INTEGER;
            case VARCHAR:
            default:
                return GraphMDLTypes.VARCHAR;
        }
    }
}
