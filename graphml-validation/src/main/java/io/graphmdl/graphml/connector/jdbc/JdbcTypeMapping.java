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

package io.graphmdl.graphml.connector.jdbc;

import io.graphmdl.graphml.base.GraphMLTypes;

import java.sql.JDBCType;

public final class JdbcTypeMapping
{
    private JdbcTypeMapping() {}

    public static String toGraphMLType(JDBCType jdbcType)
    {
        switch (jdbcType) {
            case BIGINT:
                return GraphMLTypes.BIGINT;
            case INTEGER:
                return GraphMLTypes.INTEGER;
            case VARCHAR:
            default:
                return GraphMLTypes.VARCHAR;
        }
    }
}
