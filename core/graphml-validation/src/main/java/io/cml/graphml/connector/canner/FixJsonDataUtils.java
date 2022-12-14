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

package io.cml.graphml.connector.canner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.cml.graphml.base.Utils.checkArgument;
import static io.cml.graphml.connector.canner.TrinoType.BIGINT;
import static io.cml.graphml.connector.canner.TrinoType.BOOLEAN;
import static io.cml.graphml.connector.canner.TrinoType.CHAR;
import static io.cml.graphml.connector.canner.TrinoType.DATE;
import static io.cml.graphml.connector.canner.TrinoType.DECIMAL;
import static io.cml.graphml.connector.canner.TrinoType.DOUBLE;
import static io.cml.graphml.connector.canner.TrinoType.GEOMETRY;
import static io.cml.graphml.connector.canner.TrinoType.INTEGER;
import static io.cml.graphml.connector.canner.TrinoType.INTERVAL_DAY_TO_SECOND;
import static io.cml.graphml.connector.canner.TrinoType.INTERVAL_YEAR_TO_MONTH;
import static io.cml.graphml.connector.canner.TrinoType.IPADDRESS;
import static io.cml.graphml.connector.canner.TrinoType.REAL;
import static io.cml.graphml.connector.canner.TrinoType.SMALLINT;
import static io.cml.graphml.connector.canner.TrinoType.TIME;
import static io.cml.graphml.connector.canner.TrinoType.TIMESTAMP;
import static io.cml.graphml.connector.canner.TrinoType.TIMESTAMP_WITH_TIME_ZONE;
import static io.cml.graphml.connector.canner.TrinoType.TIME_WITH_TIME_ZONE;
import static io.cml.graphml.connector.canner.TrinoType.TINYINT;
import static io.cml.graphml.connector.canner.TrinoType.UUID;
import static io.cml.graphml.connector.canner.TrinoType.VARCHAR;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class FixJsonDataUtils
{
    private FixJsonDataUtils() {}

    public static Iterable<List<Object>> fixData(List<Map<String, Object>> columns, Iterable<List<Object>> data)
    {
        if (data == null) {
            return null;
        }
        requireNonNull(columns, "columns is null");
        List<Map<String, Object>> signatures = columns.stream()
                .map(column -> (Map<String, Object>) column.get("typeSignature"))
                .collect(toList());
        List<List<Object>> rows = new ArrayList<>();
        for (List<Object> row : data) {
            checkArgument(row.size() == columns.size(), "row/column size mismatch");
            List<Object> newRow = new ArrayList<>();
            for (int i = 0; i < row.size(); i++) {
                newRow.add(fixValue(signatures.get(i), row.get(i)));
            }
            rows.add(unmodifiableList(newRow)); // allow nulls in list
        }
        return List.copyOf(rows);
    }

    /**
     * Force values coming from Jackson to have the expected object type.
     */
    private static Object fixValue(Map<String, Object> signature, Object value)
    {
        if (value == null) {
            return null;
        }

        switch ((String) signature.get("rawType")) {
            case BIGINT:
                if (value instanceof String) {
                    return Long.parseLong((String) value);
                }
                return ((Number) value).longValue();
            case INTEGER:
                if (value instanceof String) {
                    return Integer.parseInt((String) value);
                }
                return ((Number) value).intValue();
            case SMALLINT:
                if (value instanceof String) {
                    return Short.parseShort((String) value);
                }
                return ((Number) value).shortValue();
            case TINYINT:
                if (value instanceof String) {
                    return Byte.parseByte((String) value);
                }
                return ((Number) value).byteValue();
            case DOUBLE:
                if (value instanceof String) {
                    return Double.parseDouble((String) value);
                }
                return ((Number) value).doubleValue();
            case REAL:
                if (value instanceof String) {
                    return Float.parseFloat((String) value);
                }
                return ((Number) value).floatValue();
            case BOOLEAN:
                if (value instanceof String) {
                    return Boolean.parseBoolean((String) value);
                }
                return Boolean.class.cast(value);
            case VARCHAR:
            case TrinoType.JSON:
            case TIME:
            case TIME_WITH_TIME_ZONE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
            case DATE:
            case INTERVAL_YEAR_TO_MONTH:
            case INTERVAL_DAY_TO_SECOND:
            case IPADDRESS:
            case UUID:
            case DECIMAL:
            case CHAR:
            case GEOMETRY:
            default:
                return value;
        }
    }
}
