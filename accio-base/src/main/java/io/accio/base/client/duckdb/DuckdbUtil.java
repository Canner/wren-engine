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

package io.accio.base.client.duckdb;

import io.accio.base.AccioException;
import io.airlift.units.DataSize;

import java.util.HashMap;
import java.util.Map;

import static io.accio.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Locale.ENGLISH;

public final class DuckdbUtil
{
    private static final Map<String, DataSize.Unit> UNIT_MAP = new HashMap<>();

    // https://github.com/duckdb/duckdb/blob/4c7cb20474baa3a8ca1d5d8ceb22beae0e4c0e4c/src/common/string_util.cpp#L178
    static {
        UNIT_MAP.put("BYTE", DataSize.Unit.BYTE);
        UNIT_MAP.put("BYTES", DataSize.Unit.BYTE);
        UNIT_MAP.put("KB", DataSize.Unit.KILOBYTE);
        UNIT_MAP.put("KIB", DataSize.Unit.KILOBYTE);
        UNIT_MAP.put("MB", DataSize.Unit.MEGABYTE);
        UNIT_MAP.put("MIB", DataSize.Unit.MEGABYTE);
        UNIT_MAP.put("GB", DataSize.Unit.GIGABYTE);
        UNIT_MAP.put("GIB", DataSize.Unit.GIGABYTE);
        UNIT_MAP.put("TB", DataSize.Unit.TERABYTE);
        UNIT_MAP.put("TIB", DataSize.Unit.TERABYTE);
        UNIT_MAP.put("PB", DataSize.Unit.PETABYTE);
        UNIT_MAP.put("PIB", DataSize.Unit.PETABYTE);
    }

    private DuckdbUtil() {}

    public static DataSize convertDuckDBUnits(String valueWithUnit)
    {
        try {
            String valueWithUnitUpperCase = valueWithUnit.toUpperCase(ENGLISH);
            for (Map.Entry<String, DataSize.Unit> entry : UNIT_MAP.entrySet()) {
                String unit = entry.getKey();
                if (valueWithUnitUpperCase.endsWith(unit)) {
                    double value = Double.parseDouble(valueWithUnitUpperCase.substring(0, valueWithUnitUpperCase.length() - unit.length()).trim());
                    return DataSize.of((long) value, entry.getValue());
                }
            }
        }
        catch (Exception e) {
            throw new AccioException(GENERIC_INTERNAL_ERROR, String.format("Failed to parse duckdb value %s", valueWithUnit), e);
        }
        throw new AccioException(GENERIC_INTERNAL_ERROR, String.format("Failed to parse duckdb value %s", valueWithUnit));
    }
}
