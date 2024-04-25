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

package io.wren.testing.duckdb;

import com.google.common.collect.ImmutableMap;
import io.wren.testing.AbstractWireProtocolTypeTest;
import io.wren.testing.TestingWrenServer;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static io.wren.base.config.WrenConfig.DataSourceType.DUCKDB;
import static io.wren.base.type.PGArray.BOOL_ARRAY;
import static io.wren.base.type.PGArray.CHAR_ARRAY;
import static io.wren.base.type.PGArray.DATE_ARRAY;
import static io.wren.base.type.PGArray.FLOAT4_ARRAY;
import static io.wren.base.type.PGArray.FLOAT8_ARRAY;
import static io.wren.base.type.PGArray.INT2_ARRAY;
import static io.wren.base.type.PGArray.INT4_ARRAY;
import static io.wren.base.type.PGArray.INT8_ARRAY;
import static io.wren.base.type.PGArray.JSON_ARRAY;
import static io.wren.base.type.PGArray.NUMERIC_ARRAY;
import static io.wren.base.type.PGArray.TIMESTAMP_ARRAY;
import static io.wren.base.type.PGArray.VARCHAR_ARRAY;
import static io.wren.testing.DataType.bigintDataType;
import static io.wren.testing.DataType.booleanDataType;
import static io.wren.testing.DataType.byteaDataType;
import static io.wren.testing.DataType.charDataType;
import static io.wren.testing.DataType.dateDataType;
import static io.wren.testing.DataType.decimalDataType;
import static io.wren.testing.DataType.doubleDataType;
import static io.wren.testing.DataType.integerDataType;
import static io.wren.testing.DataType.jsonDataType;
import static io.wren.testing.DataType.realDataType;
import static io.wren.testing.DataType.smallintDataType;
import static io.wren.testing.DataType.textDataType;
import static io.wren.testing.DataType.timestampDataType;
import static io.wren.testing.DataType.varcharDataType;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

public class TestWireProtocolTypeWithDuckDB
        extends AbstractWireProtocolTypeTest
{
    @Override
    protected TestingWrenServer createWrenServer()
            throws Exception
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("wren.datasource.type", DUCKDB.name())
                .put("wren.directory", Files.createTempDirectory(getWrenDirectory()).toString());

        return TestingWrenServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
    }

    @Override
    protected String getDefaultCatalog()
    {
        return "memory";
    }

    @Override
    protected String getDefaultSchema()
    {
        return "tpch";
    }

    @Override
    @Test(description = "DuckDB limit width must be between 1 and 38")
    public void testDecimal()
    {
        createTypeTest()
                .addInput(decimalDataType(), new BigDecimal("0.123"))
                .addInput(decimalDataType(3, 0), new BigDecimal("0"))
                .addInput(decimalDataType(3, 0), new BigDecimal("193"))
                .addInput(decimalDataType(3, 0), new BigDecimal("19"))
                .addInput(decimalDataType(3, 0), new BigDecimal("-193"))
                .addInput(decimalDataType(3, 1), new BigDecimal("10.0"))
                .addInput(decimalDataType(3, 1), new BigDecimal("10.1"))
                .addInput(decimalDataType(3, 1), new BigDecimal("-10.1"))
                .addInput(decimalDataType(4, 2), new BigDecimal("2.00"))
                .addInput(decimalDataType(4, 2), new BigDecimal("2.30"))
                .addInput(decimalDataType(24, 2), new BigDecimal("2.00"))
                .addInput(decimalDataType(24, 2), new BigDecimal("2.30"))
                .addInput(decimalDataType(24, 2), new BigDecimal("123456789.30"))
                .addInput(decimalDataType(24, 4), new BigDecimal("12345678901234567890.3100"))
                .addInput(decimalDataType(30, 5), new BigDecimal("3141592653589793238462643.38327"))
                .addInput(decimalDataType(30, 5), new BigDecimal("-3141592653589793238462643.38327"))
                .addInput(decimalDataType(30, 0), new BigDecimal("9223372036854775807"))
                .addInput(decimalDataType(38, 0), new BigDecimal("27182818284590452353602874713526624977"))
                .addInput(decimalDataType(38, 9), new BigDecimal("27182818284590452353602874713.526624977"))
                .addInput(decimalDataType(38, 10), new BigDecimal("2718281828459045235360287471.3526624977"))
                .executeSuite();
    }

    // TODO: duckdb 0.10.2 exists issue about bytea type
    @Override
    @Test(enabled = false)
    public void testBytea()
    {
        createTypeTest()
                .addInput(byteaDataType(), "hello", value -> value.getBytes(UTF_8))
                .addInput(byteaDataType(), "\\x68\\x65\\x6c\\x6c\\x6f", ignored -> "hello".getBytes(UTF_8))
                .executeSuite();
    }

    @Test
    public void testArray()
    {
        createTypeTest()
                .addInput(arrayDataType(booleanDataType(), BOOL_ARRAY), asList(true, false))
                .addInput(arrayDataType(smallintDataType(), INT2_ARRAY), asList((short) 1, (short) 2))
                .addInput(arrayDataType(integerDataType(), INT4_ARRAY), asList(1, 2, 1_234_567_890))
                .addInput(arrayDataType(bigintDataType(), INT8_ARRAY), asList(123_456_789_012L, 1_234_567_890L))
                .addInput(arrayDataType(realDataType(), FLOAT4_ARRAY), asList(123.45f, 1.2345f))
                .addInput(arrayDataType(doubleDataType(), FLOAT8_ARRAY), asList(123.45d, 1.2345d))
                .addInput(arrayDataType(decimalDataType(3, 1), NUMERIC_ARRAY), asList(new BigDecimal("1.000"), new BigDecimal("1.100"))) // DuckDB defaults precision = 18 and scale = 3
                .addInput(arrayDataType(varcharDataType(), VARCHAR_ARRAY), asList("hello", "world"))
                .addInput(arrayDataType(charDataType(), CHAR_ARRAY), asList("h", "w"))
                // We don't support bytea[] in DuckDB
                // .addInput(arrayDataType(byteaDataType(), BYTEA_ARRAY), asList("hello", "world"), value -> asList("\\x68\\x65\\x6c\\x6c\\x6f", "\\x77\\x6f\\x72\\x6c\\x64"))
                .addInput(arrayDataType(jsonDataType(), JSON_ARRAY), asList("{\"a\": \"apple\"}", "{\"b\": \"banana\"}"))
                .addInput(arrayDataType(timestampDataType(3), TIMESTAMP_ARRAY),
                        asList(LocalDateTime.of(2019, 1, 1, 1, 1, 1, 1_000_000),
                                LocalDateTime.of(2019, 1, 1, 1, 1, 1, 2_000_000)),
                        value -> value.stream().map(Timestamp::valueOf).collect(toList()))
                .addInput(arrayDataType(dateDataType(), DATE_ARRAY), asList(LocalDate.of(2019, 1, 1), LocalDate.of(2019, 1, 2)),
                        value -> value.stream().map(Date::valueOf).collect(toList()))
                // TODO support interval
                .executeSuite();
    }

    @Test
    public void testCreatedParameterizedVarchar()
    {
        varcharDataTypeTest()
                .executeSuite();
    }

    private WireProtocolTypeTest varcharDataTypeTest()
    {
        return createTypeTest()
                .addInput(varcharDataType(10), "text_a")
                .addInput(varcharDataType(255), "text_b")
                .addInput(varcharDataType(65535), "text_d")
                .addInput(varcharDataType(10485760), "text_f")
                .addInput(varcharDataType(), "unbounded")
                .addInput(textDataType(), "wren_text");
    }

    @Test(enabled = false, description = "DuckDB does not support unicode")
    public void testCreatedParameterizedVarcharUnicode()
    {
        throw new UnsupportedOperationException();
    }

    @Test(enabled = false, description = "We do not support JSON type in DuckDB yet.")
    public void testJson()
    {
        throw new UnsupportedOperationException();
    }
}
