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

package io.wren.testing.snowflake;

import com.google.common.collect.ImmutableMap;
import io.wren.base.config.SQLGlotConfig;
import io.wren.testing.AbstractWireProtocolTypeTest;
import io.wren.testing.TestingSQLGlotServer;
import io.wren.testing.TestingWrenServer;
import jdk.jfr.Description;
import org.apache.commons.codec.binary.Hex;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static io.wren.base.config.SQLGlotConfig.SQLGLOT_PORT;
import static io.wren.base.config.SQLGlotConfig.createConfigWithFreePort;
import static io.wren.base.config.SnowflakeConfig.SNOWFLAKE_DATABASE;
import static io.wren.base.config.SnowflakeConfig.SNOWFLAKE_JDBC_URL;
import static io.wren.base.config.SnowflakeConfig.SNOWFLAKE_PASSWORD;
import static io.wren.base.config.SnowflakeConfig.SNOWFLAKE_SCHEMA;
import static io.wren.base.config.SnowflakeConfig.SNOWFLAKE_USER;
import static io.wren.base.config.WrenConfig.DataSourceType.SNOWFLAKE;
import static io.wren.base.config.WrenConfig.WREN_DATASOURCE_TYPE;
import static io.wren.base.config.WrenConfig.WREN_DIRECTORY;
import static io.wren.base.type.PGArray.BOOL_ARRAY;
import static io.wren.base.type.PGArray.BYTEA_ARRAY;
import static io.wren.base.type.PGArray.CHAR_ARRAY;
import static io.wren.base.type.PGArray.DATE_ARRAY;
import static io.wren.base.type.PGArray.FLOAT4_ARRAY;
import static io.wren.base.type.PGArray.FLOAT8_ARRAY;
import static io.wren.base.type.PGArray.INT2_ARRAY;
import static io.wren.base.type.PGArray.INT4_ARRAY;
import static io.wren.base.type.PGArray.INT8_ARRAY;
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
import static io.wren.testing.DataType.realDataType;
import static io.wren.testing.DataType.smallintDataType;
import static io.wren.testing.DataType.textDataType;
import static io.wren.testing.DataType.timestampDataType;
import static io.wren.testing.DataType.varcharDataType;
import static java.lang.System.getenv;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Locale.ENGLISH;

public class TestWireProtocolTypeWithSnowflake
        extends AbstractWireProtocolTypeTest
{
    private TestingSQLGlotServer testingSQLGlotServer;

    @BeforeClass
    public void init()
            throws Exception
    {
        testingSQLGlotServer = closer.register(prepareSQLGlot());
        wrenServer = closer.register(createWrenServer());
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws IOException
    {
        closer.close();
    }

    private TestingSQLGlotServer prepareSQLGlot()
    {
        SQLGlotConfig config = createConfigWithFreePort();
        return new TestingSQLGlotServer(config);
    }

    @Override
    protected TestingWrenServer createWrenServer()
            throws Exception
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put(WREN_DATASOURCE_TYPE, SNOWFLAKE.name())
                .put(WREN_DIRECTORY, Files.createTempDirectory(getWrenDirectory()).toString())
                .put(SNOWFLAKE_JDBC_URL, getenv("SNOWFLAKE_JDBC_URL"))
                .put(SNOWFLAKE_USER, getenv("SNOWFLAKE_USER"))
                .put(SNOWFLAKE_PASSWORD, getenv("SNOWFLAKE_PASSWORD"))
                .put(SNOWFLAKE_DATABASE, "SNOWFLAKE_SAMPLE_DATA")
                .put(SNOWFLAKE_SCHEMA, "TPCH_SF1")
                .put(SQLGLOT_PORT, String.valueOf(testingSQLGlotServer.getPort()));

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

    @Test
    @Description("Snowflake jdbc does not provide getArray and it will make the array to string.")
    public void testArray()
    {
        createTypeTest()
                .addInput(arrayDataType(booleanDataType(), BOOL_ARRAY), asList(true, false), this::formatArray)
                .addInput(arrayDataType(smallintDataType(), INT2_ARRAY), asList((short) 1, (short) 2), this::formatArray)
                .addInput(arrayDataType(integerDataType(), INT4_ARRAY), asList(1, 2, 1_234_567_890), this::formatArray)
                .addInput(arrayDataType(bigintDataType(), INT8_ARRAY), asList(123_456_789_012L, 1_234_567_890L), this::formatArray)
                .addInput(arrayDataType(realDataType(), FLOAT4_ARRAY), asList(123.45f, 1.2345f), ignored -> "[\n  1.234500000000000e+02,\n  1.234500000000000e+00\n]")
                .addInput(arrayDataType(doubleDataType(), FLOAT8_ARRAY), asList(123.45d, 1.2345d), ignored -> "[\n  1.234500000000000e+02,\n  1.234500000000000e+00\n]")
                .addInput(arrayDataType(decimalDataType(3, 1), NUMERIC_ARRAY), asList(new BigDecimal("1"), new BigDecimal("1.1")), this::formatArray)
                .addInput(arrayDataType(varcharDataType(), VARCHAR_ARRAY), asList("hello", "world"), this::formatArray)
                .addInput(arrayDataType(charDataType(), CHAR_ARRAY), asList("h", "w"), this::formatArray)
                .addInput(arrayDataType(byteaDataType(), BYTEA_ARRAY), asList("hello", "world"), value -> formatArray(value.stream().map(s -> Hex.encodeHexString(s.getBytes(UTF_8)).toUpperCase(ENGLISH)).toList()))
                .addInput(arrayDataType(timestampDataType(3), TIMESTAMP_ARRAY),
                        asList(LocalDateTime.of(2019, 1, 1, 1, 1, 1, 1_000_000),
                                LocalDateTime.of(2019, 1, 1, 1, 1, 1, 2_000_000)),
                        value -> formatArray(value.stream().map(Timestamp::valueOf).map(Timestamp::toString).toList()))
                .addInput(arrayDataType(dateDataType(), DATE_ARRAY), asList(LocalDate.of(2019, 1, 1), LocalDate.of(2019, 1, 2)),
                        value -> formatArray(value.stream().map(Date::valueOf).map(Date::toString).toList()))
                .addInput(arrayDataType(textDataType(), VARCHAR_ARRAY), null)
                .executeSuite();
    }

    /**
     * Format array to string for Snowflake jdbc output
     * "[
     * "hello",
     * "world"
     * ]"
     */
    private String formatArray(List<?> arr)
    {
        StringBuilder sb = new StringBuilder("[\n");
        for (int i = 0; i < arr.size(); i++) {
            sb.append("  ");
            if (arr.get(i) instanceof String s) {
                sb.append("\"").append(s).append("\"");
            }
            else {
                sb.append(arr.get(i));
            }
            if (i < arr.size() - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }
        sb.append("]");
        return sb.toString();
    }

    @Test
    @Override
    @Description("Snowflake make BIGINT to NUMBER, and we will get BigDecimal value from it.")
    public void tesBigint()
    {
        createTypeTest()
                .addInput(bigintDataType(), 123_456_789_012L, BigDecimal::new)
                .addInput(bigintDataType(), null)
                .executeSuite();
    }

    @Test
    @Override
    @Description("Snowflake make INT to NUMBER same with BIGINT, and we will get BigDecimal value from it.")
    public void testInteger()
    {
        createTypeTest()
                .addInput(integerDataType(), 1_234_567_890, BigDecimal::new)
                .addInput(integerDataType(), null)
                .executeSuite();
    }

    @Test
    @Override
    @Description("Snowflake make SMALLINT to NUMBER same with BIGINT, and we will get BigDecimal value from it.")
    public void testSmallint()
    {
        createTypeTest()
                .addInput(smallintDataType(), (short) 32_456, BigDecimal::new)
                .addInput(smallintDataType(), null)
                .executeSuite();
    }

    @Test
    @Override
    @Description("DECIMAL is synonymous with NUMBER in the Snowflake. The precision(Total number of digits allowed) of NUMBER is up to 38. The default precision and scale is NUMBER(38,0).")
    public void testDecimal()
    {
        createTypeTest()
                .addInput(decimalDataType(), new BigDecimal("0.123"), value -> new BigDecimal("0"))
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
                .addInput(decimalDataType(38, 10), null)
                .executeSuite();
    }

    @Test
    @Override
    @Description("REAL is synonymous with FLOAT in the Snowflake.")
    public void testReal()
    {
        createTypeTest()
                .addInput(realDataType(), 123.45f, value -> 123.45d)
                .addInput(realDataType(), null)
                .executeSuite();
    }

    @Test
    @Override
    @Description("Snowflake does not have `name` type")
    public void testCreatedParameterizedVarchar()
    {
        createTypeTest()
                .addInput(varcharDataType(10), "text_a")
                .addInput(varcharDataType(255), "text_b")
                .addInput(varcharDataType(65535), "text_d")
                .addInput(varcharDataType(10485760), "text_f")
                .addInput(varcharDataType(), "unbounded")
                .addInput(textDataType(), "wren_text")
                .addInput(textDataType(), null)
                .addInput(varcharDataType(), null)
                .executeSuite();
    }

    @Test
    @Override
    @Description("Snowflake does not support cast string to bytea, and we will use HEX_ENCODE function to convert to bytea.")
    public void testBytea()
    {
        createTypeTest()
                .addInput(byteaDataType(), "hello", value -> value.getBytes(UTF_8))
                .addInput(byteaDataType(), "\\x68656c6c6f", ignored -> "hello".getBytes(UTF_8))
                .addInput(byteaDataType(), null)
                .executeSuite();
    }

    @Override
    @Test(enabled = false, description = "We do not support JSON type in Snowflake yet.")
    public void testJson()
    {
        throw new UnsupportedOperationException();
    }

    @Test(enabled = false, description = "Sqlglot does not support unicode.")
    public void testCreatedParameterizedVarcharUnicode()
    {
        throw new UnsupportedOperationException();
    }
}
