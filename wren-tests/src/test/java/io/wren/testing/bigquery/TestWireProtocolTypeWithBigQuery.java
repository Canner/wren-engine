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

package io.wren.testing.bigquery;

import com.google.cloud.bigquery.DatasetId;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.airlift.log.Logger;
import io.wren.base.WrenMDL;
import io.wren.base.dto.Manifest;
import io.wren.connector.bigquery.BigQueryClient;
import io.wren.main.metadata.Metadata;
import io.wren.testing.AbstractWireProtocolTypeTest;
import io.wren.testing.TestingWrenServer;
import jdk.jfr.Description;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.function.Function;

import static io.wren.base.Utils.randomIntString;
import static io.wren.base.type.PGArray.BOOL_ARRAY;
import static io.wren.base.type.PGArray.BYTEA_ARRAY;
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
import static io.wren.testing.DataType.timestampDataType;
import static io.wren.testing.DataType.varcharDataType;
import static java.lang.String.format;
import static java.lang.System.getenv;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

public class TestWireProtocolTypeWithBigQuery
        extends AbstractWireProtocolTypeTest
{
    private static final Logger LOG = Logger.get(TestWireProtocolTypeWithBigQuery.class);

    @Override
    protected TestingWrenServer createWrenServer()
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("bigquery.project-id", getenv("TEST_BIG_QUERY_PROJECT_ID"))
                .put("bigquery.location", "asia-east1")
                .put("bigquery.credentials-key", getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"))
                .put("bigquery.metadata.schema.prefix", format("test_%s_", randomIntString()))
                .put("wren.datasource.type", "bigquery")
                .put("pg-wire-protocol.enabled", "true");

        try {
            Path dir = Files.createTempDirectory(getWrenDirectory());
            if (getWrenMDLPath().isPresent()) {
                Files.copy(Path.of(getWrenMDLPath().get()), dir.resolve("mdl.json"));
            }
            else {
                Files.write(dir.resolve("manifest.json"), Manifest.MANIFEST_JSON_CODEC.toJsonBytes(WrenMDL.EMPTY.getManifest()));
            }
            properties.put("wren.directory", dir.toString());
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        return TestingWrenServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
    }

    @Override
    protected void cleanup()
    {
        try {
            Metadata metadata = getInstance(Key.get(Metadata.class));
            BigQueryClient bigQueryClient = getInstance(Key.get(BigQueryClient.class));
            bigQueryClient.dropDatasetWithAllContent(DatasetId.of(getDefaultCatalog(), metadata.getPgCatalogName()));
        }
        catch (Exception ex) {
            LOG.error(ex, "cleanup bigquery schema failed");
        }
    }

    @Override
    protected String getDefaultCatalog()
    {
        return "wrenai";
    }

    @Override
    protected String getDefaultSchema()
    {
        return "tpch_tiny";
    }

    @Override
    @Description("BigQuery has only INT64 type, so we would get long value from it.")
    // TODO https://github.com/Canner/wren/issues/196
    public void testInteger()
    {
        createTypeTest()
                .addInput(integerDataType(), 1_234_567_890, value -> (long) value)
                .addInput(integerDataType(), null)
                .executeSuite();
    }

    @Override
    @Description("BigQuery has only INT64 type, so we would get long value from it.")
    // TODO https://github.com/Canner/wren/issues/196
    public void testSmallint()
    {
        createTypeTest()
                .addInput(smallintDataType(), (short) 32_456, value -> (long) value)
                .addInput(smallintDataType(), null)
                .executeSuite();
    }

    @Override
    @Description("BigQuery has only FLOAT64 type, so we would get double value from it.")
    // TODO https://github.com/Canner/wren/issues/196
    public void testReal()
    {
        createTypeTest()
                .addInput(realDataType(), 123.45f, value -> Double.valueOf(value.toString()))
                .addInput(realDataType(), null)
                .executeSuite();
    }

    @Override
    @Description("BigQuery will remove trailing zeros from values.")
    // TODO https://github.com/Canner/wren/issues/362
    public void testDecimal()
    {
        Function<BigDecimal, BigDecimal> removeTrailingZeros = value -> new BigDecimal(value.stripTrailingZeros().toPlainString());

        createTypeTest()
                .addInput(decimalDataType(), new BigDecimal("0.123"))
                .addInput(decimalDataType(3, 0), new BigDecimal("0"))
                .addInput(decimalDataType(3, 0), new BigDecimal("193"))
                .addInput(decimalDataType(3, 0), new BigDecimal("19"))
                .addInput(decimalDataType(3, 0), new BigDecimal("-193"))
                .addInput(decimalDataType(3, 1), new BigDecimal("10.0"), removeTrailingZeros)
                .addInput(decimalDataType(3, 1), new BigDecimal("10.1"))
                .addInput(decimalDataType(3, 1), new BigDecimal("-10.1"))
                .addInput(decimalDataType(4, 2), new BigDecimal("2.00"), removeTrailingZeros)
                .addInput(decimalDataType(4, 2), new BigDecimal("2.30"), removeTrailingZeros)
                .addInput(decimalDataType(24, 2), new BigDecimal("2.00"), removeTrailingZeros)
                .addInput(decimalDataType(24, 2), new BigDecimal("2.30"), removeTrailingZeros)
                .addInput(decimalDataType(24, 2), new BigDecimal("123456789.30"), removeTrailingZeros)
                .addInput(decimalDataType(24, 4), new BigDecimal("12345678901234567890.3100"), removeTrailingZeros)
                .addInput(decimalDataType(30, 5), new BigDecimal("3141592653589793238462643.38327"))
                .addInput(decimalDataType(30, 5), new BigDecimal("-3141592653589793238462643.38327"))
                .addInput(decimalDataType(30, 0), new BigDecimal("9223372036854775807"))
                .addInput(decimalDataType(38, 0), new BigDecimal("27182818284590452353602874713526624977"))
                .addInput(decimalDataType(38, 9), new BigDecimal("27182818284590452353602874713.526624977"))
                .addInput(decimalDataType(39, 9), new BigDecimal("271828182845904523536028747130.526624977"))
                .addInput(decimalDataType(38, 10), new BigDecimal("2718281828459045235360287471.3526624977"))
                .addInput(decimalDataType(38, 10), null)
                .executeSuite();
    }

    @Override
    // TODO https://github.com/Canner/wren/issues/196
    public void testArray()
    {
        // basic types
        // bigquery array can't contain null value. We don't need to test null array here.
        createTypeTest()
                .addInput(arrayDataType(booleanDataType(), BOOL_ARRAY), asList(true, false))
                .addInput(arrayDataType(smallintDataType(), INT2_ARRAY), asList((short) 1, (short) 2), value -> asList(1L, 2L))
                .addInput(arrayDataType(integerDataType(), INT4_ARRAY), asList(1, 2, 1_234_567_890), value -> asList(1L, 2L, 1_234_567_890L))
                .addInput(arrayDataType(bigintDataType(), INT8_ARRAY), asList(123_456_789_012L, 1_234_567_890L))
                .addInput(arrayDataType(realDataType(), FLOAT4_ARRAY), asList(123.45f, 1.2345f), value -> asList(123.45, 1.2345))
                .addInput(arrayDataType(doubleDataType(), FLOAT8_ARRAY), asList(123.45d, 1.2345d))
                .addInput(arrayDataType(decimalDataType(3, 1), NUMERIC_ARRAY), asList(new BigDecimal("1"), new BigDecimal("1.1")))
                .addInput(arrayDataType(varcharDataType(), VARCHAR_ARRAY), asList("hello", "world"))
                .addInput(arrayDataType(charDataType(), CHAR_ARRAY), asList("h", "w"))
                .addInput(arrayDataType(byteaDataType(), BYTEA_ARRAY), asList("hello", "world"), value -> asList("\\x68656c6c6f", "\\x776f726c64"))
                .addInput(arrayDataType(jsonDataType(), JSON_ARRAY), asList("{\"a\": \"apple\"}", "{\"b\": \"banana\"}"), value -> asList("{a:apple}", "{b:banana}"))
                .addInput(arrayDataType(timestampDataType(3), TIMESTAMP_ARRAY),
                        asList(LocalDateTime.of(2019, 1, 1, 1, 1, 1, 1_000_000),
                                LocalDateTime.of(2019, 1, 1, 1, 1, 1, 2_000_000)),
                        value -> value.stream().map(Timestamp::valueOf).collect(toList()))
                .addInput(arrayDataType(dateDataType(), DATE_ARRAY), asList(LocalDate.of(2019, 1, 1), LocalDate.of(2019, 1, 2)),
                        value -> value.stream().map(Date::valueOf).collect(toList()))
                // TODO support interval
                .executeSuite();
    }
}
