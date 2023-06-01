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

package io.graphmdl.testing.bigquery;

import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;

@Test(singleThreaded = true)
public class TestBigQueryPreAggregation
        extends AbstractPreAggregationTest
{
    @Override
    protected Optional<String> getGraphMDLPath()
    {
        return Optional.of(requireNonNull(getClass().getClassLoader().getResource("pre_agg/pre_agg_bigquery_mdl.json")).getPath());
    }

    @Override
    protected Properties getDefaultProperties()
    {
        Properties props = new Properties();
        props.setProperty("password", MOCK_PASSWORD);
        props.setProperty("user", "graphmdl");
        props.setProperty("ssl", "false");
        props.setProperty("currentSchema", "cml_temp");
        return props;
    }

    /*
     * CREATE TABLE `canner-cml.cml_temp.preagg_bigquery_type`(
     * c_string STRING, c_bytes BYTES, c_integer INT64, c_float FLOAT64, c_numeric NUMERIC,
     * c_bignumeric BIGNUMERIC, c_boolean BOOL, c_timestamp TIMESTAMP, c_date DATE, c_datetime DATETIME,
     * c_time TIME, c_geography GEOGRAPHY, c_json JSON, c_interval INTERVAL, c_struct STRUCT<s1 INT64, s2 STRING>,
     * c_array_string ARRAY<STRING>);
     *
     * INSERT INTO `canner-cml.cml_temp.preagg_bigquery_type`(
     * c_string, c_bytes, c_integer, c_float, c_numeric,
     * c_bignumeric, c_boolean, c_timestamp, c_date, c_datetime,
     * c_time, c_geography, c_json, c_interval,
     * c_struct,c_array_string, c_array_integer)
     * VALUES(
     * 'hello', B'hello', 12345, 1.2345, 1.2345,
     * 1.2345, true, '2020-01-01 15:10:55', '2020-01-01', '2020-01-01 15:10:55.123456',
     * '15:10:55', ST_GEOGPOINT(30, 50), PARSE_JSON("{\"a\": 1}"), INTERVAL '1' day, (1, "hello"),
     * ["hello", "world"]);
     */
    @Test
    public void testType()
            throws SQLException
    {
        String mappingName = preAggregationManager.getPreAggregationMetricTablePair("canner-cml", "cml_temp", "PrintBigQueryType").getRequiredTableName();
        List<Object[]> tables = queryDuckdb("show tables");

        Set<String> tableNames = tables.stream().map(table -> table[0].toString()).collect(toImmutableSet());
        assertThat(tableNames).contains(mappingName);

        try (Connection connection = createConnection();
                PreparedStatement stmt = connection.prepareStatement("select * from PrintBigQueryType");
                ResultSet resultSet = stmt.executeQuery()) {
            resultSet.next();
            assertThatNoException().isThrownBy(() -> resultSet.getString("c_string"));
//                assertThatNoException().isThrownBy(() -> resultSet.getString("c_bytes"));
            assertThatNoException().isThrownBy(() -> resultSet.getLong("c_integer"));
            assertThatNoException().isThrownBy(() -> resultSet.getDouble("c_float"));
//                assertThatNoException().isThrownBy(() -> resultSet.getBigDecimal("c_numeric"));
//                assertThatNoException().isThrownBy(() -> resultSet.getBigDecimal("c_bignumeric"));
            assertThatNoException().isThrownBy(() -> resultSet.getBoolean("c_boolean"));
//                assertThatNoException().isThrownBy(() -> resultSet.getTimestamp("c_timestamp"));
//                assertThatNoException().isThrownBy(() -> resultSet.getDate("c_date"));
//                assertThatNoException().isThrownBy(() -> resultSet.getTimestamp("c_datetime"));
//                assertThatNoException().isThrownBy(() -> resultSet.getString("c_json"));
//                assertThatNoException().isThrownBy(() -> resultSet.getString("c_interval"));
//                assertThatNoException().isThrownBy(() -> resultSet.getString("c_struct"));
//                assertThatNoException().isThrownBy(() -> resultSet.getString("c_array_string"));

            assertThat(resultSet.getString("c_string")).isEqualTo("hello");
//                assertThat(resultSet.getBytes("c_bytes")).isEqualTo("hello".getBytes(UTF_8));
            assertThat(resultSet.getLong("c_integer")).isEqualTo(12345L);
            assertThat(resultSet.getDouble("c_float")).isEqualTo(1.2345);
//                assertThat(resultSet.getBigDecimal("c_numeric")).isEqualTo(new BigDecimal("1.2345"));
//                assertThat(resultSet.getBigDecimal("c_bignumeric")).isEqualTo(new BigDecimal("1.2345"));
            assertThat(resultSet.getBoolean("c_boolean")).isTrue();
//                assertThat(resultSet.getTimestamp("c_timestamp")).isEqualTo(Timestamp.valueOf("2020-01-01 15:10:55"));
//                assertThat(resultSet.getDate("c_date")).isEqualTo(Date.valueOf("2020-01-01"));
//                assertThat(resultSet.getTimestamp("c_datetime")).isEqualTo(Timestamp.valueOf("2020-01-01 15:10:55.123456"));
//                assertThat(resultSet.getString("c_json")).isEqualTo("{\"a\":1}");
//            assertThat(resultSet.getObject("c_interval")).isEqualTo(new PGInterval(0, 0, 1, 0, 0, 0));
//                assertThat(resultSet.getObject("c_struct")).isEqualTo(ImmutableList.of(1L, "hello"));
//                assertThat(resultSet.getObject("c_array_string")).isEqualTo(ImmutableList.of("hello", "world"));
        }
    }
}
