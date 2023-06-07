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

import com.google.common.collect.ImmutableList;
import com.google.inject.Key;
import io.graphmdl.base.ConnectorRecordIterator;
import io.graphmdl.base.GraphMDL;
import io.graphmdl.base.Parameter;
import io.graphmdl.base.SessionContext;
import io.graphmdl.base.client.duckdb.DuckdbClient;
import io.graphmdl.main.GraphMDLMetastore;
import io.graphmdl.sqlrewrite.PreAggregationRewrite;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.graphmdl.base.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;

@Test(singleThreaded = true)
public class TestPreAggregation
        extends AbstractPreAggregationTest
{
    private static final Function<String, String> dropTableStatement = (tableName) -> format("BEGIN TRANSACTION;DROP TABLE IF EXISTS %s;COMMIT;", tableName);
    private final GraphMDL graphMDL = getInstance(Key.get(GraphMDLMetastore.class)).getGraphMDL();
    private final DuckdbClient duckdbClient = getInstance(Key.get(DuckdbClient.class));
    private final SessionContext defaultSessionContext = SessionContext.builder()
            .setCatalog("canner-cml")
            .setSchema("tpch_tiny")
            .build();

    @Test
    public void testPreAggregation()
    {
        String mappingName = getDefaultMetricTablePair("Revenue").getRequiredTableName();
        assertThat(getDefaultMetricTablePair("AvgRevenue")).isNull();
        List<Object[]> tables = queryDuckdb("show tables");

        Set<String> tableNames = tables.stream().map(table -> table[0].toString()).collect(toImmutableSet());
        assertThat(tableNames).contains(mappingName);

        List<Object[]> duckdbResult = queryDuckdb(format("select * from \"%s\"", mappingName));
        List<Object[]> bqResult = queryBigQuery(format("SELECT\n" +
                "     o_custkey\n" +
                "   , sum(o_totalprice) revenue\n" +
                "   FROM\n" +
                "     `%s.%s.%s`\n" +
                "   GROUP BY o_custkey", "canner-cml", "tpch_tiny", "orders"));
        assertThat(duckdbResult.size()).isEqualTo(bqResult.size());
        assertThat(Arrays.deepEquals(duckdbResult.toArray(), bqResult.toArray())).isTrue();

        String errMsg = getDefaultMetricTablePair("unqualified").getErrorMessage()
                .orElseThrow(AssertionError::new);
        assertThat(errMsg).matches("Failed to do pre-aggregation for preAggregationInfo .*");
    }

    @Test
    public void testMetricWithoutPreAggregation()
    {
        assertThat(getDefaultMetricTablePair("AvgRevenue")).isNull();
    }

    @Override
    protected Optional<String> getGraphMDLPath()
    {
        return Optional.of(requireNonNull(getClass().getClassLoader().getResource("pre_agg/pre_agg_mdl.json")).getPath());
    }

    @Test
    public void testQueryMetric()
            throws Exception
    {
        try (Connection connection = createConnection();
                PreparedStatement stmt = connection.prepareStatement("select custkey, revenue from Revenue limit 100");
                ResultSet resultSet = stmt.executeQuery()) {
            resultSet.next();
            assertThatNoException().isThrownBy(() -> resultSet.getLong("custkey"));
            assertThatNoException().isThrownBy(() -> resultSet.getInt("revenue"));
            int count = 1;

            while (resultSet.next()) {
                count++;
            }
            assertThat(count).isEqualTo(100);
        }

        try (Connection connection = createConnection();
                PreparedStatement stmt = connection.prepareStatement("select custkey, revenue from Revenue where custkey = ?")) {
            stmt.setObject(1, 1202);
            try (ResultSet resultSet = stmt.executeQuery()) {
                resultSet.next();
                assertThatNoException().isThrownBy(() -> resultSet.getLong("custkey"));
                assertThatNoException().isThrownBy(() -> resultSet.getInt("revenue"));
                assertThat(resultSet.getLong("custkey")).isEqualTo(1202L);
                assertThat(resultSet.next()).isFalse();
            }
        }
    }

    @Test
    public void testExecuteRewrittenQuery()
            throws Exception
    {
        String rewritten =
                PreAggregationRewrite.rewrite(
                                defaultSessionContext,
                                "select custkey, revenue from Revenue limit 100",
                                preAggregationTableMapping::convertToAggregationTable,
                                graphMDL)
                        .orElseThrow(AssertionError::new);
        try (ConnectorRecordIterator connectorRecordIterator = preAggregationManager.query(rewritten, ImmutableList.of())) {
            int count = 0;
            while (connectorRecordIterator.hasNext()) {
                count++;
                connectorRecordIterator.next();
            }
            assertThat(count).isEqualTo(100);
        }

        String withParam =
                PreAggregationRewrite.rewrite(
                                defaultSessionContext,
                                "select custkey, revenue from Revenue where custkey = ?",
                                preAggregationTableMapping::convertToAggregationTable,
                                graphMDL)
                        .orElseThrow(AssertionError::new);
        try (ConnectorRecordIterator connectorRecordIterator = preAggregationManager.query(withParam, ImmutableList.of(new Parameter(INTEGER, 1202)))) {
            Object[] result = connectorRecordIterator.next();
            assertThat(result.length).isEqualTo(2);
            assertThat(connectorRecordIterator.next()[0]).isEqualTo(1202L);
            assertThat(connectorRecordIterator.hasNext()).isFalse();
        }
    }

    @Test
    public void testQueryMetricWithDroppedPreAggTable()
            throws SQLException
    {
        String tableName = getDefaultMetricTablePair("ForDropTable").getRequiredTableName();
        List<Object[]> origin = queryDuckdb(format("select * from %s", tableName));
        assertThat(origin.size()).isGreaterThan(0);
        duckdbClient.executeDDL(dropTableStatement.apply(tableName));

        try (Connection connection = createConnection();
                PreparedStatement stmt = connection.prepareStatement("select custkey, revenue from ForDropTable limit 100");
                ResultSet resultSet = stmt.executeQuery()) {
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            assertThat(count).isEqualTo(100);
        }
    }
}
