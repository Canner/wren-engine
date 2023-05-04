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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.graphmdl.base.ConnectorRecordIterator;
import io.graphmdl.base.Parameter;
import io.graphmdl.base.SessionContext;
import io.graphmdl.connector.AutoCloseableIterator;
import io.graphmdl.connector.duckdb.DuckdbClient;
import io.graphmdl.main.biboost.PreAggregationManager;
import io.graphmdl.main.metadata.Metadata;
import io.graphmdl.testing.AbstractWireProtocolTest;
import io.graphmdl.testing.TestingGraphMDLServer;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.graphmdl.base.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static java.lang.System.getenv;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;

public class TestPreAggregation
        extends AbstractWireProtocolTest
{
    private final PreAggregationManager preAggregationManager = getInstance(Key.get(PreAggregationManager.class));

    @Override
    protected TestingGraphMDLServer createGraphMDLServer()
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("bigquery.project-id", getenv("TEST_BIG_QUERY_PROJECT_ID"))
                .put("bigquery.location", "asia-east1")
                .put("bigquery.credentials-key", getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"))
                .put("bigquery.bucket-name", getenv("TEST_BIG_QUERY_BUCKET_NAME"))
                .put("duckdb.storage.access-key", getenv("TEST_DUCKDB_STORAGE_ACCESS_KEY"))
                .put("duckdb.storage.secret-key", getenv("TEST_DUCKDB_STORAGE_SECRET_KEY"));

        if (getGraphMDLPath().isPresent()) {
            properties.put("graphmdl.file", getGraphMDLPath().get());
        }

        return TestingGraphMDLServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
    }

    @Override
    protected Optional<String> getGraphMDLPath()
    {
        return Optional.of(requireNonNull(getClass().getClassLoader().getResource("pre_agg_mdl.json")).getPath());
    }

    @Test
    public void testPreAggregation()
    {
        String mappingName = preAggregationManager.getPreAggregationMetricTablePair("canner-cml", "tpch_tiny", "Revenue").getRequiredTableName();
        List<Object[]> tables = queryDuckdb("show tables");

        assertThat(tables.size()).isOne();
        assertThat(tables.get(0)[0]).isEqualTo(mappingName);

        List<Object[]> duckdbResult = queryDuckdb(format("select * from \"%s\"", mappingName));
        List<Object[]> bqResult = queryBigQuery(format("SELECT\n" +
                "     o_custkey\n" +
                "   , sum(o_totalprice) revenue\n" +
                "   FROM\n" +
                "     `%s.%s.%s`\n" +
                "   GROUP BY o_custkey", "canner-cml", "tpch_tiny", "orders"));
        assertThat(duckdbResult.size()).isEqualTo(bqResult.size());
        assertThat(Arrays.deepEquals(duckdbResult.toArray(), bqResult.toArray())).isTrue();

        String errMsg = preAggregationManager.getPreAggregationMetricTablePair("canner-cml", "tpch_tiny", "unqualified").getErrorMessage()
                .orElseThrow(AssertionError::new);
        assertThat(errMsg).matches("Failed to do pre-aggregation for metric .*");
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
        SessionContext sessionContext = SessionContext.builder()
                .setCatalog("canner-cml")
                .setSchema("tpch_tiny")
                .build();
        String rewritten = preAggregationManager.rewritePreAggregation(sessionContext, "select custkey, revenue from Revenue limit 100").orElseThrow(AssertionError::new);
        try (ConnectorRecordIterator connectorRecordIterator = preAggregationManager.query(rewritten, ImmutableList.of())) {
            int count = 0;
            while (connectorRecordIterator.hasNext()) {
                count++;
                connectorRecordIterator.next();
            }
            assertThat(count).isEqualTo(100);
        }

        String withParam = preAggregationManager.rewritePreAggregation(sessionContext, "select custkey, revenue from Revenue where custkey = ?").orElseThrow(AssertionError::new);
        try (ConnectorRecordIterator connectorRecordIterator = preAggregationManager.query(withParam, ImmutableList.of(new Parameter(INTEGER, 1202)))) {
            Object[] result = connectorRecordIterator.next();
            assertThat(result.length).isEqualTo(2);
            assertThat(connectorRecordIterator.next()[0]).isEqualTo(1202L);
            assertThat(connectorRecordIterator.hasNext()).isFalse();
        }
    }

    private List<Object[]> queryDuckdb(String statement)
    {
        DuckdbClient client = getInstance(Key.get(DuckdbClient.class));
        try (AutoCloseableIterator<Object[]> iterator = client.query(statement)) {
            ImmutableList.Builder<Object[]> builder = ImmutableList.builder();
            while (iterator.hasNext()) {
                builder.add(iterator.next());
            }
            return builder.build();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<Object[]> queryBigQuery(String statement)
    {
        Metadata bigQueryMetadata = getInstance(Key.get(Metadata.class));
        try (ConnectorRecordIterator iterator = bigQueryMetadata.directQuery(statement, List.of())) {
            ImmutableList.Builder<Object[]> builder = ImmutableList.builder();
            while (iterator.hasNext()) {
                builder.add(iterator.next());
            }
            return builder.build();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
