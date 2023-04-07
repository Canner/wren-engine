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
import io.graphmdl.connector.AutoCloseableIterator;
import io.graphmdl.connector.duckdb.DuckdbClient;
import io.graphmdl.main.biboost.PreAggregationManager;
import io.graphmdl.main.metadata.Metadata;
import io.graphmdl.testing.RequireGraphMDLServer;
import io.graphmdl.testing.TestingGraphMDLServer;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.lang.String.format;
import static java.lang.System.getenv;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPreAggregation
        extends RequireGraphMDLServer
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
