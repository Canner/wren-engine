package io.graphmdl.testing.bigquery;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.graphmdl.base.ConnectorRecordIterator;
import io.graphmdl.base.client.AutoCloseableIterator;
import io.graphmdl.base.client.duckdb.DuckdbClient;
import io.graphmdl.main.metadata.Metadata;
import io.graphmdl.preaggregation.PreAggregationManager;
import io.graphmdl.testing.AbstractWireProtocolTest;
import io.graphmdl.testing.TestingGraphMDLServer;

import java.util.List;

import static java.lang.System.getenv;

public abstract class AbstractPreAggregationTest
        extends AbstractWireProtocolTest
{
    protected final PreAggregationManager preAggregationManager = getInstance(Key.get(PreAggregationManager.class));
    protected final DuckdbClient duckdbClient = getInstance(Key.get(DuckdbClient.class));

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

    protected PreAggregationManager.MetricTablePair getDefaultMetricTablePair(String metric)
    {
        return preAggregationManager.getPreAggregationMetricTablePair("canner-cml", "tpch_tiny", metric);
    }

    protected List<Object[]> queryDuckdb(String statement)
    {
        try (AutoCloseableIterator<Object[]> iterator = duckdbClient.query(statement)) {
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

    protected List<Object[]> queryBigQuery(String statement)
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
