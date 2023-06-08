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
import io.graphmdl.base.client.AutoCloseableIterator;
import io.graphmdl.base.client.duckdb.DuckdbClient;
import io.graphmdl.base.type.DateType;
import io.graphmdl.base.type.PGType;
import io.graphmdl.main.metadata.Metadata;
import io.graphmdl.preaggregation.PreAggregationManager;
import io.graphmdl.preaggregation.PreAggregationTableMapping;
import io.graphmdl.testing.AbstractWireProtocolTest;
import io.graphmdl.testing.TestingGraphMDLServer;

import java.sql.Date;
import java.util.List;

import static java.lang.System.getenv;

public abstract class AbstractPreAggregationTest
        extends AbstractWireProtocolTest
{
    protected final PreAggregationManager preAggregationManager = getInstance(Key.get(PreAggregationManager.class));
    protected final PreAggregationTableMapping preAggregationTableMapping = getInstance(Key.get(PreAggregationTableMapping.class));
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

    protected PreAggregationTableMapping.PreAggregationInfoPair getDefaultMetricTablePair(String metric)
    {
        return preAggregationTableMapping.getPreAggregationInfoPair("canner-cml", "tpch_tiny", metric);
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
            List<PGType> pgTypes = iterator.getTypes();
            ImmutableList.Builder<Object[]> builder = ImmutableList.builder();
            while (iterator.hasNext()) {
                Object[] bqResults = iterator.next();
                Object[] returnResults = new Object[pgTypes.size()];
                for (int i = 0; i < pgTypes.size(); i++) {
                    returnResults[i] = bqResults[i];
                    if (DateType.DATE.oid() == pgTypes.get(i).oid()) {
                        returnResults[i] = Date.valueOf(bqResults[i].toString());
                    }
                }
                builder.add(returnResults);
            }
            return builder.build();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
