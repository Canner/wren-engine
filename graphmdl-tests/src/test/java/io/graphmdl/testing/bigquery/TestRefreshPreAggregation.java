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

import com.google.inject.Key;
import io.graphmdl.base.GraphMDL;
import io.graphmdl.main.GraphMDLMetastore;
import org.testng.annotations.Test;

import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestRefreshPreAggregation
        extends AbstractPreAggregationTest
{
    private final GraphMDL graphMDL = getInstance(Key.get(GraphMDLMetastore.class)).getGraphMDL();

    @Override
    protected Optional<String> getGraphMDLPath()
    {
        return Optional.of(requireNonNull(getClass().getClassLoader().getResource("pre_agg/pre_agg_frequently_mdl.json")).getPath());
    }

    @Test
    public void testRefreshFrequently()
            throws InterruptedException
    {
        // manually reload pre-aggregation
        preAggregationManager.refreshPreAggregation(graphMDL);
        // We have one pre-aggregation table and the most tables existing in duckdb is 2
        assertThat(queryDuckdb("show tables").size()).isLessThan(3);
        for (int i = 0; i < 50; i++) {
            System.out.println(getDefaultMetricTablePair("RefreshFrequently").getRequiredTableName());
            Thread.sleep(200);
            assertThat(queryDuckdb("show tables").size()).isLessThan(3);
        }
    }

    // todo need a proper test
    @Test(enabled = false)
    public void testRefreshPreAggregation()
            throws InterruptedException
    {
        String before = getDefaultMetricTablePair("RefreshFrequently").getRequiredTableName();
        // considering the refresh connects to BigQuery service, it will take some time
        Thread.sleep(3000);
        String after = getDefaultMetricTablePair("RefreshFrequently").getRequiredTableName();
        assertThat(before).isNotEqualTo(after);
    }
}
