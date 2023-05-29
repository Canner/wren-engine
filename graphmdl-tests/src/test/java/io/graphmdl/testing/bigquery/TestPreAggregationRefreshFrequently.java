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
public class TestPreAggregationRefreshFrequently
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
        preAggregationManager.refreshPreAggregation(graphMDL);
        assertThat(queryDuckdb("show tables").size()).isLessThan(3);
        for (int i = 0; i < 10; i++) {
            Thread.sleep(200);
            assertThat(queryDuckdb("show tables").size()).isLessThan(3);
        }
    }
}
