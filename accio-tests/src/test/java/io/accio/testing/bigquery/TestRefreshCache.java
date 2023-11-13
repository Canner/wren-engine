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

package io.accio.testing.bigquery;

import com.google.inject.Key;
import io.accio.base.AccioMDL;
import io.accio.main.AccioMetastore;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestRefreshCache
        extends AbstractCacheTest
{
    private final Supplier<AccioMDL> accioMDL = () -> getInstance(Key.get(AccioMetastore.class)).getAccioMDL();

    @Override
    protected Optional<String> getAccioMDLPath()
    {
        return Optional.of(requireNonNull(getClass().getClassLoader().getResource("cache/cache_frequently_mdl.json")).getPath());
    }

    @Test
    public void testRefreshFrequently()
            throws InterruptedException
    {
        // manually reload cache
        cacheManager.get().createTaskUtilDone(accioMDL.get());
        // We have one cached table and the most tables existing in duckdb is 2
        assertThat(queryDuckdb("show tables").size()).isLessThan(3);
        for (int i = 0; i < 50; i++) {
            Thread.sleep(200);
            assertThat(queryDuckdb("show tables").size()).isLessThan(3);
        }
    }

    // todo need a proper test
    @Test(enabled = false)
    public void testRefreshCache()
            throws InterruptedException
    {
        String before = getDefaultCacheInfoPair("RefreshFrequently").getRequiredTableName();
        // considering the refresh connects to BigQuery service, it will take some time
        Thread.sleep(3000);
        String after = getDefaultCacheInfoPair("RefreshFrequently").getRequiredTableName();
        assertThat(before).isNotEqualTo(after);
    }
}
