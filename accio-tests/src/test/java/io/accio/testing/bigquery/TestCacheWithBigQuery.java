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

import com.google.common.collect.ImmutableList;
import io.accio.base.ConnectorRecordIterator;
import io.accio.base.Parameter;
import io.accio.base.sqlrewrite.CacheRewrite;
import io.accio.testing.TestCache;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Optional;

import static io.accio.base.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;

@Test(singleThreaded = true)
public class TestCacheWithBigQuery
        extends TestCache
{
    @Override
    protected Optional<String> getAccioMDLPath()
    {
        return Optional.of(requireNonNull(getClass().getClassLoader().getResource("cache/cache_mdl.json")).getPath());
    }

    @Override
    protected String getDefaultCatalog()
    {
        return "canner-cml";
    }

    @Override
    protected String getDefaultSchema()
    {
        return "tpch_tiny";
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
                CacheRewrite.rewrite(
                                defaultSessionContext,
                                "select custkey, revenue from Revenue limit 100",
                                cachedTableMapping.get()::convertToCachedTable,
                                accioMDL.get())
                        .orElseThrow(AssertionError::new);
        try (ConnectorRecordIterator connectorRecordIterator = cacheManager.get().query(rewritten, ImmutableList.of())) {
            int count = 0;
            while (connectorRecordIterator.hasNext()) {
                count++;
                connectorRecordIterator.next();
            }
            assertThat(count).isEqualTo(100);
        }

        String withParam =
                CacheRewrite.rewrite(
                                defaultSessionContext,
                                "select custkey, revenue from Revenue where custkey = ?",
                                cachedTableMapping.get()::convertToCachedTable,
                                accioMDL.get())
                        .orElseThrow(AssertionError::new);
        try (ConnectorRecordIterator connectorRecordIterator = cacheManager.get().query(withParam, ImmutableList.of(new Parameter(INTEGER, 1202)))) {
            Object[] result = connectorRecordIterator.next();
            assertThat(result.length).isEqualTo(2);
            assertThat(result[0]).isEqualTo(1202L);
            assertThat(connectorRecordIterator.hasNext()).isFalse();
        }
    }
}
