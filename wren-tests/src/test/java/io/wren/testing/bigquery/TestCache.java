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

package io.wren.testing.bigquery;

import com.google.common.collect.ImmutableList;
import com.google.inject.Key;
import io.wren.base.ConnectorRecordIterator;
import io.wren.base.Parameter;
import io.wren.base.SessionContext;
import io.wren.base.WrenMDL;
import io.wren.base.sqlrewrite.CacheRewrite;
import io.wren.cache.CacheInfoPair;
import io.wren.cache.TaskInfo;
import io.wren.main.WrenMetastore;
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
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.wren.base.CatalogSchemaTableName.catalogSchemaTableName;
import static io.wren.base.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;

@Test(singleThreaded = true)
public class TestCache
        extends AbstractCacheTest
{
    private static final Function<String, String> dropTableStatement = (tableName) -> format("BEGIN TRANSACTION;DROP TABLE IF EXISTS %s;COMMIT;", tableName);
    private final Supplier<WrenMDL> wrenMDL = () -> getInstance(Key.get(WrenMetastore.class)).getAnalyzedMDL().getWrenMDL();
    private final SessionContext defaultSessionContext = SessionContext.builder()
            .setCatalog("canner-cml")
            .setSchema("tpch_tiny")
            .build();

    @Test
    public void testCache()
    {
        Optional<CacheInfoPair> cacheInfoPairOptional = getDefaultCacheInfoPair("Revenue");
        assertThat(cacheInfoPairOptional).isPresent();

        String mappingName = cacheInfoPairOptional.get().getRequiredTableName();
        assertThat(getDefaultCacheInfoPair("AvgRevenue")).isEmpty();
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

        String errMsg = getDefaultCacheInfoPair("unqualified")
                .flatMap(CacheInfoPair::getErrorMessage)
                .orElseThrow(AssertionError::new);
        assertThat(errMsg).matches("Failed to do cache for cacheInfo .*");
    }

    @Test
    public void testMetricWithoutCache()
    {
        assertThat(getDefaultCacheInfoPair("AvgRevenue")).isEmpty();
    }

    @Override
    protected Optional<String> getWrenMDLPath()
    {
        return Optional.of(requireNonNull(getClass().getClassLoader().getResource("cache/cache_mdl.json")).getPath());
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
                                wrenMDL.get())
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
                                wrenMDL.get())
                        .orElseThrow(AssertionError::new);
        try (ConnectorRecordIterator connectorRecordIterator = cacheManager.get().query(withParam, ImmutableList.of(new Parameter(INTEGER, 1202)))) {
            Object[] result = connectorRecordIterator.next();
            assertThat(result.length).isEqualTo(2);
            assertThat(result[0]).isEqualTo(1202L);
            assertThat(connectorRecordIterator.hasNext()).isFalse();
        }
    }

    @Test
    public void testQueryMetricWithDroppedCachedTable()
            throws SQLException
    {
        Optional<CacheInfoPair> cacheInfoPairOptional = getDefaultCacheInfoPair("ForDropTable");
        assertThat(cacheInfoPairOptional).isPresent();
        String tableName = cacheInfoPairOptional.get().getRequiredTableName();
        List<Object[]> origin = queryDuckdb(format("select * from %s", tableName));
        assertThat(origin.size()).isGreaterThan(0);
        pgMetastore.get().directDDL(dropTableStatement.apply(tableName));

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

    @Test
    public void testModelCache()
    {
        Optional<CacheInfoPair> cacheInfoPairOptional = getDefaultCacheInfoPair("Orders");
        assertThat(cacheInfoPairOptional).isPresent();
        String mappingName = cacheInfoPairOptional.get().getRequiredTableName();
        List<Object[]> tables = queryDuckdb("show tables");

        Set<String> tableNames = tables.stream().map(table -> table[0].toString()).collect(toImmutableSet());
        assertThat(tableNames).contains(mappingName);

        List<Object[]> duckdbResult = queryDuckdb(format("select * from \"%s\"", mappingName));
        List<Object[]> bqResult = queryBigQuery("SELECT\n" +
                "     o_orderkey orderkey\n" +
                "   , o_custkey custkey\n" +
                "   , o_orderstatus orderstatus\n" +
                "   , o_totalprice totalprice\n" +
                "   , o_orderdate orderdate" +
                " from `canner-cml`.tpch_tiny.orders");
        assertThat(duckdbResult.size()).isEqualTo(bqResult.size());
        assertThat(Arrays.deepEquals(duckdbResult.toArray(), bqResult.toArray())).isTrue();
    }

    @Test
    public void testRemoveCache()
    {
        WrenMDL mdl = wrenMDL.get();
        Optional<TaskInfo> taskInfoOptional = cacheManager.get()
                .getTaskInfo(catalogSchemaTableName(mdl.getCatalog(), mdl.getSchema(), "RemoveCustomer")).join();
        assertThat(taskInfoOptional).isPresent();

        cacheManager.get().removeCacheIfExist(catalogSchemaTableName(mdl.getCatalog(), mdl.getSchema(), "RemoveCustomer"));
        Optional<TaskInfo> removedOptional = cacheManager.get()
                .getTaskInfo(catalogSchemaTableName(mdl.getCatalog(), mdl.getSchema(), "RemoveCustomer")).join();
        assertThat(removedOptional).isEmpty();
    }
}
