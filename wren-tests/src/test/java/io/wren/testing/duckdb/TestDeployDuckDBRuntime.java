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

package io.wren.testing.duckdb;

import com.google.common.collect.ImmutableMap;
import io.wren.testing.TestingPostgreSqlServer;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static io.wren.base.config.ConfigManager.ConfigEntry.configEntry;
import static io.wren.base.config.PostgresConfig.POSTGRES_JDBC_URL;
import static io.wren.base.config.PostgresConfig.POSTGRES_PASSWORD;
import static io.wren.base.config.PostgresConfig.POSTGRES_USER;
import static io.wren.base.config.WrenConfig.DataSourceType.DUCKDB;
import static io.wren.base.config.WrenConfig.DataSourceType.POSTGRES;
import static io.wren.base.config.WrenConfig.WREN_DATASOURCE_TYPE;
import static org.assertj.core.api.Assertions.assertThatNoException;

@Test(singleThreaded = true)
public class TestDeployDuckDBRuntime
        extends AbstractWireProtocolTestWithDuckDB
{
    TestingPostgreSqlServer testingPostgreSqlServer;

    @Override
    protected Map<String, String> properties()
    {
        // Make default data source type not DuckDB
        testingPostgreSqlServer = new TestingPostgreSqlServer();
        return ImmutableMap.<String, String>builder()
                .put(WREN_DATASOURCE_TYPE, POSTGRES.name())
                .put(POSTGRES_JDBC_URL, testingPostgreSqlServer.getJdbcUrl())
                .put(POSTGRES_USER, testingPostgreSqlServer.getUser())
                .put(POSTGRES_PASSWORD, testingPostgreSqlServer.getPassword())
                .build();
    }

    @Test
    public void testDeployDuckDBRuntime()
            throws Exception
    {
        patchConfig(List.of(configEntry(WREN_DATASOURCE_TYPE, DUCKDB.name())));
        initDuckDB(server());
        assertThatNoException().isThrownBy(() -> {
            try (Connection connection = createConnection()) {
                Statement statement = connection.createStatement();
                statement.execute("SELECT count(*) from Orders");
                ResultSet resultSet = statement.getResultSet();
                resultSet.next();
            }
        });
    }
}
