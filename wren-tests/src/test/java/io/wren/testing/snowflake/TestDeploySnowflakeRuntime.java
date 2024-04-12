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

package io.wren.testing.snowflake;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static io.wren.base.config.ConfigManager.ConfigEntry.configEntry;
import static io.wren.base.config.SnowflakeConfig.SNOWFLAKE_JDBC_URL;
import static io.wren.base.config.SnowflakeConfig.SNOWFLAKE_PASSWORD;
import static io.wren.base.config.SnowflakeConfig.SNOWFLAKE_USER;
import static io.wren.base.config.WrenConfig.DataSourceType.DUCKDB;
import static io.wren.base.config.WrenConfig.DataSourceType.SNOWFLAKE;
import static io.wren.base.config.WrenConfig.WREN_DATASOURCE_TYPE;
import static java.lang.System.getenv;
import static org.assertj.core.api.Assertions.assertThatNoException;

@Test(singleThreaded = true)
public class TestDeploySnowflakeRuntime
        extends AbstractWireProtocolTestWithSnowflake
{
    @BeforeClass
    public void init()
            throws Exception
    {
        testingSQLGlotServer = closer.register(prepareSQLGlot());
        wrenServer = closer.register(createWrenServer());
        client = closer.register(createHttpClient());
    }

    @AfterClass
    public void close()
            throws IOException
    {
        closer.close();
    }

    @Override
    protected Map<String, String> properties()
    {
        // Make default data source type not Snowflake
        return ImmutableMap.<String, String>builder()
                .put(WREN_DATASOURCE_TYPE, DUCKDB.name())
                .build();
    }

    @Test
    public void testDeploySnowflakeRuntime()
    {
        patchConfig(List.of(
                configEntry(WREN_DATASOURCE_TYPE, SNOWFLAKE.name()),
                configEntry(SNOWFLAKE_JDBC_URL, getenv("SNOWFLAKE_JDBC_URL")),
                configEntry(SNOWFLAKE_USER, getenv("SNOWFLAKE_USER")),
                configEntry(SNOWFLAKE_PASSWORD, getenv("SNOWFLAKE_PASSWORD"))));

        assertThatNoException().isThrownBy(() -> {
            try (Connection connection = createConnection()) {
                Statement statement = connection.createStatement();
                statement.execute("SELECT count(*) from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS");
                ResultSet resultSet = statement.getResultSet();
                resultSet.next();
            }
        });
    }
}
