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

package io.accio.testing.postgres;

import com.google.common.collect.ImmutableMap;
import io.accio.testing.TestingAccioServer;
import io.accio.testing.TestingPostgreSqlServer;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static io.accio.base.config.ConfigManager.ConfigEntry.configEntry;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestDeployPostgresRuntime
        extends AbstractWireProtocolTestWithPostgres
{
    TestingPostgreSqlServer testingPostgreSqlServer;

    @Override
    protected TestingAccioServer createAccioServer()
    {
        testingPostgreSqlServer = new TestingPostgreSqlServer();
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder();
        if (getAccioMDLPath().isPresent()) {
            properties.put("accio.file", getAccioMDLPath().get());
        }

        return TestingAccioServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
    }

    @Test
    public void testDeployPostgresRuntime()
    {
        patchConfig(List.of(
                configEntry("postgres.jdbc.url", testingPostgreSqlServer.getJdbcUrl()),
                configEntry("postgres.user", testingPostgreSqlServer.getUser()),
                configEntry("postgres.password", testingPostgreSqlServer.getPassword()),
                configEntry("accio.datasource.type", "POSTGRES")));

        assertThatNoException().isThrownBy(() -> {
            try (Connection connection = createConnection()) {
                Statement statement = connection.createStatement();
                statement.execute("select * from (values ('rows1', 10), ('rows2', 10), ('rows3', 10)) as t(col1, col2)");
                ResultSet resultSet = statement.getResultSet();
                resultSet.next();
            }
        });
    }

    @Test
    public void testFakeConnectionProperties()
    {
        patchConfig(List.of(
                configEntry("postgres.jdbc.url", "fake"),
                configEntry("postgres.user", "fake"),
                configEntry("postgres.password", "fake"),
                configEntry("accio.datasource.type", "POSTGRES")));

        assertThatThrownBy(() -> {
            try (Connection connection = createConnection()) {
                Statement statement = connection.createStatement();
                statement.execute("select * from (values ('rows1', 10), ('rows2', 10), ('rows3', 10)) as t(col1, col2)");
                ResultSet resultSet = statement.getResultSet();
                resultSet.next();
            }
        });
    }
}
