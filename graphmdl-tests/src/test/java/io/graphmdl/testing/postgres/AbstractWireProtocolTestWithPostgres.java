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

package io.graphmdl.testing.postgres;

import com.google.common.collect.ImmutableMap;
import io.graphmdl.testing.AbstractWireProtocolTest;
import io.graphmdl.testing.TestingGraphMDLServer;
import io.graphmdl.testing.TestingPostgreSqlServer;

public class AbstractWireProtocolTestWithPostgres
        extends AbstractWireProtocolTest
{
    private TestingPostgreSqlServer testingPostgreSqlServer;

    @Override
    protected TestingGraphMDLServer createGraphMDLServer()
    {
        testingPostgreSqlServer = new TestingPostgreSqlServer();
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("postgres.jdbc.url", testingPostgreSqlServer.getJdbcUrl())
                .put("postgres.user", testingPostgreSqlServer.getUser())
                .put("postgres.password", testingPostgreSqlServer.getPassword())
                .put("graphmdl.datasource.type", "POSTGRES");

        if (getGraphMDLPath().isPresent()) {
            properties.put("graphmdl.file", getGraphMDLPath().get());
        }

        return TestingGraphMDLServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
    }

    @Override
    protected String getDefaultCatalog()
    {
        return "tpch";
    }

    @Override
    protected String getDefaultSchema()
    {
        return "tpch";
    }
}
