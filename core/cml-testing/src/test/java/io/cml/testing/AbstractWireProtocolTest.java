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

package io.cml.testing;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static io.cml.metrics.MetricConfig.METRIC_ROOT_PATH;
import static java.lang.String.format;
import static java.lang.System.getenv;

public abstract class AbstractWireProtocolTest
        extends RequireCmlServer
{
    public static final String MOCK_PASSWORD = "ignored";

    @Override
    protected TestingCmlServer createCmlServer()
    {
        return TestingCmlServer.builder()
                .setRequiredConfigs(
                        ImmutableMap.<String, String>builder()
                                .put("bigquery.project-id", getenv("TEST_BIG_QUERY_PROJECT_ID"))
                                .put("bigquery.location", "asia-east1")
                                .put("bigquery.credentials-key", getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"))
                                .put(METRIC_ROOT_PATH, "ignored")
                                .build())
                .build();
    }

    protected TestingWireProtocolClient wireProtocolClient()
            throws IOException
    {
        HostAndPort hostAndPort = server().getPgHostAndPort();
        return new TestingWireProtocolClient(
                new InetSocketAddress(hostAndPort.getHost(), hostAndPort.getPort()));
    }

    protected Connection createConnection()
            throws SQLException
    {
        HostAndPort hostAndPort = server().getPgHostAndPort();
        String url = format("jdbc:postgresql://%s:%s/%s", hostAndPort.getHost(), hostAndPort.getPort(), "test");
        Properties props = getDefaultProperties();
        return DriverManager.getConnection(url, props);
    }

    protected Properties getDefaultProperties()
    {
        Properties props = new Properties();
        props.setProperty("password", MOCK_PASSWORD);
        props.setProperty("user", "canner");
        props.setProperty("ssl", "false");
        return props;
    }
}
