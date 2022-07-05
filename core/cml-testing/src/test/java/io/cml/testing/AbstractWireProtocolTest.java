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

import com.google.common.net.HostAndPort;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static java.lang.String.format;

public abstract class AbstractWireProtocolTest
        extends RequireWireProtocolServer
{
    public static final String MOCK_PASSWORD = "ignored";

    protected TestingWireProtocolClient wireProtocolClient()
            throws IOException
    {
        HostAndPort hostAndPort = wireProtocolServer().getHostAndPort();
        return new TestingWireProtocolClient(
                new InetSocketAddress(hostAndPort.getHost(), hostAndPort.getPort()));
    }

    protected Connection createConnection()
            throws SQLException
    {
        HostAndPort hostAndPort = wireProtocolServer().getHostAndPort();
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
