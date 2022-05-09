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
package io.cml;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class PostgresWireProtocolConfig
{
    public static final String CANNERFLOW_PG_WIRE_PROTOCOL_ENABLED = "cannerflow.wire-protocol.enabled";
    public static final String CANNERFLOW_PG_WIRE_PROTOCOL_PORT = "cannerflow.wire-protocol.port";
    public static final String CANNERFLOW_PG_WIRE_PROTOCOL_SSL_ENABLED = "cannerflow.wire-protocol.ssl.enabled";
    public static final String CANNERFLOW_WIRE_PROTOCOL_NETTY_THREAD_COUNT = "cannerflow.wire-protocol.netty.thread.count";

    private String port = "7432";
    private boolean sslEnable;
    private boolean enabled;
    private int nettyThreadCount;

    @NotNull
    public boolean isEnabled()
    {
        return enabled;
    }

    @Config(CANNERFLOW_PG_WIRE_PROTOCOL_ENABLED)
    public PostgresWireProtocolConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    @NotNull
    public String getPort()
    {
        return port;
    }

    @Config(CANNERFLOW_PG_WIRE_PROTOCOL_PORT)
    public PostgresWireProtocolConfig setPort(String port)
    {
        this.port = port;
        return this;
    }

    @NotNull
    public boolean isSslEnable()
    {
        return sslEnable;
    }

    @Config(CANNERFLOW_PG_WIRE_PROTOCOL_SSL_ENABLED)
    public PostgresWireProtocolConfig setSslEnable(boolean sslEnable)
    {
        this.sslEnable = sslEnable;
        return this;
    }

    @NotNull
    public int getNettyThreadCount()
    {
        return nettyThreadCount;
    }

    @Config(CANNERFLOW_WIRE_PROTOCOL_NETTY_THREAD_COUNT)
    public PostgresWireProtocolConfig setNettyThreadCount(int nettyThreadCount)
    {
        this.nettyThreadCount = nettyThreadCount;
        return this;
    }
}
