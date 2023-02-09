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
package io.graphmdl.main;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class PostgresWireProtocolConfig
{
    public static final String PG_WIRE_PROTOCOL_PORT = "pg-wire-protocol.port";
    public static final String PG_WIRE_PROTOCOL_SSL_ENABLED = "pg-wire-protocol.ssl.enabled";
    public static final String PG_WIRE_PROTOCOL_NETTY_THREAD_COUNT = "pg-wire-protocol.netty.thread.count";

    private String port = "7432";
    private boolean sslEnable;
    private int nettyThreadCount;

    @NotNull
    public String getPort()
    {
        return port;
    }

    @Config(PG_WIRE_PROTOCOL_PORT)
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

    @Config(PG_WIRE_PROTOCOL_SSL_ENABLED)
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

    @Config(PG_WIRE_PROTOCOL_NETTY_THREAD_COUNT)
    public PostgresWireProtocolConfig setNettyThreadCount(int nettyThreadCount)
    {
        this.nettyThreadCount = nettyThreadCount;
        return this;
    }
}
