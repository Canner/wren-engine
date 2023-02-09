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

package io.graphmdl.main.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportSettings;

/**
 * Factory utility for creating channel server bootstraps, based on the relevant netty {@link Settings}
 */
public final class ChannelBootstrapFactory
{
    private ChannelBootstrapFactory() {}

    public static ServerBootstrap newChannelBootstrap(Settings settings, EventLoopGroup eventLoopGroup)
    {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.channel(NioServerSocketChannel.class);
        Boolean reuseAddress = TransportSettings.TCP_REUSE_ADDRESS.get(settings);
        return serverBootstrap
                .group(eventLoopGroup)
                .option(ChannelOption.SO_REUSEADDR, reuseAddress)
                .childOption(ChannelOption.SO_REUSEADDR, reuseAddress)
                .childOption(ChannelOption.TCP_NODELAY, TransportSettings.TCP_NO_DELAY.get(settings))
                .childOption(ChannelOption.SO_KEEPALIVE, TransportSettings.TCP_KEEP_ALIVE.get(settings));
    }
}
