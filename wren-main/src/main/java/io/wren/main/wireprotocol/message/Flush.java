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

package io.wren.main.wireprotocol.message;

import io.airlift.log.Logger;
import io.netty.channel.Channel;
import io.wren.main.wireprotocol.WireProtocolSession;

import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;

public class Flush
        implements Commit
{
    /**
     * Flush Message
     * | 'H' | int32 len
     * <p>
     * Flush forces the backend to deliver any data pending in it's output buffers.
     */
    public static final Flush FLUSH = new Flush();
    private static final Logger LOG = Logger.get(Flush.class);

    @Override
    public void commit(CompletableFuture<?> planned, Channel channel, WireProtocolSession session)
    {
        try {
            channel.flush();
        }
        catch (Throwable t) {
            LOG.error(format("Flush failed. Caused by %s", t.getMessage()));
            ResponseMessages.sendErrorResponse(channel, t);
        }
    }
}
