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
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.wren.main.wireprotocol.WireProtocolSession;

import java.util.concurrent.CompletableFuture;

import static io.wren.main.wireprotocol.Utils.readCString;
import static java.lang.String.format;

public class Close
        implements Commit
{
    public enum CloseType
    {
        PORTAL((byte) 'P'),
        STATEMENT((byte) 'S');

        private final byte value;

        CloseType(byte value)
        {
            this.value = value;
        }

        public byte getValue()
        {
            return value;
        }

        public static CloseType fromValue(byte value)
        {
            for (CloseType type : CloseType.values()) {
                if (type.getValue() == value) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Invalid close type: " + (char) value);
        }
    }

    /**
     * | 'C' | int32 len | byte portalOrStatement | string portalOrStatementName |
     */
    public static Close close(ByteBuf buffer)
    {
        CloseType type = CloseType.fromValue(buffer.readByte());
        String portalOrStatementName = readCString(buffer);
        return new Close(type, portalOrStatementName);
    }

    private static final Logger LOG = Logger.get(Close.class);

    private final CloseType type;

    private final String portalOrStatementName;

    private Close(CloseType type, String portalOrStatementName)
    {
        this.type = type;
        this.portalOrStatementName = portalOrStatementName;
    }

    @Override
    public void commit(CompletableFuture<?> planned, Channel channel, WireProtocolSession session)
    {
        LOG.info("Close type: %s, name: %s", type, portalOrStatementName);
        try {
            session.close(type, portalOrStatementName);
        }
        catch (Exception e) {
            LOG.error(format("Close failed. Caused by %s", e.getMessage()));
        }
        ResponseMessages.sendCloseComplete(channel);
    }
}
