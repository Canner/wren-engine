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
import io.wren.base.ConnectorRecordIterator;
import io.wren.main.wireprotocol.FormatCodes;
import io.wren.main.wireprotocol.Portal;
import io.wren.main.wireprotocol.ResultSetSender;
import io.wren.main.wireprotocol.WireProtocolSession;

import java.util.Optional;

import static io.wren.main.wireprotocol.Utils.readCString;
import static io.wren.main.wireprotocol.message.MessageUtils.isIgnoredCommand;
import static io.wren.main.wireprotocol.message.MessageUtils.sendHardWiredSessionProperty;

public class Execute
        implements Plan
{
    /**
     * Execute Message
     * Header:
     * | 'E' | int32 len
     * <p>
     * Body:
     * | string portalName
     * | int32 maxRows (0 = unlimited)
     */
    public static Execute execute(ByteBuf buffer)
    {
        String portalName = readCString(buffer);
        int maxRows = buffer.readInt();
        return new Execute(portalName, maxRows);
    }

    static Execute execute(String portalName, int maxRows)
    {
        return new Execute(portalName, maxRows);
    }

    private static final Logger LOG = Logger.get(Execute.class);
    private final String portalName;
    private final int maxRows;

    private Execute(String portalName, int maxRows)
    {
        this.portalName = portalName;
        this.maxRows = maxRows;
    }

    public int getMaxRows()
    {
        return maxRows;
    }

    public String getPortalName()
    {
        return portalName;
    }

    @Override
    public Runnable execute(Channel channel, WireProtocolSession session)
    {
        return () -> {
            try {
                Portal portal = session.getPortal(portalName);
                String statement = portal.getPreparedStatement().getOriginalStatement();
                if (statement.isEmpty()) {
                    ResponseMessages.sendEmptyQueryResponse(channel);
                    return;
                }
                if (isIgnoredCommand(statement)) {
                    sendHardWiredSessionProperty(channel, statement);
                    ResponseMessages.sendCommandComplete(channel, statement, 0);
                    return;
                }

                if (!portal.isSuspended()) {
                    Optional<ConnectorRecordIterator> connectorRecordIterable = session.execute(portalName).join();
                    if (connectorRecordIterable.isEmpty()) {
                        sendHardWiredSessionProperty(channel, statement);
                        ResponseMessages.sendCommandComplete(channel, statement, 0);
                        return;
                    }
                    portal.setConnectorRecordIterator(connectorRecordIterable.get());
                }

                ConnectorRecordIterator connectorRecordIterable = portal.getConnectorRecordIterator();
                FormatCodes.FormatCode[] resultFormatCodes = session.getResultFormatCodes(portalName);
                ResultSetSender resultSetSender = new ResultSetSender(
                        statement,
                        channel,
                        connectorRecordIterable,
                        maxRows,
                        portal.getRowCount(),
                        resultFormatCodes);
                if (resultSetSender.sendResultSet()) {
                    session.close(Close.CloseType.PORTAL, portalName);
                }
                else {
                    portal.setRowCount(resultSetSender.getTotalRowCount());
                }
            }
            catch (Exception e) {
                LOG.error(e, "Error executing query: %s", portalName);
                ResponseMessages.sendErrorResponse(channel, e);
            }
        };
    }
}
