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
import io.wren.base.ConnectorRecordIterator;
import io.wren.main.wireprotocol.FormatCodes;
import io.wren.main.wireprotocol.Portal;
import io.wren.main.wireprotocol.ResultSetSender;
import io.wren.main.wireprotocol.WireProtocolSession;

import java.util.Optional;

import static io.wren.main.wireprotocol.message.MessageUtils.isIgnoredCommand;
import static io.wren.main.wireprotocol.message.MessageUtils.sendHardWiredSessionProperty;

public class DescribePortalAndExecute
        implements Plan
{
    private static final Logger LOG = Logger.get(DescribePortalAndExecute.class.getName());
    private final String portalName;
    private final int maxRows;
    private ResultSetSender resultSetSender;

    public DescribePortalAndExecute(Execute parent)
    {
        this.portalName = parent.getPortalName();
        this.maxRows = parent.getMaxRows();
    }

    public Optional<ResultSetSender> getResultSetSender()
    {
        return Optional.ofNullable(resultSetSender);
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
                    ResponseMessages.sendNoData(channel);
                    ResponseMessages.sendEmptyQueryResponse(channel);
                    return;
                }
                if (isIgnoredCommand(statement)) {
                    ResponseMessages.sendNoData(channel);
                    sendHardWiredSessionProperty(channel, statement);
                    ResponseMessages.sendCommandComplete(channel, statement, 0);
                    return;
                }

                if (!portal.isSuspended()) {
                    Optional<ConnectorRecordIterator> connectorRecordIterable = session.execute(portalName).join();
                    if (connectorRecordIterable.isEmpty()) {
                        ResponseMessages.sendNoData(channel);
                        sendHardWiredSessionProperty(channel, statement);
                        ResponseMessages.sendCommandComplete(channel, statement, 0);
                        return;
                    }
                    portal.setConnectorRecordIterator(connectorRecordIterable.get());
                }

                ConnectorRecordIterator connectorRecordIterable = portal.getConnectorRecordIterator();
                FormatCodes.FormatCode[] resultFormatCodes = session.getResultFormatCodes(portalName);
                resultSetSender = new ResultSetSender(
                        statement,
                        channel,
                        connectorRecordIterable,
                        maxRows,
                        portal.getRowCount(),
                        resultFormatCodes);
                FormatCodes.FormatCode[] formatCodes = session.getResultFormatCodes(portalName);
                ResponseMessages.sendRowDescription(channel, resultSetSender.getColumns(), formatCodes);
            }
            catch (Exception e) {
                LOG.error(e, "Describe portal and execute failed. Caused by %s", e.getMessage());
                ResponseMessages.sendErrorResponse(channel, e);
            }
        };
    }
}
