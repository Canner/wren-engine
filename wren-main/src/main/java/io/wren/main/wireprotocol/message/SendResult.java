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
import io.wren.main.wireprotocol.ResultSetSender;
import io.wren.main.wireprotocol.WireProtocolSession;

import java.util.Optional;

public class SendResult
        implements Plan
{
    private static final Logger LOG = Logger.get(SendResult.class.getName());
    private final DescribePortalAndExecute parent;

    public SendResult(DescribePortalAndExecute parent)
    {
        this.parent = parent;
    }

    @Override
    public Runnable execute(Channel channel, WireProtocolSession session)
    {
        return () -> {
            try {
                Optional<ResultSetSender> sender = parent.getResultSetSender();
                if (sender.isPresent()) {
                    if (sender.get().sendResultSet()) {
                        session.close(Close.CloseType.PORTAL, parent.getPortalName());
                    }
                    else {
                        session.getPortal(parent.getPortalName()).setRowCount(sender.get().getTotalRowCount());
                    }
                }
            }
            catch (Exception e) {
                LOG.error(e, "Error sending result set");
                ResponseMessages.sendErrorResponse(channel, e);
            }
        };
    }
}
