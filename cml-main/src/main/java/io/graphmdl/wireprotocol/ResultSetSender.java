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

package io.graphmdl.wireprotocol;

import io.graphmdl.spi.ConnectorRecordIterator;
import io.graphmdl.spi.type.PGType;
import io.netty.channel.Channel;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.List;

class ResultSetSender
        extends BaseResultSender
{
    private final String query;
    private final Channel channel;
    private final Iterator<Object[]> connectorRecordIterator;
    private final List<PGType> schema;
    private final int maxRows;

    @Nullable
    private final FormatCodes.FormatCode[] formatCodes;

    private long localRowCount;
    private long totalRowCount;

    ResultSetSender(String query,
            Channel channel,
            ConnectorRecordIterator connectorRecordIterator,
            int maxRows,
            long previousCount,
            @Nullable FormatCodes.FormatCode[] formatCodes)
    {
        this.query = query;
        this.channel = channel;
        this.connectorRecordIterator = connectorRecordIterator;
        this.schema = connectorRecordIterator.getTypes();
        this.maxRows = maxRows;
        this.totalRowCount = previousCount;
        this.formatCodes = formatCodes;
    }

    @Override
    public void sendRow(Object[] row)
    {
        localRowCount++;
        Messages.sendDataRow(channel, row, schema, formatCodes);
        if (localRowCount % 1000 == 0) {
            channel.flush();
        }
    }

    @Override
    public void batchFinished()
    {
        Messages.sendPortalSuspended(channel);
    }

    @Override
    public void allFinished(boolean interrupted)
    {
        if (interrupted) {
            super.allFinished(true);
        }
        else {
            Messages.sendCommandComplete(channel, query, totalRowCount);
        }
    }

    @Override
    public void fail(@Nonnull Throwable throwable)
    {
        Messages.sendErrorResponse(channel, throwable).addListener(f -> super.fail(throwable));
    }

    public long sendResultSet()
    {
        while (connectorRecordIterator.hasNext()) {
            sendRow(connectorRecordIterator.next());
            if (maxRows > 0 && connectorRecordIterator.hasNext() && localRowCount % maxRows == 0) {
                batchFinished();
                totalRowCount += localRowCount;
                return totalRowCount;
            }
        }
        totalRowCount += localRowCount;
        allFinished(false);
        return totalRowCount;
    }
}
