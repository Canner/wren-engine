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

package io.wren.main.wireprotocol;

import io.netty.channel.Channel;
import io.wren.base.Column;
import io.wren.base.ConnectorRecordIterator;
import io.wren.base.WrenException;
import io.wren.base.type.PGType;
import io.wren.main.wireprotocol.message.ResponseMessages;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.wren.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class ResultSetSender
        extends BaseResultSender
{
    private final String query;
    private final Channel channel;
    private final ConnectorRecordIterator connectorRecordIterator;
    private final List<PGType> schema;
    private final List<Column> columns;
    private final int maxRows;

    @Nullable
    private final FormatCodes.FormatCode[] formatCodes;

    private long localRowCount;
    private long totalRowCount;

    public ResultSetSender(String query,
            Channel channel,
            ConnectorRecordIterator connectorRecordIterator,
            int maxRows,
            long previousCount,
            @Nullable FormatCodes.FormatCode[] formatCodes)
    {
        this.query = query;
        this.channel = channel;
        this.connectorRecordIterator = connectorRecordIterator;
        this.schema = connectorRecordIterator.getColumns().stream().map(Column::getType).collect(toImmutableList());
        this.columns = connectorRecordIterator.getColumns();
        this.maxRows = maxRows;
        this.totalRowCount = previousCount;
        this.formatCodes = formatCodes;
    }

    public List<Column> getColumns()
    {
        return columns;
    }

    @Override
    public void sendRow(Object[] row)
    {
        localRowCount++;
        ResponseMessages.sendDataRow(channel, row, schema, formatCodes);
        if (localRowCount % 1000 == 0) {
            channel.flush();
        }
    }

    @Override
    public void batchFinished()
    {
        ResponseMessages.sendPortalSuspended(channel);
    }

    @Override
    public void allFinished(boolean interrupted)
    {
        if (interrupted) {
            super.allFinished(true);
        }
        else {
            ResponseMessages.sendCommandComplete(channel, query, totalRowCount);
        }
    }

    @Override
    public void fail(@Nonnull Throwable throwable)
    {
        ResponseMessages.sendErrorResponse(channel, throwable).addListener(f -> super.fail(throwable));
    }

    public long getTotalRowCount()
    {
        return totalRowCount;
    }

    /**
     * Send the result set to the client.
     *
     * @return If all finished, return true, otherwise return false.
     */
    public boolean sendResultSet()
    {
        while (connectorRecordIterator.hasNext()) {
            sendRow(connectorRecordIterator.next());
            if (maxRows > 0 && connectorRecordIterator.hasNext() && localRowCount % maxRows == 0) {
                batchFinished();
                totalRowCount += localRowCount;
                return false;
            }
        }
        totalRowCount += localRowCount;
        try {
            connectorRecordIterator.close();
            allFinished(false);
            return true;
        }
        catch (Exception e) {
            throw new WrenException(GENERIC_INTERNAL_ERROR, e);
        }
    }
}
