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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.wren.base.ConnectorRecordIterator;
import io.wren.main.wireprotocol.ResultSetSender;
import io.wren.main.wireprotocol.TransactionState;
import io.wren.main.wireprotocol.WireProtocolSession;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static io.wren.main.wireprotocol.Utils.readCString;
import static io.wren.main.wireprotocol.message.MessageUtils.isIgnoredCommand;
import static io.wren.main.wireprotocol.message.MessageUtils.sendHardWiredSessionProperty;

public class SimpleQuery
        implements Commit
{
    public static SimpleQuery simpleQuery(ByteBuf buffer)
    {
        String statement = readCString(buffer);
        checkArgument(statement != null, "query must not be null");
        return new SimpleQuery(statement);
    }

    private static final Logger LOG = Logger.get(SimpleQuery.class);

    private final String statement;

    private SimpleQuery(String statement)
    {
        this.statement = statement;
    }

    @Override
    public void commit(CompletableFuture<?> planned, Channel channel, WireProtocolSession session)
    {
        planned.thenCompose(v -> handleSimpleQuery(channel, session));
    }

    private CompletableFuture<?> handleSimpleQuery(Channel channel, WireProtocolSession session)
    {
        List<String> queries = QueryStringSplitter.splitQuery(statement);

        CompletableFuture<?> composedFuture = CompletableFuture.completedFuture(null);
        for (String query : queries) {
            composedFuture = composedFuture.thenCompose(result -> handleSingleQuery(query, channel, session));
        }
        return composedFuture.whenComplete(new ReadyForQueryCallback(channel, TransactionState.IDLE));
    }

    private CompletableFuture<?> handleSingleQuery(String statement, Channel channel, WireProtocolSession wireProtocolSession)
    {
        if (statement.isEmpty() || ";".equals(statement.trim())) {
            ResponseMessages.sendEmptyQueryResponse(channel);
            return CompletableFuture.completedFuture(null);
        }
        if (isIgnoredCommand(statement)) {
            sendHardWiredSessionProperty(channel, statement);
            ResponseMessages.sendCommandComplete(channel, statement, 0);
            return CompletableFuture.completedFuture(null);
        }
        try {
            wireProtocolSession.parse("", statement, ImmutableList.of());
            wireProtocolSession.bind("", "", ImmutableList.of(), null);
            Optional<ConnectorRecordIterator> iterator = wireProtocolSession.execute("").join();
            if (iterator.isEmpty()) {
                sendHardWiredSessionProperty(channel, statement);
                ResponseMessages.sendCommandComplete(channel, statement, 0);
                return CompletableFuture.completedFuture(null);
            }
            ResultSetSender resultSetSender = new ResultSetSender(
                    statement,
                    channel,
                    iterator.get(),
                    0,
                    0,
                    null);
            ResponseMessages.sendRowDescription(channel, wireProtocolSession.describePortal("").get(), null);
            resultSetSender.sendResultSet();
            return wireProtocolSession.sync();
        }
        catch (Exception e) {
            LOG.error(e, "Query failed. Statement: %s", statement);
            ResponseMessages.sendErrorResponse(channel, e);
            CompletableFuture<?> future = CompletableFuture.completedFuture(null);
            future.completeExceptionally(e);
            return future;
        }
    }
}
