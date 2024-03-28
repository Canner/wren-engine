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

import io.netty.channel.Channel;
import io.wren.main.wireprotocol.ClientInterrupted;
import io.wren.main.wireprotocol.TransactionState;

import java.util.function.BiConsumer;

public class ReadyForQueryCallback
        implements BiConsumer<Object, Throwable>
{
    private final Channel channel;
    private final TransactionState transactionState;

    public ReadyForQueryCallback(Channel channel, TransactionState transactionState)
    {
        this.channel = channel;
        this.transactionState = transactionState;
    }

    @Override
    public void accept(Object result, Throwable t)
    {
        boolean clientInterrupted = t instanceof ClientInterrupted
                || (t != null && t.getCause() instanceof ClientInterrupted);
        if (!clientInterrupted) {
            ResponseMessages.sendReadyForQuery(channel, transactionState);
        }
    }
}
