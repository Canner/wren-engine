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
import io.wren.base.type.PGTypes;
import io.wren.main.wireprotocol.WireProtocolSession;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.wren.main.wireprotocol.Utils.readCString;

public class Parse
        implements Plan
{
    /**
     * Parse Message
     * header:
     * | 'P' | int32 len
     * <p>
     * body:
     * | string statementName | string query | int16 numParamTypes |
     * foreach param:
     * | int32 type_oid (zero = unspecified)
     */
    public static Parse parse(ByteBuf buffer)
    {
        String statementName = readCString(buffer);
        String query = readCString(buffer);
        checkArgument(statementName != null, "statement name can't be null");
        checkArgument(query != null, "query can't be null");
        short numParams = buffer.readShort();
        int[] paramTypes = new int[numParams];
        for (int i = 0; i < numParams; i++) {
            paramTypes[i] = buffer.readInt();
        }
        return new Parse(statementName, query, numParams, paramTypes);
    }

    private static final Logger LOG = Logger.get(Parse.class);

    private final String statementName;
    private final String query;
    private final short parameterCount;
    private final int[] parameterTypes;

    private Parse(String statementName, String query, short parameterCount, int[] parameterTypes)
    {
        this.statementName = statementName;
        this.query = query;
        this.parameterCount = parameterCount;
        this.parameterTypes = parameterTypes;
    }

    @Override
    public Runnable execute(Channel channel, WireProtocolSession session)
    {
        return () -> {
            try {
                List<Integer> paramTypes = new ArrayList<>(parameterCount);
                for (int i = 0; i < parameterCount; i++) {
                    int oid = parameterTypes[i];
                    paramTypes.add(PGTypes.oidToPgType(oid).oid());
                }
                LOG.debug("Create prepared statement %s query: %s", statementName, query);
                session.parse(statementName, query, paramTypes);
                ResponseMessages.sendParseComplete(channel);
            }
            catch (Exception e) {
                LOG.error(e, "Error parsing query: %s", query);
                ResponseMessages.sendErrorResponse(channel, e);
            }
        };
    }
}
