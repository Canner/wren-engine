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
import io.wren.base.Column;
import io.wren.main.wireprotocol.FormatCodes;
import io.wren.main.wireprotocol.WireProtocolSession;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.wren.main.wireprotocol.Utils.readCString;

public abstract class Describe
        implements Plan
{
    /**
     * Describe Message
     * Header:
     * | 'D' | int32 len
     * <p>
     * Body:
     * | 'S' = prepared statement or 'P' = portal
     * | string nameOfPortalOrStatement
     */
    public static Describe describe(ByteBuf buffer)
    {
        byte type = buffer.readByte();
        if (type == 'S') {
            return new DescribeStatement(readCString(buffer));
        }
        if (type == 'P') {
            return new DescribePortal(readCString(buffer));
        }
        throw new IllegalArgumentException("Invalid describe type: " + (char) type);
    }

    static Describe describeStatement(String statementName)
    {
        return new DescribeStatement(statementName);
    }

    static Describe describePortal(String portalName)
    {
        return new DescribePortal(portalName);
    }

    private static final Logger LOG = Logger.get(Describe.class);
    private final String targetName;

    private Describe(String targetName)
    {
        this.targetName = targetName;
    }

    public String getTargetName()
    {
        return targetName;
    }

    public static class DescribeStatement
            extends Describe
    {
        private DescribeStatement(String statementName)
        {
            super(statementName);
        }

        @Override
        public Runnable execute(Channel channel, WireProtocolSession session)
        {
            return () -> {
                try {
                    LOG.info("Describe statement: %s", getTargetName());
                    Optional<List<Integer>> paramTypes = session.describeStatement(getTargetName());
                    if (paramTypes.isEmpty()) {
                        ResponseMessages.sendNoData(channel);
                        return;
                    }
                    ResponseMessages.sendParameterDescription(channel, paramTypes.get());
                    Optional<List<Column>> described = session.dryRunAfterDescribeStatement(
                            getTargetName(), paramTypes.get().stream().map(ignore -> "null").collect(toImmutableList()),
                            null);
                    if (described.isEmpty()) {
                        ResponseMessages.sendNoData(channel);
                    }
                    else {
                        // dry run for getting the row description
                        ResponseMessages.sendRowDescription(channel, described.get(),
                                described.get().stream().map(ignore -> FormatCodes.FormatCode.TEXT).collect(toImmutableList()).toArray(new FormatCodes.FormatCode[0]));
                    }
                }
                catch (Exception e) {
                    LOG.error(e, "Describe statement failed. Caused by %s", e.getMessage());
                    ResponseMessages.sendErrorResponse(channel, e);
                }
            };
        }
    }

    public static class DescribePortal
            extends Describe
    {
        private DescribePortal(String portalName)
        {
            super(portalName);
        }

        @Override
        public Runnable execute(Channel channel, WireProtocolSession session)
        {
            return () -> {
                try {
                    LOG.info("Describe portal: %s", getTargetName());
                    Optional<List<Column>> columns = session.describePortal(getTargetName());
                    if (columns.isPresent()) {
                        FormatCodes.FormatCode[] formatCodes = session.getResultFormatCodes(getTargetName());
                        ResponseMessages.sendRowDescription(channel, columns.get(), formatCodes);
                    }
                    else {
                        ResponseMessages.sendNoData(channel);
                    }
                }
                catch (Exception e) {
                    LOG.error(e, "Describe portal failed. Caused by %s", e.getMessage());
                    ResponseMessages.sendErrorResponse(channel, e);
                }
            };
        }
    }
}
