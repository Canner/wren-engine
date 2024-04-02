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

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.wren.base.WrenException;
import io.wren.main.wireprotocol.message.Plan;
import io.wren.main.wireprotocol.message.ResponseMessages;
import io.wren.main.wireprotocol.ssl.SslReqHandler;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static io.wren.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.wren.main.wireprotocol.Utils.readCString;
import static io.wren.main.wireprotocol.Utils.readCharArray;
import static io.wren.main.wireprotocol.message.Bind.bind;
import static io.wren.main.wireprotocol.message.Close.close;
import static io.wren.main.wireprotocol.message.Describe.describe;
import static io.wren.main.wireprotocol.message.Execute.execute;
import static io.wren.main.wireprotocol.message.Flush.FLUSH;
import static io.wren.main.wireprotocol.message.Parse.parse;
import static io.wren.main.wireprotocol.message.SimpleQuery.simpleQuery;
import static io.wren.main.wireprotocol.message.Sync.SYNC;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PostgresWireProtocol
{
    public static final Map<String, String> DEFAULT_PG_CONFIGS = ImmutableMap.<String, String>builder()
            .put(PostgresSessionProperties.SERVER_VERSION, "13.0")
            .put(PostgresSessionProperties.SERVER_ENCODING, "UTF8")
            .put(PostgresSessionProperties.CLIENT_ENCODING, "UTF8")
            .put(PostgresSessionProperties.DATE_STYLE, "ISO")
            .put(PostgresSessionProperties.TIMEZONE, "UTC")
            // Reports whether we with support for 64-bit-integer dates and times.
            .put(PostgresSessionProperties.INTEGER_DATETIMES, "on")
            .build();

    private static final Logger LOG = Logger.get(PostgresWireProtocol.class);

    final MessageDecoder decoder;
    final MessageHandler handler;
    private Channel channel;
    private int msgLength;
    private byte msgType;
    private final SslReqHandler sslReqHandler;

    private final WireProtocolSession wireProtocolSession;

    enum State
    {
        PRE_STARTUP,
        STARTUP_HEADER,
        STARTUP_BODY,
        MSG_HEADER,
        MSG_BODY
    }

    private State state = State.PRE_STARTUP;

    public PostgresWireProtocol(WireProtocolSession wireProtocolSession, SslReqHandler sslReqHandler)
    {
        this.wireProtocolSession = requireNonNull(wireProtocolSession, "wireProtocolSession is null");
        this.sslReqHandler = sslReqHandler;
        this.decoder = new MessageDecoder();
        this.handler = new MessageHandler();
    }

    private static void traceLogProtocol(int protocol)
    {
        if (LOG.isDebugEnabled()) {
            int major = protocol >> 16;
            int minor = protocol & 0x0000FFFF;
            LOG.debug("protocol %s.%s", major, minor);
        }
    }

    private Properties readStartupMessage(ByteBuf buffer)
    {
        Properties properties = new Properties();
        ByteBuf byteBuf = buffer.readSlice(msgLength);
        while (true) {
            String key = readCString(byteBuf);
            if (key == null) {
                break;
            }
            String value = readCString(byteBuf);
            LOG.info("payload: key=%s value=%s", key, value);
            if (!"".equals(key) && !"".equals(value)) {
                properties.setProperty(key, value);
            }
        }
        return properties;
    }

    /**
     * Authentication Flow:
     * <p>
     * |                                  |
     * |      StartupMessage              |
     * |--------------------------------->|
     * |                                  |
     * |      AuthenticationTextPassword  |
     * |      or                          |
     * |      AuthenticationOK            |
     * |      or                          |
     * |      ErrorResponse               |
     * |<---------------------------------|
     * |                                  |
     * |       ParameterStatus            |
     * |<---------------------------------|
     * |                                  |
     * |       ReadyForQuery              |
     * |<---------------------------------|
     */

    private void handleStartupBody(ByteBuf buffer, Channel channel)
    {
        wireProtocolSession.setProperties(readStartupMessage(buffer));
        initAuthentication(channel);
    }

    private void initAuthentication(Channel channel)
    {
        if (wireProtocolSession.isAuthenticationEnabled()) {
            Optional<String> password = wireProtocolSession.getPassword();
            if (password.isPresent()) {
                finishAuthentication(channel, password.get());
            }
            else {
                ResponseMessages.sendAuthenticationCleartextPassword(channel);
            }
        }
        else {
            ResponseMessages.sendAuthenticationOK(channel)
                    .addListener(f -> sendParamsAndRdyForQuery(channel));
        }
    }

    private void handlePassword(ByteBuf buffer, final Channel channel)
    {
        String personalAccessToken = String.valueOf(readCharArray(buffer));
        finishAuthentication(channel, personalAccessToken);
    }

    private void finishAuthentication(Channel channel, String password)
    {
        try {
            Optional<String> clientUser = wireProtocolSession.getClientUser();
            if (clientUser.isEmpty() || clientUser.get().isEmpty()) {
                ResponseMessages.sendAuthenticationError(channel, "user is empty");
            }
            if (wireProtocolSession.doAuthentication(password)) {
                ResponseMessages.sendAuthenticationOK(channel)
                        .addListener(f -> sendParamsAndRdyForQuery(channel));
            }
            else {
                ResponseMessages.sendAuthenticationError(channel, format("Database %s not found or permission denied", wireProtocolSession.getDefaultDatabase()));
            }
        }
        catch (Exception e) {
            LOG.error(e);
            ResponseMessages.sendAuthenticationError(channel, format("Database %s not found or permission denied", wireProtocolSession.getDefaultDatabase()));
        }
    }

    private void sendParamsAndRdyForQuery(Channel channel)
    {
        for (Map.Entry<String, String> config : DEFAULT_PG_CONFIGS.entrySet()) {
            ResponseMessages.sendParameterStatus(channel, config.getKey(), config.getValue());
        }
        ResponseMessages.sendReadyForQuery(channel, TransactionState.IDLE);
    }

    private class MessageHandler
            extends SimpleChannelInboundHandler<ByteBuf>
    {
        private final Queue<Plan> messageQueue = new ArrayDeque<>();

        @Override
        public void channelRegistered(ChannelHandlerContext ctx)
        {
            LOG.debug("channel registered.");
            channel = ctx.channel();
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, ByteBuf buffer)
        {
            LOG.debug("channel read.");
            checkArgument(channel != null, "Channel must be initialized");
            dispatchState(buffer, channel);
        }

        private void dispatchState(ByteBuf buffer, Channel channel)
        {
            LOG.debug("channel dispatch state: %s", state);
            switch (state) {
                case STARTUP_HEADER:
                case MSG_HEADER:
                    throw new IllegalStateException("Decoder should've processed the headers");
                case STARTUP_BODY:
                    state = PostgresWireProtocol.State.MSG_HEADER;
                    handleStartupBody(buffer, channel);
                    return;
                case MSG_BODY:
                    state = PostgresWireProtocol.State.MSG_HEADER;
                    LOG.debug("msg=%s msgLength=%s readableBytes=%s", ((char) msgType), msgLength, buffer.readableBytes());
                    dispatchMessage(buffer, channel);
                    return;
                default:
                    throw new IllegalStateException("Illegal state: " + state);
            }
        }

        /**
         * Simple Query Mode:
         * handleSimpleQuery()
         *
         * @see <a href="https://www.postgresql.org/docs/9.3/protocol-flow.html#AEN99807">SIMPLE-QUERY</a>
         * <p>
         * Extended Query Mode:
         * handleParseMessage() -> handleBindMessage() -> handleExecute() -> handleSync()
         * @see <a href="https://www.postgresql.org/docs/9.3/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY">PROTOCOL-FLOW-EXT-QUERY</a>
         */
        private void dispatchMessage(ByteBuf buffer, Channel channel)
        {
            LOG.debug("channel dispatch message. msgType: %s", msgType);
            try {
                switch (msgType) {
                    case 'Q': // Query (simple)
                        simpleQuery(buffer).commit(commitPlans(), channel, wireProtocolSession);
                        return;
                    case 'P':
                        messageQueue.add(parse(buffer));
                        return;
                    case 'p':
                        handlePassword(buffer, channel);
                        return;
                    case 'B':
                        messageQueue.add(bind(buffer));
                        return;
                    case 'D':
                        messageQueue.add(describe(buffer));
                        return;
                    case 'E':
                        messageQueue.add(execute(buffer));
                        return;
                    case 'H':
                        FLUSH.commit(commitPlans(), channel, wireProtocolSession);
                        return;
                    case 'S':
                        SYNC.commit(commitPlans(), channel, wireProtocolSession);
                        return;
                    case 'C':
                        close(buffer).commit(commitPlans(), channel, wireProtocolSession);
                        return;
                    case 'X': // Terminate (called when jdbc connection is closed)
                        channel.close();
                        return;
                    default:
                        ResponseMessages.sendErrorResponse(
                                channel,
                                new WrenException(GENERIC_INTERNAL_ERROR, "Unsupported messageType: " + msgType));
                }
            }
            catch (Exception e) {
                LOG.error(e, "Error processing message: %s", msgType);
                ResponseMessages.sendErrorResponse(channel, e);
            }
        }

        /**
         * Consume the messageQueue and plan all existed messages.
         *
         * @return planned future
         */
        private CompletableFuture<?> commitPlans()
        {
            CompletableFuture<?> start = CompletableFuture.completedFuture(null);
            MessagePlanner.plan(messageQueue)
                    .stream().map(plan -> plan.execute(channel, wireProtocolSession))
                    .forEach(start::thenRun);
            return start;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        {
            LOG.error(cause, "Uncaught exception: %s", cause);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx)
                throws Exception
        {
            LOG.debug("channelDisconnected");
            channel = null;
            super.channelUnregistered(ctx);
        }
    }

    /**
     * FrameDecoder that makes sure that a full message is in the buffer before delegating work to the MessageHandler
     */
    private class MessageDecoder
            extends ByteToMessageDecoder
    {
        {
            setCumulator(COMPOSITE_CUMULATOR);
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
        {
            LOG.debug("decoded!");
            ByteBuf decode = decode(in, ctx.pipeline());
            if (decode != null) {
                out.add(decode);
            }
        }

        private ByteBuf decode(ByteBuf buffer, ChannelPipeline pipeline)
        {
            switch (state) {
                case PRE_STARTUP:
                    if (sslReqHandler.process(buffer, pipeline) == SslReqHandler.State.DONE) {
                        state = State.STARTUP_HEADER;
                        // We need to call decode again in case there are additional bytes in the buffer
                        return decode(buffer, pipeline);
                    }
                    return null;
                /*
                 * StartupMessage:
                 * | int32 length | int32 protocol | [ string paramKey | string paramValue , ... ]
                 */
                case STARTUP_HEADER:
                    if (buffer.readableBytes() < 8) {
                        return null;
                    }
                    msgLength = buffer.readInt() - 8; // exclude length itself and protocol
                    LOG.warn("Header pkgLength: %s", msgLength);
                    int protocol = buffer.readInt();
                    traceLogProtocol(protocol);
                    return nullOrBuffer(buffer, State.STARTUP_BODY);
                /*
                 * Regular Data Packet:
                 * | char tag | int32 len | payload
                 */
                case MSG_HEADER:
                    if (buffer.readableBytes() < 5) {
                        return null;
                    }
                    buffer.markReaderIndex();
                    msgType = buffer.readByte();
                    msgLength = buffer.readInt() - 4; // exclude length itself
                    return nullOrBuffer(buffer, State.MSG_BODY);
                case MSG_BODY:
                case STARTUP_BODY:
                    return nullOrBuffer(buffer, state);
                default:
                    throw new IllegalStateException("Invalid state " + state);
            }
        }

        /**
         * return null if there aren't enough bytes to read the whole message. Otherwise returns the buffer.
         * <p>
         * If null is returned the decoder will be called again, otherwise the MessageHandler will be called next.
         */
        private ByteBuf nullOrBuffer(ByteBuf buffer, State nextState)
        {
            if (buffer.readableBytes() < msgLength) {
                buffer.resetReaderIndex();
                return null;
            }
            state = nextState;
            return buffer.readBytes(msgLength);
        }
    }
}
