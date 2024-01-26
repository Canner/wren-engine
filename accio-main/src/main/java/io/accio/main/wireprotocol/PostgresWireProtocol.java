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
package io.accio.main.wireprotocol;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.accio.base.AccioException;
import io.accio.base.Column;
import io.accio.base.ConnectorRecordIterator;
import io.accio.base.type.PGType;
import io.accio.base.type.PGTypes;
import io.accio.main.wireprotocol.ssl.SslReqHandler;
import io.airlift.log.Logger;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.accio.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
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

    // set (session) property { = | to } { value | 'value' }
    private static final Pattern SET_STMT_PATTERN = Pattern.compile("(?i)^ *SET( +SESSION)* +(?<property>[a-zA-Z0-9_]+)( *= *| +TO +)(?<value>[^ ']+|'.*')");
    private static final Pattern SET_TRANSACTION_PATTERN = Pattern.compile("SET +(SESSION CHARACTERISTICS AS )? *TRANSACTION");
    private static final Pattern SET_SESSION_AUTHORIZATION = Pattern.compile("SET (SESSION |LOCAL )?SESSION AUTHORIZATION");

    private static final Set<String> IGNORED_COMMAND = Set.of(
            "BEGIN",
            "COMMIT",
            "DISCARD",
            "RESET",
            "CLOSE",
            "UNLISTEN");

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

    @Nullable
    static String readCString(ByteBuf buffer)
    {
        byte[] bytes = new byte[buffer.bytesBefore((byte) 0) + 1];
        if (bytes.length == 0) {
            return null;
        }
        buffer.readBytes(bytes);
        return new String(bytes, 0, bytes.length - 1, StandardCharsets.UTF_8);
    }

    @Nullable
    private static char[] readCharArray(ByteBuf buffer)
    {
        byte[] bytes = new byte[buffer.bytesBefore((byte) 0)];
        if (bytes.length == 0) {
            return null;
        }
        buffer.readBytes(bytes);
        return StandardCharsets.UTF_8.decode(ByteBuffer.wrap(bytes)).array();
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
        Optional<String> password = wireProtocolSession.getPassword();
        if (password.isPresent()) {
            finishAuthentication(channel, password.get());
        }
        else {
            Messages.sendAuthenticationCleartextPassword(channel);
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
                Messages.sendAuthenticationError(channel, "user is empty");
            }
            if (wireProtocolSession.doAuthentication(password)) {
                Messages.sendAuthenticationOK(channel)
                        .addListener(f -> sendParamsAndRdyForQuery(channel));
            }
            else {
                Messages.sendAuthenticationError(channel, format("Database %s not found or permission denied", wireProtocolSession.getDefaultDatabase()));
            }
        }
        catch (Exception e) {
            LOG.error(e);
            Messages.sendAuthenticationError(channel, format("Database %s not found or permission denied", wireProtocolSession.getDefaultDatabase()));
        }
    }

    private void sendParamsAndRdyForQuery(Channel channel)
    {
        for (Map.Entry<String, String> config : DEFAULT_PG_CONFIGS.entrySet()) {
            Messages.sendParameterStatus(channel, config.getKey(), config.getValue());
        }
        Messages.sendReadyForQuery(channel, TransactionState.IDLE);
    }

    private void handleSimpleQuery(ByteBuf buffer, final Channel channel)
    {
        String statement = readCString(buffer);
        LOG.debug("get statement: %s", statement);
        checkArgument(statement != null, "query must not be null");

        List<String> queries = QueryStringSplitter.splitQuery(statement);

        CompletableFuture<?> composedFuture = CompletableFuture.completedFuture(null);
        for (String query : queries) {
            composedFuture = composedFuture.thenCompose(result -> handleSingleQuery(query, channel));
        }
        composedFuture.whenComplete(new ReadyForQueryCallback(channel, TransactionState.IDLE));
    }

    private CompletableFuture<?> handleSingleQuery(String statement, Channel channel)
    {
        if (statement.isEmpty() || ";".equals(statement.trim())) {
            Messages.sendEmptyQueryResponse(channel);
            return CompletableFuture.completedFuture(null);
        }
        if (isIgnoredCommand(statement)) {
            sendHardWiredSessionProperty(statement);
            Messages.sendCommandComplete(channel, statement, 0);
            return CompletableFuture.completedFuture(null);
        }
        try {
            wireProtocolSession.parse("", statement, ImmutableList.of());
            wireProtocolSession.bind("", "", ImmutableList.of(), null);
            Optional<ConnectorRecordIterator> iterator = wireProtocolSession.execute("").join();
            if (iterator.isEmpty()) {
                sendHardWiredSessionProperty(statement);
                Messages.sendCommandComplete(channel, statement, 0);
                return CompletableFuture.completedFuture(null);
            }
            ResultSetSender resultSetSender = new ResultSetSender(
                    statement,
                    channel,
                    iterator.get(),
                    0,
                    0,
                    null);
            Messages.sendRowDescription(channel, wireProtocolSession.describePortal("").get(), null);
            resultSetSender.sendResultSet();
            return wireProtocolSession.sync();
        }
        catch (Exception e) {
            LOG.error(e, format("Query failed. Statement: %s", statement));
            Messages.sendErrorResponse(channel, e);
            CompletableFuture<?> future = CompletableFuture.completedFuture(null);
            future.completeExceptionally(e);
            return future;
        }
    }

    public static boolean isIgnoredCommand(String statement)
    {
        Optional<String> command = Arrays.stream(statement.toUpperCase(ENGLISH).split(" |;"))
                .filter(split -> !split.isEmpty())
                .findFirst();

        if ((command.isPresent() && IGNORED_COMMAND.contains(command.get())) ||
                SET_TRANSACTION_PATTERN.matcher(statement).find() || SET_SESSION_AUTHORIZATION.matcher(statement).find()) {
            return true;
        }

        Matcher matcher = SET_STMT_PATTERN.matcher(statement);
        return matcher.find() && PostgresSessionProperties.isIgnoredSessionProperties(matcher.group("property"));
    }

    public static Optional<Pair<String, String>> parseSetStmt(String statement)
    {
        Matcher matcher = SET_STMT_PATTERN.matcher(statement);
        if (matcher.find()) {
            String property = matcher.group("property");
            String val = matcher.group("value");
            return Optional.of(Pair.of(property, val.matches("'.*'") ? val.substring(1, val.length() - 1) : val));
        }
        return Optional.empty();
    }

    private void sendHardWiredSessionProperty(String statement)
    {
        Optional<Pair<String, String>> property = parseSetStmt(statement);
        if (property.isPresent() && PostgresSessionProperties.isHardWiredSessionProperty(property.get().getKey())) {
            Messages.sendParameterStatus(channel, property.get().getKey(), property.get().getValue());
        }
    }

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
    private void handleParseMessage(ByteBuf buffer, final Channel channel)
    {
        String query = null;
        try {
            String statementName = readCString(buffer);
            query = readCString(buffer);
            checkArgument(statementName != null, "statement name can't be null");
            checkArgument(query != null, "query can't be null");
            short numParams = buffer.readShort();
            List<Integer> paramTypes = new ArrayList<>(numParams);
            for (int i = 0; i < numParams; i++) {
                int oid = buffer.readInt();
                paramTypes.add(PGTypes.oidToPgType(oid).oid());
            }
            wireProtocolSession.parse(statementName, query, paramTypes);
            Messages.sendParseComplete(channel);
        }
        catch (Exception e) {
            LOG.error(e, "Parse query failed. Query: %s", query);
            Messages.sendErrorResponse(channel, e);
        }
    }

    /**
     * Bind Message
     * Header:
     * | 'B' | int32 len
     * <p>
     * Body:
     * <pre>
     * | string portalName | string statementName
     * | int16 numFormatCodes
     *      foreach
     *      | int16 formatCode
     * | int16 numParams
     *      foreach
     *      | int32 valueLength
     *      | byteN value
     * | int16 numResultColumnFormatCodes
     *      foreach
     *      | int16 formatCode
     * </pre>
     */
    private void handleBindMessage(ByteBuf buffer, Channel channel)
    {
        String statementName = null;
        try {
            String portalName = readCString(buffer);
            statementName = readCString(buffer);
            FormatCodes.FormatCode[] formatCodes = FormatCodes.fromBuffer(buffer);
            List<Object> params = readParameters(buffer, statementName, formatCodes);
            FormatCodes.FormatCode[] resultFormatCodes = FormatCodes.fromBuffer(buffer);
            wireProtocolSession.bind(portalName, statementName, params, resultFormatCodes);
            Messages.sendBindComplete(channel);
        }
        catch (Exception e) {
            LOG.error(format("Bind query failed. Statement: %s. Root cause is %s", wireProtocolSession.getOriginalStatement(statementName), e));
            Messages.sendErrorResponse(channel, e);
        }
    }

    private List<Object> readParameters(ByteBuf buffer, String statementName, FormatCodes.FormatCode[] formatCodes)
    {
        short numParams = buffer.readShort();
        List<Object> params = new ArrayList<>(); // use `ArrayList` to handle `null` elements.
        for (int i = 0; i < numParams; i++) {
            int valueLength = buffer.readInt();
            if (valueLength == -1) {
                params.add(null);
            }
            else {
                int paramType = wireProtocolSession.getParamTypeOid(statementName, i);
                PGType<?> pgType = PGTypes.oidToPgType(paramType);
                FormatCodes.FormatCode formatCode = FormatCodes.getFormatCode(formatCodes, i);
                switch (formatCode) {
                    case TEXT:
                        params.add(pgType.readTextValue(buffer, valueLength));
                        break;
                    case BINARY:
                        params.add(pgType.readBinaryValue(buffer, valueLength));
                        break;
                    default:
                        throw new AccioException(GENERIC_INTERNAL_ERROR,
                                format("Unsupported format code '%d' for param '%s'", formatCode.ordinal(), paramType));
                }
            }
        }
        return params;
    }

    /**
     * Execute Message
     * Header:
     * | 'E' | int32 len
     * <p>
     * Body:
     * | string portalName
     * | int32 maxRows (0 = unlimited)
     */
    private void handleExecute(ByteBuf buffer, Channel channel)
    {
        String portalName = readCString(buffer);
        int maxRows = buffer.readInt();
        String statement = "uninitialized statement";

        LOG.info("Execute portal: %s", portalName);
        try {
            Portal portal = wireProtocolSession.getPortal(portalName);

            statement = portal.getPreparedStatement().getOriginalStatement();
            if (statement.isEmpty()) {
                Messages.sendEmptyQueryResponse(channel);
                return;
            }
            if (isIgnoredCommand(statement)) {
                sendHardWiredSessionProperty(statement);
                Messages.sendCommandComplete(channel, statement, 0);
                return;
            }

            if (!portal.isSuspended()) {
                Optional<ConnectorRecordIterator> connectorRecordIterable = wireProtocolSession.execute(portalName).join();
                if (connectorRecordIterable.isEmpty()) {
                    sendHardWiredSessionProperty(statement);
                    Messages.sendCommandComplete(channel, statement, 0);
                    return;
                }
                portal.setResultSetSender(connectorRecordIterable.get());
            }

            ConnectorRecordIterator connectorRecordIterable = portal.getConnectorRecordIterable();
            FormatCodes.FormatCode[] resultFormatCodes = wireProtocolSession.getResultFormatCodes(portalName);
            ResultSetSender resultSetSender = new ResultSetSender(
                    statement,
                    channel,
                    connectorRecordIterable,
                    maxRows,
                    portal.getRowCount(),
                    resultFormatCodes);
            portal.setRowCount(resultSetSender.sendResultSet());
        }
        catch (Exception e) {
            LOG.error(e, format("Execute query failed. Statement: %s. Root cause is %s", statement, e.getMessage()));
            Messages.sendErrorResponse(channel, e);
        }
    }

    private void handleSync(final Channel channel)
    {
        try {
            ReadyForQueryCallback readyForQueryCallback = new ReadyForQueryCallback(channel, TransactionState.IDLE);
            wireProtocolSession.sync().whenComplete(readyForQueryCallback);
        }
        catch (Throwable t) {
            LOG.error(format("Sync failed. Root cause is %s", t.getMessage()));
            Messages.sendErrorResponse(channel, t);
            Messages.sendReadyForQuery(channel, TransactionState.FAILED_TRANSACTION);
        }
    }

    /**
     * Describe Message
     * Header:
     * | 'D' | int32 len
     * <p>
     * Body:
     * | 'S' = prepared statement or 'P' = portal
     * | string nameOfPortalOrStatement
     */
    private void handleDescribeMessage(ByteBuf buffer, Channel channel)
    {
        try {
            byte type = buffer.readByte();
            String portalOrStatement = readCString(buffer);

            // TODO: check parameter's size equal to parameter type's size
            switch (type) {
                case 'P':
                    Optional<List<Column>> columns = wireProtocolSession.describePortal(portalOrStatement);
                    if (columns.isPresent()) {
                        FormatCodes.FormatCode[] formatCodes = wireProtocolSession.getResultFormatCodes(portalOrStatement);
                        Messages.sendRowDescription(channel, columns.get(), formatCodes);
                        break;
                    }
                    Messages.sendNoData(channel);
                    break;
                case 'S':
                    List<Integer> paramTypes = wireProtocolSession.describeStatement(portalOrStatement);
                    Messages.sendParameterDescription(channel, paramTypes);
                    wireProtocolSession.bind("", portalOrStatement, paramTypes.stream().map(ignore -> "null").collect(toImmutableList()), null);
                    Optional<List<Column>> described = wireProtocolSession.describePortal("");
                    if (described.isEmpty()) {
                        Messages.sendNoData(channel);
                    }
                    else {
                        // dry run for getting the row description
                        Messages.sendRowDescription(channel, described.get(),
                                described.get().stream().map(ignore -> FormatCodes.FormatCode.TEXT).collect(toImmutableList()).toArray(new FormatCodes.FormatCode[0]));
                    }
                    break;
                default:
                    throw new AccioException(GENERIC_INTERNAL_ERROR, format("Type %s is invalid. We only support 'P' and 'S'.", type));
            }
        }
        catch (Exception e) {
            LOG.error(format("Describe message failed. Root cause is %s", e.getMessage()));
            Messages.sendErrorResponse(channel, e);
        }
    }

    /**
     * Flush Message
     * | 'H' | int32 len
     * <p>
     * Flush forces the backend to deliver any data pending in it's output buffers.
     */
    private void handleFlush(Channel channel)
    {
        try {
            channel.flush();
        }
        catch (Throwable t) {
            LOG.error(format("Flush failed. Root cause is %s", t.getMessage()));
            Messages.sendErrorResponse(channel, t);
        }
    }

    /**
     * | 'C' | int32 len | byte portalOrStatement | string portalOrStatementName |
     */
    private void handleClose(ByteBuf buffer, Channel channel)
    {
        byte type = buffer.readByte();
        String portalOrStatementName = readCString(buffer);
        LOG.info("Close %s", portalOrStatementName);
        try {
            wireProtocolSession.close(type, portalOrStatementName);
        }
        catch (Exception e) {
            LOG.error(format("Close failed. Root cause is %s", e.getMessage()));
        }
        Messages.sendCloseComplete(channel);
    }

    private static class ReadyForQueryCallback
            implements BiConsumer<Object, Throwable>
    {
        private final Channel channel;
        private final TransactionState transactionState;

        private ReadyForQueryCallback(Channel channel, TransactionState transactionState)
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
                Messages.sendReadyForQuery(channel, transactionState);
            }
        }
    }

    private class MessageHandler
            extends SimpleChannelInboundHandler<ByteBuf>
    {
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
            switch (msgType) {
                case 'Q': // Query (simple)
                    handleSimpleQuery(buffer, channel);
                    return;
                case 'P':
                    handleParseMessage(buffer, channel);
                    return;
                case 'p':
                    handlePassword(buffer, channel);
                    return;
                case 'B':
                    handleBindMessage(buffer, channel);
                    return;
                case 'D':
                    handleDescribeMessage(buffer, channel);
                    return;
                case 'E':
                    handleExecute(buffer, channel);
                    return;
                case 'H':
                    handleFlush(channel);
                    return;
                case 'S':
                    handleSync(channel);
                    return;
                case 'C':
                    handleClose(buffer, channel);
                    return;
                case 'X': // Terminate (called when jdbc connection is closed)
                    channel.close();
                    return;
                default:
                    Messages.sendErrorResponse(
                            channel,
                            new AccioException(GENERIC_INTERNAL_ERROR, "Unsupported messageType: " + msgType));
            }
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
