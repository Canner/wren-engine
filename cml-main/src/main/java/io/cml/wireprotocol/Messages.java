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

package io.cml.wireprotocol;

import io.airlift.log.Logger;
import io.cml.spi.Column;
import io.cml.spi.type.PGType;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import javax.annotation.Nullable;

import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;

/**
 * Regular data packet is in the following format:
 * <p>
 * +----------+-----------+----------+
 * | char tag | int32 len | payload  |
 * +----------+-----------+----------+
 * <p>
 * The tag indicates the message type, the second field is the length of the packet
 * (excluding the tag, but including the length itself)
 * <p>
 * <p>
 * See https://www.postgresql.org/docs/9.2/static/protocol-message-formats.html
 */
public class Messages
{
    private static final Logger LOGGER = Logger.get(Messages.class);

    private static final byte[] METHOD_NAME_CLIENT_AUTH = "ClientAuthentication".getBytes(UTF_8);

    private Messages() {}

    public static ChannelFuture sendAuthenticationOK(Channel channel)
    {
        ByteBuf buffer = channel.alloc().buffer(9);
        buffer.writeByte('R');
        buffer.writeInt(8); // size excluding char
        buffer.writeInt(0);
        ChannelFuture channelFuture = channel.writeAndFlush(buffer);
        if (LOGGER.isDebugEnabled()) {
            channelFuture.addListener((ChannelFutureListener) future -> LOGGER.debug("sentAuthenticationOK"));
        }
        return channelFuture;
    }

    /**
     * | 'C' | int32 len | str commandTag
     *
     * @param query :the query
     * @param rowCount : number of rows in the result set or number of rows affected by the DML statement
     */
    static ChannelFuture sendCommandComplete(Channel channel, String query, long rowCount)
    {
        query = query.trim().split(" |;|\n\t|\n", 2)[0].toUpperCase(ENGLISH);
        String commandTag = buildCommandTag(query, rowCount);

        byte[] commandTagBytes = commandTag.getBytes(UTF_8);
        int length = 4 + commandTagBytes.length + 1;
        ByteBuf buffer = channel.alloc().buffer(length + 1);
        buffer.writeByte('C');
        buffer.writeInt(length);
        writeCString(buffer, commandTagBytes);
        ChannelFuture channelFuture = channel.writeAndFlush(buffer);
        if (LOGGER.isDebugEnabled()) {
            channelFuture.addListener((ChannelFutureListener) future -> LOGGER.debug("sentCommandComplete"));
        }
        return channelFuture;
    }

    /**
     * ReadyForQuery (B)
     * <p>
     * Byte1('Z')
     * Identifies the message type. ReadyForQuery is sent whenever the
     * backend is ready for a new query cycle.
     * <p>
     * Int32(5)
     * Length of message contents in bytes, including self.
     * <p>
     * Byte1
     * Current backend transaction status indicator. Possible values are
     * 'I' if idle (not in a transaction block); 'T' if in a transaction
     * block; or 'E' if in a failed transaction block (queries will be
     * rejected until block is ended).
     */
    static void sendReadyForQuery(Channel channel, TransactionState transactionState)
    {
        ByteBuf buffer = channel.alloc().buffer(6);
        buffer.writeByte('Z');
        buffer.writeInt(5);
        buffer.writeByte(transactionState.code());
        ChannelFuture channelFuture = channel.writeAndFlush(buffer);
        if (LOGGER.isDebugEnabled()) {
            channelFuture.addListener((ChannelFutureListener) future -> LOGGER.debug("sentReadyForQuery"));
        }
    }

    /**
     * | 'S' | int32 len | str name | str value
     * <p>
     * See https://www.postgresql.org/docs/9.2/static/protocol-flow.html#PROTOCOL-ASYNC
     * <p>
     * > At present there is a hard-wired set of parameters for which ParameterStatus will be generated: they are
     * <p>
     * - server_version,
     * - server_encoding,
     * - client_encoding,
     * - application_name,
     * - is_superuser,
     * - session_authorization,
     * - DateStyle,
     * - IntervalStyle,
     * - TimeZone,
     * - integer_datetimes,
     * - standard_conforming_string
     */
    static void sendParameterStatus(Channel channel, final String name, final String value)
    {
        byte[] nameBytes = name.getBytes(UTF_8);
        byte[] valueBytes = value.getBytes(UTF_8);

        int length = 4 + nameBytes.length + 1 + valueBytes.length + 1;
        ByteBuf buffer = channel.alloc().buffer(length + 1);
        buffer.writeByte('S');
        buffer.writeInt(length);
        writeCString(buffer, nameBytes);
        writeCString(buffer, valueBytes);
        ChannelFuture channelFuture = channel.write(buffer);
        if (LOGGER.isDebugEnabled()) {
            channelFuture.addListener((ChannelFutureListener) future -> LOGGER.debug("sentParameterStatus %s=%s", name, value));
        }
    }

    static void sendAuthenticationError(Channel channel, String message)
    {
        LOGGER.warn(message);
        byte[] msg = message.getBytes(UTF_8);
        byte[] errorCode = PGErrorStatus.INVALID_AUTHORIZATION_SPECIFICATION.code().getBytes(UTF_8);

        sendErrorResponse(channel, message, msg, PGError.SEVERITY_FATAL, null, null,
                METHOD_NAME_CLIENT_AUTH, errorCode);
    }

    static ChannelFuture sendErrorResponse(Channel channel, Throwable throwable)
    {
        PGError error = PGError.fromThrowable(throwable);
        byte[] msg = error.message().getBytes(UTF_8);
        byte[] errorCode = error.status().code().getBytes(UTF_8);
        byte[] lineNumber;
        byte[] fileName = null;
        byte[] methodName;

        if (error.throwable() == null) {
            return sendErrorResponse(channel, error.message(), msg, PGError.SEVERITY_ERROR, null, null, null, errorCode);
        }

        StackTraceElement[] stackTrace = error.throwable().getStackTrace();

        if (stackTrace == null && stackTrace.length == 0) {
            return sendErrorResponse(channel, error.message(), msg, PGError.SEVERITY_ERROR, null, null, null, errorCode);
        }

        StackTraceElement stackTraceElement = stackTrace[0];
        lineNumber = String.valueOf(stackTraceElement.getLineNumber()).getBytes(UTF_8);
        if (stackTraceElement.getFileName() != null) {
            fileName = stackTraceElement.getFileName().getBytes(UTF_8);
        }
        methodName = stackTraceElement.getMethodName().getBytes(UTF_8);

        return sendErrorResponse(channel, error.message(), msg, PGError.SEVERITY_ERROR, lineNumber, fileName, methodName, errorCode);
    }

    /**
     * 'E' | int32 len | char code | str value | \0 | char code | str value | \0 | ... | \0
     * <p>
     * char code / str value -> key-value fields
     * example error fields are: message, detail, hint, error position
     * <p>
     * See https://www.postgresql.org/docs/9.2/static/protocol-error-fields.html for a list of error codes
     */
    private static ChannelFuture sendErrorResponse(Channel channel,
            String message,
            byte[] msg,
            byte[] severity,
            byte[] lineNumber,
            byte[] fileName,
            byte[] methodName,
            byte[] errorCode)
    {
        int length = 4 +
                1 + (severity.length + 1) +
                1 + (msg.length + 1) +
                1 + (errorCode.length + 1) +
                (fileName != null ? 1 + (fileName.length + 1) : 0) +
                (lineNumber != null ? 1 + (lineNumber.length + 1) : 0) +
                (methodName != null ? 1 + (methodName.length + 1) : 0) +
                1;
        ByteBuf buffer = channel.alloc().buffer(length + 1);
        buffer.writeByte('E');
        buffer.writeInt(length);
        buffer.writeByte('S');
        writeCString(buffer, severity);
        buffer.writeByte('M');
        writeCString(buffer, msg);
        buffer.writeByte(('C'));
        writeCString(buffer, errorCode);
        if (fileName != null) {
            buffer.writeByte('F');
            writeCString(buffer, fileName);
        }
        if (lineNumber != null) {
            buffer.writeByte('L');
            writeCString(buffer, lineNumber);
        }
        if (methodName != null) {
            buffer.writeByte('R');
            writeCString(buffer, methodName);
        }
        buffer.writeByte(0);
        ChannelFuture channelFuture = channel.writeAndFlush(buffer);
        if (LOGGER.isDebugEnabled()) {
            channelFuture.addListener((ChannelFutureListener) future -> LOGGER.debug("sentErrorResponse msg=%s length=%s", message, length));
        }
        return channelFuture;
    }

    /**
     * Byte1('D')
     * Identifies the message as a data row.
     * <p>
     * Int32
     * Length of message contents in bytes, including self.
     * <p>
     * Int16
     * The number of column values that follow (possibly zero).
     * <p>
     * Next, the following pair of fields appear for each column:
     * <p>
     * Int32
     * The length of the column value, in bytes (this count does not include itself).
     * Can be zero. As a special case, -1 indicates a NULL column value. No value bytes follow in the NULL case.
     * <p>
     * ByteN
     * The value of the column, in the format indicated by the associated format code. n is the above length.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static void sendDataRow(Channel channel, Object[] row, List<PGType> schema, @Nullable FormatCodes.FormatCode[] formatCodes)
    {
        int length = 4 + 2;

        ByteBuf buffer = channel.alloc().buffer();
        buffer.writeByte('D');
        buffer.writeInt(0); // will be set at the end
        buffer.writeShort(row.length);

        for (int i = 0; i < row.length; i++) {
            Object value;
            try {
                if (row[i] != null) {
                    FormatCodes.FormatCode formatCode = FormatCodes.getFormatCode(formatCodes, i);
                    value = row[i];
                    switch (formatCode) {
                        case TEXT:
                            length += schema.get(i).writeAsText(buffer, value);
                            break;
                        case BINARY:
                            length += schema.get(i).writeAsBinary(buffer, value);
                            break;
                    }
                }
                else {
                    buffer.writeInt(-1);
                    length += 4;
                }
            }
            catch (Exception e) {
                buffer.release();
                throw e;
            }
        }

        buffer.setInt(1, length);
        channel.write(buffer);
    }

    static void writeCString(ByteBuf buffer, byte[] valBytes)
    {
        buffer.writeBytes(valBytes);
        buffer.writeByte(0);
    }

    /**
     * ParameterDescription (B)
     * <p>
     * Byte1('t')
     * <p>
     * Identifies the message as a parameter description.
     * Int32
     * <p>
     * Length of message contents in bytes, including self.
     * Int16
     * <p>
     * The number of parameters used by the statement (can be zero).
     * <p>
     * Then, for each parameter, there is the following:
     * <p>
     * Int32
     * <p>
     * Specifies the object ID of the parameter data type.
     *
     * @param channel The channel to write the parameter description to.
     * @param oids The oid of input parameters
     */
    static void sendParameterDescription(Channel channel, List<Integer> oids)
    {
        final int messageByteSize = 4 + 2 + oids.size() * 4;
        ByteBuf buffer = channel.alloc().buffer(messageByteSize);
        buffer.writeByte('t');
        buffer.writeInt(messageByteSize);
        if (oids.size() > Short.MAX_VALUE) {
            buffer.release();
            throw new IllegalArgumentException("Too many parameters. Max supported: " + Short.MAX_VALUE);
        }
        buffer.writeShort(oids.size());
        for (Integer oid : oids) {
            buffer.writeInt(oid);
        }
        ChannelFuture channelFuture = channel.write(buffer);
        if (LOGGER.isDebugEnabled()) {
            channelFuture.addListener((ChannelFutureListener) future -> LOGGER.debug("sentParameterDescription"));
        }
    }

    /**
     * RowDescription (B)
     * <p>
     * | 'T' | int32 len | int16 numCols
     * <p>
     * For each field:
     * <p>
     * | string name | int32 table_oid | int16 attr_num | int32 oid | int16 typlen | int32 type_modifier | int16 format_code
     * <p>
     * See https://www.postgresql.org/docs/current/static/protocol-message-formats.html
     */
    static void sendRowDescription(Channel channel, List<Column> columns, @Nullable FormatCodes.FormatCode[] formatCodes)
    {
        int length = 4 + 2;
        int columnSize = 4 + 2 + 4 + 2 + 4 + 2;
        ByteBuf buffer = channel.alloc().buffer(
                length + (columns.size() * (10 + columnSize))); // use 10 as an estimate for columnName length

        buffer.writeByte('T');
        buffer.writeInt(0); // will be set at the end
        buffer.writeShort(columns.size());

        int idx = 0;
        for (Column column : columns) {
            byte[] nameBytes = column.getName().getBytes(UTF_8);
            length += nameBytes.length + 1;
            length += columnSize;

            writeCString(buffer, nameBytes);
            buffer.writeInt(0);     // table_oid
            buffer.writeShort(0);   // attr_num

            PGType<?> pgType = (PGType<?>) column.getType();
            buffer.writeInt(pgType.oid());
            buffer.writeShort(pgType.typeLen());
            buffer.writeInt(pgType.typeMod());
            buffer.writeShort(FormatCodes.getFormatCode(formatCodes, idx).ordinal());

            idx++;
        }

        buffer.setInt(1, length);
        ChannelFuture channelFuture = channel.write(buffer);
        if (LOGGER.isDebugEnabled()) {
            channelFuture.addListener((ChannelFutureListener) future -> LOGGER.debug("sentRowDescription"));
        }
    }

    /**
     * ParseComplete
     * | '1' | int32 len |
     */
    static void sendParseComplete(Channel channel)
    {
        sendShortMsg(channel, '1', "sentParseComplete");
    }

    /**
     * BindComplete
     * | '2' | int32 len |
     */
    static void sendBindComplete(Channel channel)
    {
        sendShortMsg(channel, '2', "sentBindComplete");
    }

    /**
     * EmptyQueryResponse
     * | 'I' | int32 len |
     */
    static void sendEmptyQueryResponse(Channel channel)
    {
        sendShortMsg(channel, 'I', "sentEmptyQueryResponse");
    }

    /**
     * NoData
     * | 'n' | int32 len |
     */
    static void sendNoData(Channel channel)
    {
        sendShortMsg(channel, 'n', "sentNoData");
    }

    /**
     * Send a message that just contains the msgType and the msg length
     */
    private static void sendShortMsg(Channel channel, char msgType, final String traceLogMsg)
    {
        ByteBuf buffer = channel.alloc().buffer(5);
        buffer.writeByte(msgType);
        buffer.writeInt(4);

        ChannelFuture channelFuture = channel.write(buffer);
        if (LOGGER.isDebugEnabled()) {
            channelFuture.addListener((ChannelFutureListener) future -> LOGGER.debug(traceLogMsg));
        }
    }

    static ChannelFuture sendPortalSuspended(Channel channel)
    {
        ByteBuf buffer = channel.alloc().buffer(5);
        buffer.writeByte('s');
        buffer.writeInt(4);

        ChannelFuture channelFuture = channel.writeAndFlush(buffer);
        if (LOGGER.isDebugEnabled()) {
            channelFuture.addListener((ChannelFutureListener) future -> LOGGER.debug("sentPortalSuspended"));
        }
        return channelFuture;
    }

    /**
     * CloseComplete
     * | '3' | int32 len |
     */
    static void sendCloseComplete(Channel channel)
    {
        sendShortMsg(channel, '3', "sentCloseComplete");
    }

    /**
     * AuthenticationCleartextPassword (B)
     * <p>
     * Byte1('R')
     * Identifies the message as an authentication request.
     * <p>
     * Int32(8)
     * Length of message contents in bytes, including self.
     * <p>
     * Int32(3)
     * Specifies that a clear-text password is required.
     *
     * @param channel The channel to write to.
     */
    static void sendAuthenticationCleartextPassword(Channel channel)
    {
        ByteBuf buffer = channel.alloc().buffer(9);
        buffer.writeByte('R');
        buffer.writeInt(8);
        buffer.writeInt(3);
        ChannelFuture channelFuture = channel.writeAndFlush(buffer);
        if (LOGGER.isDebugEnabled()) {
            channelFuture.addListener((ChannelFutureListener) future -> LOGGER.debug("sentAuthenticationCleartextPassword"));
        }
    }

    private static String buildCommandTag(String query, long rowCount)
    {
        /*
         * from https://www.postgresql.org/docs/current/static/protocol-message-formats.html:
         *
         * For an INSERT command, the tag is INSERT oid rows, where rows is the number of rows inserted.
         * oid is the object ID of the inserted row if rows is 1 and the target table has OIDs; otherwise oid is 0.
         */
        switch (query) {
            case "INSERT":
                return "INSERT 0 " + rowCount;
            case "SELECT":
                return query + " " + rowCount;
            default:
                return query;
        }
    }
}
