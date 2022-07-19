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

package io.cml.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Bytes;
import io.cml.spi.type.PGType;
import io.cml.spi.type.PGTypes;
import io.cml.spi.type.UuidType;
import io.cml.spi.type.VarcharType;
import io.cml.wireprotocol.FormatCodes;
import io.cml.wireprotocol.PGError;
import io.cml.wireprotocol.PGErrorStatus;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.intellij.lang.annotations.Language;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.cml.spi.type.BigIntType.BIGINT;
import static io.cml.spi.type.DateType.DATE;
import static io.cml.spi.type.InetType.INET;
import static io.cml.spi.type.IntegerType.INTEGER;
import static io.cml.spi.type.TimestampType.TIMESTAMP;
import static io.cml.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIMEZONE;
import static io.cml.wireprotocol.FormatCodes.FormatCode.BINARY;
import static io.cml.wireprotocol.FormatCodes.FormatCode.TEXT;
import static io.cml.wireprotocol.FormatCodes.getFormatCode;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestingWireProtocolClient
        implements Closeable
{
    private final Socket socketClient;
    private final InputStream in;
    private final OutputStream out;

    public TestingWireProtocolClient(InetSocketAddress isa)
            throws IOException
    {
        this.socketClient = new Socket();
        socketClient.setSoTimeout(60000);
        socketClient.connect(isa);

        this.in = socketClient.getInputStream();
        this.out = socketClient.getOutputStream();
    }

    /**
     * StartupMessage (F)
     * Int32
     * Length of message contents in bytes, including self.
     * <p>
     * Int32(196608)
     * The protocol version number. The most significant 16 bits are the major version number (3 for the protocol described here).
     * The least significant 16 bits are the minor version number (0 for the protocol described here).
     * <p>
     * String
     * The parameter name. Currently recognized names are:
     * <p>
     * user
     * The database user name to connect as. Required; there is no default.
     * <p>
     * String
     * The parameter value.
     *
     * @see <a href="https://www.postgresql.org/docs/9.3/protocol-message-formats.html">protocol-message-formats</a>
     */
    public void sendStartUpMessage(int version, String password, String database, String user)
            throws IOException
    {
        byte[] startup = getStartUpByteArray(password, database, user);
        int len = 4 + startup.length + 4;
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeInt(len);
        buffer.writeInt(version);
        buffer.writeBytes(startup);
        out.write(buffer.array(), 0, len);
        out.flush();
    }

    private byte[] getStartUpByteArray(String password, String database, String user)
    {
        return String.format("password\0%s\0database\0%s\0user\0%s\0", password, database, user).getBytes(UTF_8);
    }

    /**
     * Query (F)
     * Byte1('Q')
     * Identifies the message as a simple query.
     * <p>
     * Int32
     * Length of message contents in bytes, including self.
     * <p>
     * String
     * The query string itself.
     */
    public void sendSimpleQuery(@Language("SQL") String message)
            throws IOException
    {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte('Q');
        byte[] messageBytes = toCStringBytes(message);
        int len = 4 + messageBytes.length;
        buffer.writeInt(len);
        buffer.writeBytes(messageBytes);
        out.write(buffer.array(), 0, len + 1);
        out.flush();
    }

    public void sendDescribe(int type, String name)
            throws IOException
    {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte('D');
        byte[] nameBytes = toCStringBytes(name);

        int len = 4 + 1 + nameBytes.length;
        buffer.writeInt(len);
        buffer.writeByte(type);
        buffer.writeBytes(nameBytes);
        out.write(buffer.array(), 0, len + 1);
        out.flush();
    }

    /**
     * Parse (F)
     * Byte1('P')
     * Identifies the message as a Parse command.
     * <p>
     * Int32
     * Length of message contents in bytes, including self.
     * <p>
     * String
     * The name of the destination prepared statement (an empty string selects the unnamed prepared statement).
     * <p>
     * String
     * The query string to be parsed.
     * <p>
     * Int16
     * The number of parameter data types specified (may be zero).
     * <p>
     * Then, for each parameter, there is the following:
     * <p>
     * Int32
     * Specifies the object ID of the parameter data type. Placing a zero here is equivalent to leaving the type unspecified.
     */
    public void sendParse(String name, @Language("SQL") String statement, List<Integer> paramTypeOids)
            throws IOException
    {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte('P');
        byte[] nameBytes = toCStringBytes(name);
        byte[] stmtBytes = toCStringBytes(statement);
        int len = 4 + nameBytes.length +
                stmtBytes.length +
                2 + paramTypeOids.size() * 4;
        buffer.writeInt(len);
        buffer.writeBytes(nameBytes);
        buffer.writeBytes(stmtBytes);
        buffer.writeShort(paramTypeOids.size());
        for (int oid : paramTypeOids) {
            buffer.writeInt(oid);
        }
        out.write(buffer.array(), 0, len + 1);
        out.flush();
    }

    public void sendNullParse(String name)
            throws IOException
    {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte('P');
        byte[] nameBytes = toCStringBytes(name);
        int len = 4 + nameBytes.length;
        buffer.writeInt(len);
        buffer.writeBytes(nameBytes);
        out.write(buffer.array(), 0, len + 1);
        out.flush();
    }

    /**
     * Bind (F)
     * Byte1('B')
     * Identifies the message as a Bind command.
     * <p>
     * Int32
     * Length of message contents in bytes, including self.
     * <p>
     * String
     * The name of the destination portal (an empty string selects the unnamed portal).
     * <p>
     * String
     * The name of the source prepared statement (an empty string selects the unnamed prepared statement).
     * <p>
     * Int16
     * The number of parameter format codes that follow (denoted C below).
     * <p>
     * Int16[C]
     * The parameter format codes. Each must presently be zero (text) or one (binary).
     * <p>
     * Int16
     * The number of parameter values that follow (possibly zero). This must match the number of parameters needed by the query.
     */
    public void sendBind(String portalName, String statementName, List<Parameter> params, List<FormatCodes.FormatCode> resultFormatCodes)
            throws IOException
    {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte('B');
        byte[] portalNameBytes = toCStringBytes(portalName);
        byte[] statementNameBytes = toCStringBytes(statementName);
        int lengthWriteIndex = buffer.writerIndex();
        buffer.writeInt(0);
        buffer.writeBytes(portalNameBytes);
        buffer.writeBytes(statementNameBytes);
        writeFormatCodes(buffer, params.stream().map(Parameter::getFormatCode).collect(toImmutableList()));
        buffer.writeShort(params.size());

        int paramsLength = 0;
        for (Parameter param : params) {
            paramsLength += writeParams(buffer, param);
        }
        writeFormatCodes(buffer, resultFormatCodes);

        int len = 4 +
                portalNameBytes.length + 1 +
                statementNameBytes.length + 1 +
                2 + params.size() * 2 + // numFormatCodes
                2 + // numParams
                paramsLength +
                2 + resultFormatCodes.size() * 2; // numResultColumnFormatCodes
        buffer.setInt(lengthWriteIndex, len);
        out.write(buffer.array(), 0, len + 1);
        out.flush();
    }

    public void sendBind(String portalName, String statementName, List<Parameter> params)
            throws IOException
    {
        sendBind(portalName, statementName, params, ImmutableList.of());
    }

    private static void writeFormatCodes(ByteBuf buffer, List<FormatCodes.FormatCode> formatCodes)
    {
        int size = formatCodes.size();
        buffer.writeShort(size);
        if (size == 0) {
            return;
        }
        formatCodes.forEach(formatCode -> {
            if (isNull(formatCode)) {
                buffer.writeShort(0);
                return;
            }
            switch (formatCode) {
                case TEXT:
                    buffer.writeShort(0);
                    break;
                case BINARY:
                    buffer.writeShort(1);
            }
        });
    }

    /**
     * Parameter
     * Int32
     * The length of the parameter value, in bytes (this count does not include itself).
     * Can be zero. As a special case, -1 indicates a NULL parameter value. No value bytes follow in the NULL case.
     * <p>
     * Byten
     * The value of the parameter, in the format indicated by the associated format code. n is the above length.
     */
    private int writeParams(ByteBuf buffer, Parameter param)
    {
        byte[] value = param.getFormattedBytes();
        buffer.writeInt(value.length);
        // the strings here are _not_ zero-padded because we specify the length upfront
        buffer.writeBytes(value);
        return 4 + value.length;
    }

    public byte[] toCStringBytes(String jString)
    {
        return (jString + "\0").getBytes(UTF_8);
    }

    /**
     * Execute (F)
     * Byte1('E')
     * Identifies the message as an Execute command.
     * <p>
     * Int32
     * Length of message contents in bytes, including self.
     * <p>
     * String
     * The name of the portal to execute (an empty string selects the unnamed portal).
     * <p>
     * Int32
     * Maximum number of rows to return, if portal contains a query that returns rows (ignored otherwise). Zero denotes "no limit".
     */
    public void sendExecute(String portalName, int maxRow)
            throws IOException
    {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte('E');
        byte[] portalNameBytes = toCStringBytes(portalName);
        int len = 4 + portalNameBytes.length + 4;
        buffer.writeInt(len);
        buffer.writeBytes(portalNameBytes);
        buffer.writeInt(maxRow);
        out.write(buffer.array(), 0, len + 1);
        out.flush();
    }

    /**
     * Terminate (F)
     * Byte1('X')
     * Identifies the message as a termination.
     * <p>
     * Int32(4)
     * Length of message contents in bytes, including self.
     */
    public void sendTerminate()
            throws IOException
    {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte('X');
        int len = 4;
        buffer.writeInt(len);
        out.write(buffer.array(), 0, len + 1);
        out.flush();
    }

    /**
     * Sync (F)
     * Byte1('S')
     * Identifies the message as a Sync command.
     * <p>
     * Int32(4)
     * Length of message contents in bytes, including self.
     */
    public void sendSync()
            throws IOException
    {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte('S');
        int len = 4;
        buffer.writeInt(len);
        out.write(buffer.array(), 0, len + 1);
        out.flush();
    }

    /**
     * Close (F)
     * Byte1('C')
     * Identifies the message as a Close command.
     * <p>
     * Int32
     * Length of message contents in bytes, including self.
     * <p>
     * Byte1
     * 'S' to close a prepared statement; or 'P' to close a portal.
     * <p>
     * String
     * The name of the prepared statement or portal to close (an empty string selects the unnamed prepared statement or portal).
     */
    public void sendClose(int type, String name)
            throws IOException
    {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte('C');
        byte[] nameBytes = toCStringBytes(name);
        int len = 4 + 1 + nameBytes.length;
        buffer.writeInt(len);
        buffer.writeByte(type);
        buffer.writeBytes(nameBytes);
        out.write(buffer.array(), 0, len + 1);
        out.flush();
    }

    /**
     * Flush (F)
     * Byte1('H')
     * Identifies the message as a Flush command.
     * <p>
     * Int32(4)
     * Length of message contents in bytes, including self.
     */
    public void sendFlush()
            throws IOException
    {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte('H');
        int len = 4;
        buffer.writeInt(len);
        out.write(buffer.array(), 0, len + 1);
        out.flush();
    }

    /**
     * DataRow (B)
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
     * The length of the column value, in bytes (this count does not include itself). Can be zero. As a special case, -1 indicates a NULL column value. No value bytes follow in the NULL case.
     * <p>
     * Byten
     * The value of the column, in the format indicated by the associated format code. n is the above length.
     */
    public void assertDataRow(String expectedRowString)
            throws IOException
    {
        assertDataRow(expectedRowString, ImmutableList.of(), null);
    }

    public void assertDataRow(String expectedRowString, ImmutableList<PGType<?>> types, FormatCodes.FormatCode[] formatCodes)
            throws IOException
    {
        byte[] header = new byte[1 + 4 + 2];
        in.read(header);
        ByteBuffer headerBuffer = ByteBuffer.wrap(header);
        String id = new String(new byte[] {headerBuffer.get()}, UTF_8);
        assertThat(id).isEqualTo("D");
        int totalLength = headerBuffer.getInt();
        int numColumns = headerBuffer.getShort();
        byte[] row = new byte[totalLength - 4 - 2];
        in.read(row);
        ByteBuffer rowBuffer = ByteBuffer.wrap(row);
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < numColumns; i++) {
            if (getFormatCode(formatCodes, i).equals(TEXT)) {
                int fieldLen = rowBuffer.getInt();
                byte[] tmp = new byte[fieldLen];
                rowBuffer.get(tmp);
                stringBuilder.append(new String(tmp, UTF_8));
            }
            else {
                Object result = getFieldBinaryValue(rowBuffer, types.get(i));
                stringBuilder.append(result);
            }
            stringBuilder.append(',');
        }
        stringBuilder.setLength(stringBuilder.length() - 1);

        assertThat(stringBuilder.toString()).isEqualTo(expectedRowString);
    }

    private Object getFieldBinaryValue(ByteBuffer buffer, PGType<?> type)
    {
        int fieldLen = buffer.getInt();
        if (fieldLen == -1) {
            return "NULL";
        }
        if (DATE.equals(type) || INTEGER.equals(type)) {
            return buffer.getInt();
        }
        else if (TIMESTAMP.equals(type) || TIMESTAMP_WITH_TIMEZONE.equals(type) || BIGINT.equals(type)) {
            return buffer.getLong();
        }
        else if (INET.equals(type) || type instanceof VarcharType) {
            byte[] tmp = new byte[fieldLen];
            buffer.get(tmp);
            return "'" + new String(tmp, UTF_8) + "'";
        }
        else if (UuidType.UUID.equals(type)) {
            long most = buffer.getLong();
            long least = buffer.getLong();
            return new UUID(most, least);
        }
        throw new AssertionError("Unsupported pg type: " + type.typName());
    }

    private String readStringAsUtf8(ByteBuffer source, int length)
    {
        byte[] tmp = new byte[length];
        source.get(tmp);
        return new String(tmp, UTF_8);
    }

    /**
     * ParameterStatus (B)
     * Byte1('S')
     * Identifies the message as a run-time parameter status report.
     * <p>
     * Int32
     * Length of message contents in bytes, including self.
     * <p>
     * String
     * The name of the run-time parameter being reported.
     * <p>
     * String
     * The current value of the parameter
     */
    public void assertParameterStatus(String statusName, String expectedMessage)
            throws IOException
    {
        String statusNameWithEnd = statusName + "\0";
        String expectedMessageWithEnd = expectedMessage + "\0";

        byte[] statusId = readBytes(1);
        assertThat(statusId).isEqualTo(new byte[] {'S'});

        byte[] lengthBytes = readBytes(4);
        int length = ByteBuffer.wrap(lengthBytes).getInt();
        byte[] statusMessage = readBytes(length - 4);
        ByteBuffer messageBuffer = ByteBuffer.wrap(statusMessage);
        byte[] nameBytes = new byte[statusNameWithEnd.length()];
        messageBuffer.get(nameBytes);
        assertThat(new String(nameBytes, UTF_8)).isEqualTo(statusNameWithEnd);

        byte[] bodyBytes = new byte[length - 4 - statusNameWithEnd.length()];
        messageBuffer.get(bodyBytes);
        String tmp = new String(bodyBytes, UTF_8);
        assertThat(tmp).isEqualTo(expectedMessageWithEnd);
    }

    /**
     * EmptyQueryResponse (B)
     * Byte1('I')
     * Identifies the message as a response to an empty query string. (This substitutes for CommandComplete.)
     * <p>
     * Int32(4)
     * Length of message contents in bytes, including self.
     */
    public void assertEmptyResponse()
            throws IOException
    {
        byte[] statusId = readBytes(1);
        assertThat(statusId).isEqualTo(new byte[] {'I'});

        byte[] lengthBytes = readBytes(4);
        int length = ByteBuffer.wrap(lengthBytes).getInt();
        assertThat(length).isEqualTo(4);
    }

    /**
     * ReadyForQuery (B)
     * Byte1('Z')
     * Identifies the message type. ReadyForQuery is sent whenever the backend is ready for a new query cycle.
     * <p>
     * Int32(5)
     * Length of message contents in bytes, including self.
     * <p>
     * Byte1
     * Current backend transaction status indicator.
     * Possible values are 'I' if idle (not in a transaction block); 'T' if in a transaction block;
     * or 'E' if in a failed transaction block (queries will be rejected until block is ended).
     */
    public void assertReadyForQuery(char state)
            throws IOException
    {
        byte[] readyForQueryResponse = readBytes(6);
        assertThat(readyForQueryResponse).isEqualTo(new byte[] {'Z', 0, 0, 0, 5, (byte) state});
    }

    /**
     * CommandComplete (B)
     * Byte1('C')
     * Identifies the message as a command-completed response.
     * <p>
     * Int32
     * Length of message contents in bytes, including self.
     * <p>
     * String
     * The command tag. This is usually a single word that identifies which SQL command was completed.
     *
     * @see <a href="https://www.postgresql.org/docs/9.3/protocol-message-formats.html"></a>
     */
    public void assertCommandComplete(String expectedTag)
            throws IOException
    {
        String expectedTagWithEnd = expectedTag + "\0";

        byte[] commandCompleteHeader = readBytes(5);
        assertThat(commandCompleteHeader).isEqualTo(new byte[] {'C', 0, 0, 0, (byte) (expectedTag.length() + 5)});
        ByteBuffer headerBuffer = ByteBuffer.wrap(commandCompleteHeader);
        int commandTagLen = headerBuffer.getInt(1) - 4;
        byte[] commandTagBytes = readBytes(commandTagLen);
        String commandTag = new String(commandTagBytes, UTF_8);
        assertThat(commandTag).isEqualTo(expectedTagWithEnd);
    }

    /**
     * PortalSuspended (B)
     * Byte1('s')
     * Identifies the message as a portal-suspended indicator. Note this only appears if an Execute message's row-count limit was reached.
     * <p>
     * Int32(4)
     * Length of message contents in bytes, including self.
     */
    public void assertPortalPortalSuspended()
            throws IOException
    {
        byte[] response = readBytes(5);
        assertThat(response).isEqualTo(new byte[] {'s', 0, 0, 0, 4});
    }

    /**
     * ParseComplete (B)
     * Byte1('1')
     * Identifies the message as a Parse-complete indicator.
     * <p>
     * Int32(4)
     * Length of message contents in bytes, including self.
     */
    public void assertParseComplete()
            throws IOException
    {
        byte[] response = readBytes(5);
        assertThat(response).isEqualTo(new byte[] {'1', 0, 0, 0, 4});
    }

    /**
     * BindComplete (B)
     * Byte1('2')
     * Identifies the message as a Bind-complete indicator.
     * <p>
     * Int32(4)
     * Length of message contents in bytes, including self.
     */
    public void assertBindComplete()
            throws IOException
    {
        byte[] response = readBytes(5);
        assertThat(response).isEqualTo(new byte[] {'2', 0, 0, 0, 4});
    }

    /**
     * CloseComplete (B)
     * Byte1('3')
     * Identifies the message as a Close-complete indicator.
     * <p>
     * Int32(4)
     * Length of message contents in bytes, including self.
     */
    public void assertCloseComplete()
            throws IOException
    {
        byte[] response = readBytes(5);
        assertThat(response).isEqualTo(new byte[] {'3', 0, 0, 0, 4});
    }

    /**
     * AuthenticationOk (B)
     * Byte1('R')
     * Identifies the message as an authentication request.
     * <p>
     * Int32(8)
     * Length of message contents in bytes, including self.
     * <p>
     * Int32(0)
     * Specifies that the authentication was successful.
     */
    public void assertAuthOk()
            throws IOException
    {
        byte[] authOkResponse = readBytes(9);
        assertThat(authOkResponse).isEqualTo(new byte[] {'R', 0, 0, 0, 8, 0, 0, 0, 0});
    }

    public void assertAuthError(String expectedMessage)
            throws IOException
    {
        byte[] errorCode = PGErrorStatus.INVALID_AUTHORIZATION_SPECIFICATION.code().getBytes(StandardCharsets.UTF_8);
        byte[] methodNameClientAuth = "ClientAuthentication".getBytes(StandardCharsets.UTF_8);

        assertErrorResponse(expectedMessage.getBytes(UTF_8), PGError.SEVERITY_FATAL, null, null,
                methodNameClientAuth, errorCode);
    }

    /**
     * 'E' | int32 len | char code | str value | \0 | char code | str value | \0 | ... | \0
     * <p>
     * char code / str value -> key-value fields
     * example error fields are: message, detail, hint, error position
     * <p>
     * See https://www.postgresql.org/docs/9.2/static/protocol-error-fields.html for a list of error codes
     */
    public void assertErrorResponse(byte[] msg,
            byte[] severity,
            byte[] lineNumber,
            byte[] fileName,
            byte[] methodName,
            byte[] errorCode)
            throws IOException
    {
        int expectedLength = 4 +
                1 + (severity.length + 1) +
                1 + (msg.length + 1) +
                1 + (errorCode.length + 1) +
                (fileName != null ? 1 + (fileName.length + 1) : 0) +
                (lineNumber != null ? 1 + (lineNumber.length + 1) : 0) +
                (methodName != null ? 1 + (methodName.length + 1) : 0) +
                1;
        assertThat(readBytes(1)).isEqualTo(new byte[] {'E'});
        int length = ByteBuffer.wrap(readBytes(4)).getInt();
        assertThat(length).isEqualTo(expectedLength);
        assertThat(readBytes(1)).isEqualTo(new byte[] {'S'});
        assertCString(severity);
        assertThat(readBytes(1)).isEqualTo(new byte[] {'M'});
        assertCString(msg);
        assertThat(readBytes(1)).isEqualTo(new byte[] {'C'});
        assertCString(errorCode);
        if (fileName != null) {
            assertThat(readBytes(1)).isEqualTo(new byte[] {'F'});
            assertCString(fileName);
        }
        if (lineNumber != null) {
            assertThat(readBytes(1)).isEqualTo(new byte[] {'L'});
            assertCString(lineNumber);
        }
        if (methodName != null) {
            assertThat(readBytes(1)).isEqualTo(new byte[] {'R'});
            assertCString(methodName);
        }
        assertCString(new byte[] {});
    }

    public void assertErrorMessage(@Language("RegExp") String expectedPattern)
            throws IOException
    {
        ErrorResponse response = readErrorResponse();
        assertThat(response.message).matches(expectedPattern);
    }

    private void assertCString(byte[] message)
            throws IOException
    {
        byte[] cString = Bytes.concat(message, new byte[] {'\0'});
        assertThat(readBytes(cString.length)).isEqualTo(cString);
    }

    public static class ErrorResponse
    {
        private String message;
        private String severity;
        private String lineNumber;
        private String fileName;
        private String methodName;
        private String errorCode;

        public String getMessage()
        {
            return message;
        }

        public void setMessage(String message)
        {
            this.message = message;
        }

        public String getSeverity()
        {
            return severity;
        }

        public void setSeverity(String severity)
        {
            this.severity = severity;
        }

        public String getLineNumber()
        {
            return lineNumber;
        }

        public void setLineNumber(String lineNumber)
        {
            this.lineNumber = lineNumber;
        }

        public String getFileName()
        {
            return fileName;
        }

        public void setFileName(String fileName)
        {
            this.fileName = fileName;
        }

        public String getMethodName()
        {
            return methodName;
        }

        public void setMethodName(String methodName)
        {
            this.methodName = methodName;
        }

        public String getErrorCode()
        {
            return errorCode;
        }

        public void setErrorCode(String errorCode)
        {
            this.errorCode = errorCode;
        }
    }

    /**
     * RowDescription (B)
     * Byte1('T')
     * Identifies the message as a row description.
     * <p>
     * Int32
     * Length of message contents in bytes, including self.
     * <p>
     * Int16
     * Specifies the number of fields in a row (can be zero).
     */

    public List<Field> assertAndGetRowDescriptionFields()
            throws IOException
    {
        byte[] statusId = readBytes(1);
        assertThat(statusId).isEqualTo(new byte[] {'T'});

        byte[] lengthBytes = readBytes(4);
        int length = ByteBuffer.wrap(lengthBytes).getInt();

        byte[] statusMessage = readBytes(length - 4);
        ByteBuffer messageBuffer = ByteBuffer.wrap(statusMessage);
        short num = messageBuffer.getShort();
        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        for (int i = 0; i < num; i++) {
            fields.add(new Field(messageBuffer));
        }
        return fields.build();
    }

    /**
     * NoData (B)
     * Byte1('n')
     * Identifies the message as a no-data indicator.
     * <p>
     * Int32(4)
     * Length of message contents in bytes, including self.
     */
    public void assertNoData()
            throws IOException
    {
        byte[] statusId = readBytes(1);
        assertThat(statusId).isEqualTo(new byte[] {'n'});

        byte[] lengthBytes = readBytes(4);
        int length = ByteBuffer.wrap(lengthBytes).getInt();
        assertThat(length).isEqualTo(4);
    }

    /**
     * String
     * The field name.
     * <p>
     * Int32
     * If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.
     * <p>
     * Int16
     * If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.
     * <p>
     * Int32
     * The object ID of the field's data type.
     * <p>
     * Int16
     * The data type size (see pg_type.typlen). Note that negative values denote variable-width types.
     * <p>
     * Int32
     * The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
     * <p>
     * Int16
     * The format code being used for the field. Currently will be zero (text) or one (binary).
     * In a RowDescription returned from the statement variant of Describe, the format code is not yet known and will always be zero.
     */

    public static class Field
    {
        String name;
        int tableId;
        short columnsNum;
        int typeId;
        short typeSize;
        int typeModifier;
        short formatCode;

        public Field(ByteBuffer inBuffer)
        {
            ImmutableList.Builder<Byte> nameBuffer = ImmutableList.builder();
            byte element;
            while ((element = inBuffer.get()) != '\0') {
                nameBuffer.add(element);
            }
            name = new String(Bytes.toArray(nameBuffer.build()), UTF_8);
            tableId = inBuffer.getInt();
            columnsNum = inBuffer.getShort();
            typeId = inBuffer.getInt();
            typeSize = inBuffer.getShort();
            typeModifier = inBuffer.getInt();
            formatCode = inBuffer.getShort();
        }

        public int getTypeId()
        {
            return typeId;
        }
    }

    public List<PGType<?>> assertAndGetParameterDescription()
            throws IOException
    {
        byte[] statusId = readBytes(1);
        assertThat(statusId).isEqualTo(new byte[] {'t'});

        byte[] lengthBytes = readBytes(4);
        int length = ByteBuffer.wrap(lengthBytes).getInt();

        byte[] statusMessage = readBytes(length - 4);
        ByteBuffer messageBuffer = ByteBuffer.wrap(statusMessage);
        short num = messageBuffer.getShort();
        ImmutableList.Builder<PGType<?>> fields = ImmutableList.builder();
        for (int i = 0; i < num; i++) {
            fields.add(requireNonNull(PGTypes.oidToPgType(messageBuffer.getInt())));
        }
        return fields.build();
    }

    public void printResult(ImmutableList<PGType<?>> types, FormatCodes.FormatCode[] formatCodes)
            throws IOException
    {
        while (true) {
            byte[] header = new byte[1 + 4];
            in.read(header);
            ByteBuffer headerBuffer = ByteBuffer.wrap(header);
            String id = new String(new byte[] {headerBuffer.get()}, UTF_8);
            int totalLength = headerBuffer.getInt() - 4;

            switch (id) {
                case "D":
                    printDataRow(totalLength, types, formatCodes);
                    break;
                case "C":
                    printCommandComplete(totalLength);
                    return;
                default:
                    throw new IllegalStateException("id: " + id);
            }
        }
    }

    public void printDataRow(int totalLength, ImmutableList<PGType<?>> types, FormatCodes.FormatCode[] formatCodes)
            throws IOException
    {
        byte[] row = new byte[totalLength];
        in.read(row);
        ByteBuffer rowBuffer = ByteBuffer.wrap(row);
        int numColumns = rowBuffer.getShort();
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < numColumns; i++) {
            if (getFormatCode(formatCodes, i).equals(TEXT)) {
                int fieldLen = rowBuffer.getInt();
                byte[] tmp = new byte[fieldLen];
                rowBuffer.get(tmp);
                stringBuilder.append(new String(tmp, UTF_8));
            }
            else {
                Object result = getFieldBinaryValue(rowBuffer, types.get(i));
                stringBuilder.append(result);
            }
            stringBuilder.append(',');
        }
        stringBuilder.setLength(stringBuilder.length() - 1);
        System.out.println(stringBuilder);
    }

    public void printCommandComplete(int totalLength)
            throws IOException
    {
        byte[] commandTagBytes = new byte[totalLength];
        in.read(commandTagBytes);
        String commandTag = new String(commandTagBytes, UTF_8);
        System.out.println(commandTag);
    }

    @Override
    public void close()
            throws IOException
    {
        sendTerminate();
        in.close();
        out.close();
        socketClient.close();
    }

    private byte[] readBytes(int length)
            throws IOException
    {
        byte[] bytes = new byte[length];
        in.read(bytes);
        return bytes;
    }

    public ErrorResponse readErrorResponse()
            throws IOException
    {
        assertThat(readBytes(1)).isEqualTo(new byte[] {'E'});
        int length = ByteBuffer.wrap(readBytes(4)).getInt();
        ErrorResponse errorResponse = new ErrorResponse();
        ByteBuffer buffer = ByteBuffer.wrap(readBytes(length - 4));
        while (buffer.hasRemaining()) {
            byte id = buffer.get();
            // https://www.postgresql.org/docs/9.3/protocol-error-fields.html
            switch (id) {
                case 'S':
                    errorResponse.setSeverity(readCString(buffer));
                    break;
                case 'M':
                    errorResponse.setMessage(readCString(buffer));
                    break;
                case 'C':
                    errorResponse.setErrorCode(readCString(buffer));
                    break;
                case 'F':
                    errorResponse.setFileName(readCString(buffer));
                    break;
                case 'L':
                    errorResponse.setLineNumber(readCString(buffer));
                    break;
                case 'R':
                    errorResponse.setMethodName(readCString(buffer));
            }
        }
        return errorResponse;
    }

    private static String readCString(ByteBuffer input)
    {
        ByteBuffer output = ByteBuffer.allocate(2048);
        for (byte c = input.get(); c != '\0'; c = input.get()) {
            output.put(c);
        }
        output.flip();
        byte[] bytes = new byte[output.limit()];
        output.get(bytes);
        return new String(bytes, UTF_8);
    }

    public static final class DescribeType
    {
        public static final byte PORTAL = 'P';

        public static final byte STATEMENT = 'S';

        private DescribeType() {}
    }

    public static class Parameter
    {
        private final Object value;
        private final PGType<?> type;
        private final FormatCodes.FormatCode formatCode;

        public static Parameter textParameter(Object value, PGType<?> type)
        {
            return new Parameter(value, type, TEXT);
        }

        public static Parameter binaryParameter(Object value, PGType<?> type)
        {
            return new Parameter(value, type, BINARY);
        }

        private Parameter(Object value, PGType<?> type, FormatCodes.FormatCode formatCode)
        {
            this.value = value;
            this.type = type;
            this.formatCode = formatCode;
        }

        public Object getValue()
        {
            return value;
        }

        public PGType<?> getType()
        {
            return type;
        }

        public FormatCodes.FormatCode getFormatCode()
        {
            return formatCode;
        }

        public byte[] getFormattedBytes()
        {
            if (formatCode == TEXT) {
                return value.toString().getBytes(UTF_8);
            }
            return getBinary();
        }

        private byte[] getBinary()
        {
            if (type.equals(UuidType.UUID)) {
                ByteBuffer buffer = ByteBuffer.allocate(16);
                UUID uuid = UUID.fromString(value.toString());
                buffer.putLong(uuid.getMostSignificantBits());
                buffer.putLong(uuid.getLeastSignificantBits());
                return buffer.array();
            }
            throw new RuntimeException("Unsupported write binary type: " + type);
        }
    }
}
