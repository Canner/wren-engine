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
import io.wren.base.WrenException;
import io.wren.base.type.PGType;
import io.wren.base.type.PGTypes;
import io.wren.main.wireprotocol.FormatCodes;
import io.wren.main.wireprotocol.WireProtocolSession;

import java.util.ArrayList;
import java.util.List;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.wren.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.wren.main.wireprotocol.FormatCodes.FormatCode;
import static io.wren.main.wireprotocol.Utils.readCString;
import static java.lang.String.format;

public class Bind
        implements Plan
{
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
    public static Bind bind(ByteBuf buffer)
    {
        String portalName = readCString(buffer);
        String statementName = readCString(buffer);
        FormatCode[] formatCodes = FormatCodes.fromBuffer(buffer);
        List<byte[]> parameters = readParameters(buffer);
        FormatCode[] resultFormats = FormatCodes.fromBuffer(buffer);
        return new Bind(portalName, statementName, formatCodes, parameters, resultFormats);
    }

    static Bind bind(String portalName, String statementName, FormatCode[] parameterFormats, List<byte[]> parameters, FormatCode[] resultFormats)
    {
        return new Bind(portalName, statementName, parameterFormats, parameters, resultFormats);
    }

    private static List<byte[]> readParameters(ByteBuf buffer)
    {
        int parameterCount = buffer.readShort();
        List<byte[]> parameters = new ArrayList<>(parameterCount);
        for (int i = 0; i < parameterCount; i++) {
            int length = buffer.readInt();
            if (length == -1) {
                parameters.add(null);
            }
            else {
                byte[] value = new byte[length];
                buffer.readBytes(value);
                parameters.add(value);
            }
        }
        return parameters;
    }

    private static final Logger LOG = Logger.get(Bind.class);
    private final String portalName;
    private final String statementName;
    private final FormatCode[] parameterFormats;
    private final List<byte[]> parameters;
    private final FormatCode[] resultFormats;

    private Bind(
            String portalName,
            String statementName,
            FormatCode[] parameterFormats,
            List<byte[]> parameters,
            FormatCode[] resultFormats)
    {
        this.portalName = portalName;
        this.statementName = statementName;
        this.parameterFormats = parameterFormats;
        this.parameters = parameters;
        this.resultFormats = resultFormats;
    }

    @Override
    public Runnable execute(Channel channel, WireProtocolSession session)
    {
        return () -> {
            try {
                List<Object> params = readParameters(session);
                session.bind(portalName, statementName, params, resultFormats);
                ResponseMessages.sendBindComplete(channel);
            }
            catch (WrenException e) {
                LOG.error(e, "Bind failed. Portal: %s, Statement: %s", portalName, statementName);
                ResponseMessages.sendErrorResponse(channel, e);
            }
        };
    }

    private List<Object> readParameters(WireProtocolSession session)
    {
        List<Object> serialized = new ArrayList<>(); // use `ArrayList` to handle `null` elements.
        for (int i = 0; i < parameters.size(); i++) {
            if (parameters.get(i) == null) {
                serialized.add(parameters);
            }
            else {
                int paramType = session.getParamTypeOid(statementName, i);
                PGType<?> pgType = PGTypes.oidToPgType(paramType);
                FormatCodes.FormatCode formatCode = FormatCodes.getFormatCode(parameterFormats, i);
                switch (formatCode) {
                    case TEXT:
                        serialized.add(pgType.readTextValue(wrappedBuffer(parameters.get(i)), parameters.get(i).length));
                        break;
                    case BINARY:
                        serialized.add(pgType.readBinaryValue(wrappedBuffer(parameters.get(i)), parameters.get(i).length));
                        break;
                    default:
                        throw new WrenException(GENERIC_INTERNAL_ERROR,
                                format("Unsupported format code '%d' for param '%s'", formatCode.ordinal(), paramType));
                }
            }
        }
        return serialized;
    }
}
