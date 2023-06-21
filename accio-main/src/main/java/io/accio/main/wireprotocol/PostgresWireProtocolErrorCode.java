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

import io.accio.base.ErrorCode;
import io.accio.base.ErrorCodeSupplier;
import io.accio.base.ErrorType;

import static io.accio.base.ErrorType.INTERNAL_ERROR;
import static io.accio.base.ErrorType.USER_ERROR;

public enum PostgresWireProtocolErrorCode
        implements ErrorCodeSupplier
{
    UNSUPPORTED_OPERATION(0, USER_ERROR),
    UNDEFINED_FUNCTION(1, USER_ERROR),
    INVALID_PREPARED_STATEMENT_NAME(2, USER_ERROR),
    UNKNOWN_SESSION_COMMAND(3, USER_ERROR),
    UNKNOWN_PROPERTY(4, USER_ERROR),

    PG_ARRAY_PARSE_ERROR(10, INTERNAL_ERROR),
    WRONG_SSL_CONFIGURATION(11, INTERNAL_ERROR),
    MESSAGE_SENDING_ERROR(12, INTERNAL_ERROR),
    /**/;

    private final ErrorCode errorCode;

    PostgresWireProtocolErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0610_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
