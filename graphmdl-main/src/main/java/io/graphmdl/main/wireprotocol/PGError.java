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

package io.graphmdl.main.wireprotocol;

import com.google.common.collect.ImmutableMap;
import io.graphmdl.base.ErrorCode;
import io.graphmdl.base.ErrorCodeSupplier;
import io.graphmdl.base.ErrorType;
import io.graphmdl.base.GraphMDLException;
import io.graphmdl.base.metadata.StandardErrorCode;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class PGError
{
    public static final byte[] SEVERITY_FATAL = "FATAL".getBytes(StandardCharsets.UTF_8);
    public static final byte[] SEVERITY_ERROR = "ERROR".getBytes(StandardCharsets.UTF_8);

    private final PGErrorStatus status;
    private final String message;

    @Nullable
    private final Throwable throwable;

    public PGError(PGErrorStatus status, String message, @Nullable Throwable throwable)
    {
        this.status = status;
        this.message = message;
        this.throwable = throwable;
    }

    public PGErrorStatus status()
    {
        return status;
    }

    @Nullable
    public Throwable throwable()
    {
        return throwable;
    }

    public String message()
    {
        return message;
    }

    @Override
    public String toString()
    {
        return "PGError{" +
                "status=" + status +
                ", message='" + message + '\'' +
                ", throwable=" + throwable +
                '}';
    }

    public static PGError fromThrowable(Throwable throwable)
    {
        if (throwable instanceof GraphMDLException) {
            return new PGError(
                    Converter.from(((GraphMDLException) throwable).getErrorCode()),
                    throwable.getMessage(),
                    throwable);
        }

        return new PGError(PGErrorStatus.INTERNAL_ERROR, throwable.getMessage(), throwable);
    }

    private static final class Converter
    {
        enum UndefinedErrorCode
                implements ErrorCodeSupplier
        {
            INSUFFICIENT_RESOURCES(0, ErrorType.INSUFFICIENT_RESOURCES),
            INTERNAL_ERROR(1, ErrorType.INTERNAL_ERROR),
            EXTERNAL(2, ErrorType.EXTERNAL),
            USER_ERROR(3, ErrorType.USER_ERROR);

            private static final Map<ErrorType, ErrorCodeSupplier> MAP = Arrays.stream(values())
                    .collect(toImmutableMap(supplier -> supplier.toErrorCode().getType(), Function.identity()));

            private final ErrorCode errorCode;

            UndefinedErrorCode(int code, ErrorType type)
            {
                this.errorCode = new ErrorCode(code + 0xffff * 3, name(), type);
            }

            @Override
            public ErrorCode toErrorCode()
            {
                return this.errorCode;
            }

            public static ErrorCodeSupplier valueOf(ErrorType type)
            {
                return MAP.get(type);
            }
        }

        private static final Map<ErrorCode, PGErrorStatus> MAP = new ImmutableMap.Builder<ErrorCode, PGErrorStatus>()
        {
            public void put(ErrorCodeSupplier supplier, PGErrorStatus status)
            {
                this.put(supplier.toErrorCode(), status);
            }

            {
                // error type is insufficient resources
                put(UndefinedErrorCode.INSUFFICIENT_RESOURCES, PGErrorStatus.INSUFFICIENT_RESOURCES);
                put(StandardErrorCode.CLUSTER_OUT_OF_MEMORY, PGErrorStatus.OUT_OF_MEMORY);
                put(StandardErrorCode.EXCEEDED_CPU_LIMIT, PGErrorStatus.CONFIGURATION_LIMIT_EXCEEDED);
                put(StandardErrorCode.EXCEEDED_GLOBAL_MEMORY_LIMIT, PGErrorStatus.CONFIGURATION_LIMIT_EXCEEDED);
                put(StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT, PGErrorStatus.CONFIGURATION_LIMIT_EXCEEDED);
                put(StandardErrorCode.EXCEEDED_SPILL_LIMIT, PGErrorStatus.CONFIGURATION_LIMIT_EXCEEDED);
                put(StandardErrorCode.EXCEEDED_TIME_LIMIT, PGErrorStatus.CONFIGURATION_LIMIT_EXCEEDED);
                // error type is internal error
                put(UndefinedErrorCode.INTERNAL_ERROR, PGErrorStatus.INTERNAL_ERROR);
                // error type is external
                put(UndefinedErrorCode.EXTERNAL, PGErrorStatus.FDW_ERROR);
                // error type is user error
                put(UndefinedErrorCode.USER_ERROR, PGErrorStatus.CANNOT_CONNECT_NOW);
                put(StandardErrorCode.SYNTAX_ERROR, PGErrorStatus.SYNTAX_ERROR);
                put(StandardErrorCode.USER_CANCELED, PGErrorStatus.QUERY_CANCELED);
                put(StandardErrorCode.DIVISION_BY_ZERO, PGErrorStatus.DIVISION_BY_ZERO);
                put(StandardErrorCode.INVALID_CAST_ARGUMENT, PGErrorStatus.INVALID_CHARACTER_VALUE_FOR_CAST);
                put(StandardErrorCode.NOT_SUPPORTED, PGErrorStatus.FEATURE_NOT_SUPPORTED);
                put(StandardErrorCode.INVALID_WINDOW_FRAME, PGErrorStatus.WINDOWING_ERROR);
                put(StandardErrorCode.NESTED_WINDOW, PGErrorStatus.WINDOWING_ERROR);
                put(StandardErrorCode.FUNCTION_NOT_WINDOW, PGErrorStatus.WINDOWING_ERROR);
                put(StandardErrorCode.CONSTRAINT_VIOLATION, PGErrorStatus.INTEGRITY_CONSTRAINT_VIOLATION);
                put(StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE, PGErrorStatus.NUMERIC_VALUE_OUT_OF_RANGE);
                put(StandardErrorCode.UNKNOWN_TRANSACTION, PGErrorStatus.TRANSACTION_RESOLUTION_UNKNOWN);
                put(StandardErrorCode.NOT_IN_TRANSACTION, PGErrorStatus.INVALID_TRANSACTION_STATE);
                put(StandardErrorCode.INCOMPATIBLE_CLIENT, PGErrorStatus.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION);
                put(StandardErrorCode.AMBIGUOUS_FUNCTION_CALL, PGErrorStatus.AMBIGUOUS_FUNCTION);
                put(StandardErrorCode.INVALID_SCHEMA_PROPERTY, PGErrorStatus.INVALID_SCHEMA_DEFINITION);
                put(StandardErrorCode.QUERY_TEXT_TOO_LARGE, PGErrorStatus.STATEMENT_TOO_COMPLEX);
                put(StandardErrorCode.UNSUPPORTED_SUBQUERY, PGErrorStatus.S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED);
                put(StandardErrorCode.EXCEEDED_FUNCTION_MEMORY_LIMIT, PGErrorStatus.PROGRAM_LIMIT_EXCEEDED);
                put(StandardErrorCode.INVALID_COLUMN_PROPERTY, PGErrorStatus.INVALID_COLUMN_DEFINITION);
                put(StandardErrorCode.TYPE_NOT_FOUND, PGErrorStatus.DATATYPE_MISMATCH);
                put(StandardErrorCode.TYPE_MISMATCH, PGErrorStatus.DATATYPE_MISMATCH);
                put(StandardErrorCode.SCHEMA_NOT_FOUND, PGErrorStatus.UNDEFINED_SCHEMA);
                put(StandardErrorCode.MISSING_SCHEMA_NAME, PGErrorStatus.UNDEFINED_SCHEMA);
                put(StandardErrorCode.TABLE_NOT_FOUND, PGErrorStatus.UNDEFINED_TABLE);
                put(StandardErrorCode.COLUMN_NOT_FOUND, PGErrorStatus.UNDEFINED_COLUMN);
                put(StandardErrorCode.MISSING_COLUMN_NAME, PGErrorStatus.UNDEFINED_COLUMN);
                put(StandardErrorCode.ROLE_NOT_FOUND, PGErrorStatus.INVALID_ROLE_SPECIFICATION);
                put(StandardErrorCode.ROLE_ALREADY_EXISTS, PGErrorStatus.INVALID_ROLE_SPECIFICATION);
                put(StandardErrorCode.SCHEMA_ALREADY_EXISTS, PGErrorStatus.DUPLICATE_SCHEMA);
                put(StandardErrorCode.TABLE_ALREADY_EXISTS, PGErrorStatus.DUPLICATE_TABLE);
                put(StandardErrorCode.COLUMN_ALREADY_EXISTS, PGErrorStatus.DUPLICATE_COLUMN);
                put(StandardErrorCode.DUPLICATE_COLUMN_NAME, PGErrorStatus.DUPLICATE_COLUMN);
                put(StandardErrorCode.DUPLICATE_NAMED_QUERY, PGErrorStatus.DUPLICATE_PSTATEMENT);
                put(StandardErrorCode.AMBIGUOUS_NAME, PGErrorStatus.AMBIGUOUS_COLUMN);
                put(StandardErrorCode.INVALID_COLUMN_REFERENCE, PGErrorStatus.INVALID_COLUMN_REFERENCE);
                put(StandardErrorCode.MISSING_GROUP_BY, PGErrorStatus.GROUPING_ERROR);
                put(StandardErrorCode.TOO_MANY_GROUPING_SETS, PGErrorStatus.GROUPING_ERROR);
                put(StandardErrorCode.TOO_MANY_ARGUMENTS, PGErrorStatus.TOO_MANY_ARGUMENTS);
                put(StandardErrorCode.NULL_TREATMENT_NOT_ALLOWED, PGErrorStatus.NULL_VALUE_NOT_ALLOWED);
            }
        }.build();

        public static PGErrorStatus from(ErrorCode errorCode)
        {
            if (MAP.containsKey(errorCode)) {
                return MAP.get(errorCode);
            }
            return MAP.get(UndefinedErrorCode.valueOf(errorCode.getType()).toErrorCode());
        }
    }
}
