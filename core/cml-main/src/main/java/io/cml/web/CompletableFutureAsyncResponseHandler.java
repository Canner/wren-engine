package io.cml.web;

import io.airlift.log.Logger;
import io.cml.spi.CmlException;
import io.cml.web.dto.ErrorMessageDto;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import java.util.concurrent.CompletionException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static io.cml.spi.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.cml.spi.metadata.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

public final class CompletableFutureAsyncResponseHandler
        implements ExceptionMapper<Throwable>
{
    private static final Logger LOG = Logger.get(CompletableFutureAsyncResponseHandler.class);

    public static <T> void bindAsyncResponse(AsyncResponse asyncResponse, Supplier<T> supplier)
    {
        T result = null;
        Throwable throwable = null;
        try {
            result = supplier.get();
        }
        catch (Throwable ex) {
            throwable = ex;
        }
        bindAsyncResponse(asyncResponse).accept(result, throwable);
    }

    public static BiConsumer<? super Object, ? super Throwable> bindAsyncResponse(AsyncResponse asyncResponse)
    {
        return (response, throwable) -> {
            if (throwable != null) {
                LOG.error(throwable);
                asyncResponse.resume(throwable);
            }
            else if (response instanceof Response) {
                asyncResponse.resume(response);
            }
            else {
                asyncResponse.resume(Response.ok(response).build());
            }
        };
    }

    @Override
    public Response toResponse(Throwable throwable)
    {
        LOG.warn(throwable, format("Exception, type: %s, message: %s", throwable.getClass(), throwable.getMessage()));
        if (throwable instanceof CmlException) {
            return failure((CmlException) throwable);
        }
        else if (throwable instanceof CompletionException && throwable.getCause() instanceof CmlException) {
            return failure((CmlException) throwable.getCause());
        }
        else {
            return Response
                    .status(INTERNAL_SERVER_ERROR)
                    .type(APPLICATION_JSON)
                    .entity(new ErrorMessageDto(GENERIC_INTERNAL_ERROR.name(), throwable.getMessage()))
                    .build();
        }
    }

    private static Response failure(CmlException exception)
    {
        switch (exception.getErrorCode().getType()) {
            case USER_ERROR:
                if (exception.getErrorCode().equals(NOT_FOUND.toErrorCode())) {
                    return Response
                            .status(Response.Status.NOT_FOUND)
                            .type(APPLICATION_JSON)
                            .entity(createErrorMessageDto(exception))
                            .build();
                }
                return Response
                        .status(BAD_REQUEST)
                        .type(APPLICATION_JSON)
                        .entity(createErrorMessageDto(exception))
                        .build();
            case EXTERNAL:
            case INTERNAL_ERROR:
            case INSUFFICIENT_RESOURCES:
            default:
                return Response
                        .status(INTERNAL_SERVER_ERROR)
                        .type(APPLICATION_JSON)
                        .entity(createErrorMessageDto(exception))
                        .build();
        }
    }

    private static ErrorMessageDto createErrorMessageDto(CmlException e)
    {
        return new ErrorMessageDto(e.getErrorCode().getName(), e.getMessage());
    }
}
