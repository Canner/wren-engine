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
package io.wren.testing;

import io.wren.base.metadata.StandardErrorCode;
import io.wren.main.web.dto.ErrorMessageDto;
import jakarta.ws.rs.WebApplicationException;
import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.ThrowableAssert;
import org.intellij.lang.annotations.Language;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public final class WebApplicationExceptionAssert
        extends AbstractThrowableAssert<WebApplicationExceptionAssert, WebApplicationException>
{
    public static WebApplicationExceptionAssert assertWebApplicationException(ThrowableAssert.ThrowingCallable throwingCallable)
    {
        Throwable throwable = catchThrowable(throwingCallable);
        if (throwable == null) {
            failBecauseExceptionWasNotThrown(WebApplicationException.class);
        }
        assertThat(throwable).isInstanceOf(WebApplicationException.class);
        return new WebApplicationExceptionAssert((WebApplicationException) throwable);
    }

    private WebApplicationExceptionAssert(WebApplicationException actual)
    {
        super(actual, WebApplicationExceptionAssert.class);
    }

    public WebApplicationExceptionAssert hasHTTPStatus(int code)
    {
        try {
            assertThat(actual.getResponse().getStatus()).isEqualTo(code);
        }
        catch (AssertionError e) {
            e.addSuppressed(actual);
            throw e;
        }
        return myself;
    }

    public WebApplicationExceptionAssert hasErrorCode(StandardErrorCode errorCode)
    {
        try {
            Object responseEntity = actual.getResponse().getEntity();
            assertThat(responseEntity).isInstanceOf(ErrorMessageDto.class);
            assertThat(((ErrorMessageDto) responseEntity).getCode()).isEqualTo(errorCode.name());
        }
        catch (AssertionError e) {
            e.addSuppressed(actual);
            throw e;
        }
        return myself;
    }

    public WebApplicationExceptionAssert hasErrorMessageMatches(@Language("RegExp") String regex)
    {
        try {
            Object responseEntity = actual.getResponse().getEntity();
            assertThat(responseEntity).isInstanceOf(ErrorMessageDto.class);
            assertThat(((ErrorMessageDto) responseEntity).getMessage()).matches(regex);
        }
        catch (AssertionError e) {
            e.addSuppressed(actual);
            throw e;
        }
        return myself;
    }
}
