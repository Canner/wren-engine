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

package io.wren.main.web.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ErrorMessageDto
{
    private String code;
    private String message;

    @JsonCreator
    public ErrorMessageDto(@JsonProperty("code") String code, @JsonProperty("message") String message)
    {
        this.code = code;
        this.message = message;
    }

    @JsonProperty
    public String getCode()
    {
        return code;
    }

    public void setCode(String code)
    {
        this.code = code;
    }

    @JsonProperty
    public String getMessage()
    {
        return message;
    }

    public void setMessage(String message)
    {
        this.message = message;
    }

    @Override
    public boolean equals(Object that)
    {
        if (this == that) {
            return true;
        }
        if (that == null || getClass() != that.getClass()) {
            return false;
        }
        ErrorMessageDto errorMessageDto = (ErrorMessageDto) that;
        return Objects.equals(code, errorMessageDto.code) &&
                Objects.equals(message, errorMessageDto.message);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(code, message);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("code", code)
                .add("message", message)
                .toString();
    }
}
