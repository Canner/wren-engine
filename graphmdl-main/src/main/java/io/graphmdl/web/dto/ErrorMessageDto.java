/*
 * Copyright (C) Canner, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Canner dev team <contact@cannerdata.com>, Nov 2020
 */

package io.graphmdl.web.dto;

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
