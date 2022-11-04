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

package io.cml.graphml.validation;

import javax.annotation.Nullable;

import java.time.Duration;

import static java.lang.String.format;

public class ValidationResult
{
    public static ValidationResult pass(String name, Duration duration)
    {
        return new ValidationResult(name, Status.PASS, duration, null);
    }

    public static ValidationResult error(String name, Duration duration, String message)
    {
        return new ValidationResult(name, Status.ERROR, duration, message);
    }

    public static ValidationResult fail(String name, Duration duration, String message)
    {
        return new ValidationResult(name, Status.FAIL, duration, message);
    }

    public static ValidationResult warn(String name, Duration duration, String message)
    {
        return new ValidationResult(name, Status.WARN, duration, message);
    }

    public enum Status
    {
        PASS,
        WARN,
        ERROR,
        FAIL,
        SKIP
    }

    private final String name;
    private final Status status;
    private final Duration duration;
    private final String message;

    public ValidationResult(String name, Status status, Duration duration, @Nullable String message)
    {
        this.name = name;
        this.status = status;
        this.duration = duration;
        this.message = message;
    }

    public String getName()
    {
        return name;
    }

    public static String formatRuleWithIdentifier(String ruleName, String modelName, String identifier)
    {
        return format("%s:%s:%s", ruleName, modelName, identifier);
    }

    public Status getStatus()
    {
        return status;
    }

    public Duration getDuration()
    {
        return duration;
    }

    @Nullable
    public String getMessage()
    {
        return message;
    }
}
