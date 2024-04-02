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

package io.wren.base;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Objects;

import static java.lang.Character.MAX_RADIX;
import static java.lang.Math.abs;
import static java.lang.Math.min;

public class Utils
{
    private Utils() {}

    private static final SecureRandom random = new SecureRandom();
    private static final int RANDOM_SUFFIX_LENGTH = 10;

    public static void checkArgument(boolean expression, String errorMessagePattern, Object... errorMessageArgs)
    {
        if (!expression) {
            throw new IllegalArgumentException(String.format(errorMessagePattern, errorMessageArgs));
        }
    }

    public static String requireNonNullEmpty(String value, String errorMessage)
    {
        checkArgument(value != null && !value.isEmpty(), errorMessage);
        return value;
    }

    public static String randomIntString()
    {
        String randomSuffix = Long.toString(abs(random.nextLong()), MAX_RADIX);
        return randomSuffix.substring(0, min(RANDOM_SUFFIX_LENGTH, randomSuffix.length()));
    }

    public static <T> T firstNonNull(T... objects)
    {
        return Arrays.stream(objects).filter(Objects::nonNull).findFirst().orElse(null);
    }
}
