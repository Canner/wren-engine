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

package io.graphmdl.base;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Objects;

public class Utils
{
    private Utils() {}

    private static final SecureRandom random = new SecureRandom();

    public static void checkArgument(boolean expression, String errorMessage)
    {
        if (!expression) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    public static String randomIntString()
    {
        return Integer.toString(random.nextInt());
    }

    public static <T> T firstNonNull(T... objects)
    {
        return Arrays.stream(objects).filter(Objects::nonNull).findFirst().orElse(null);
    }
}
