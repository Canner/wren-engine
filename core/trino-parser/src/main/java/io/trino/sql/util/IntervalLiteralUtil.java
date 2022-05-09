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

package io.trino.sql.util;

import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.NodeLocation;

import java.util.Locale;
import java.util.Optional;

import static io.trino.sql.tree.IntervalLiteral.Sign.NEGATIVE;
import static io.trino.sql.tree.IntervalLiteral.Sign.POSITIVE;

public final class IntervalLiteralUtil
{
    private IntervalLiteralUtil() {}

    /**
     * we use this method to parse PostgreSQL style interval string to IntervalLiteral.
     * <p>
     * e.g.
     * when client sends a query like
     * select CAST((CAST(now() AS timestamp) + (INTERVAL '-30 day')) AS date);
     * will get the same result as
     * select CAST((CAST(now() AS timestamp) + (INTERVAL - '30' day)) AS date);
     */
    public static IntervalLiteral parse(NodeLocation location, String text)
    {
        String[] strings = text.split(" ");
        String value = strings[0].replaceFirst("[-+]", "");
        IntervalLiteral.Sign sign = (strings[0].startsWith("-")) ? NEGATIVE : POSITIVE;
        IntervalLiteral.IntervalField field = IntervalLiteral.IntervalField.valueOf(strings[1].toUpperCase(Locale.ROOT));

        return new IntervalLiteral(location, value, sign, field, Optional.empty());
    }
}
