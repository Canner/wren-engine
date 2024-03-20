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

package io.wren.base.dto;

import static java.util.Locale.ENGLISH;

public enum TimeUnit
{
    YEAR("INTERVAL '1 YEAR'"),
    QUARTER("INTERVAL '3 MONTH'"),
    MONTH("INTERVAL '1 MONTH'"),
    WEEK("INTERVAL '7 DAY'"),
    DAY("INTERVAL '1 DAY'"),
    HOUR("INTERVAL '1 HOUR'"),
    MINUTE("INTERVAL '1 MINUTE'"),
    SECOND("INTERVAL '1 SECOND'");

    private final String intervalExpression;

    TimeUnit(String intervalExpression)
    {
        this.intervalExpression = intervalExpression;
    }

    public String getIntervalExpression()
    {
        return intervalExpression;
    }

    public static TimeUnit timeUnit(String name)
    {
        return valueOf(name.toUpperCase(ENGLISH));
    }
}
