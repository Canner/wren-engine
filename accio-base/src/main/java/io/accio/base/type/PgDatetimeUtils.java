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

package io.accio.base.type;

public final class PgDatetimeUtils
{
    private PgDatetimeUtils() {}

    public static final int SECS_PER_DAY = 86400;
    // amount of seconds between 1970-01-01 and 2000-01-01
    public static final int EPOCH_DIFF_IN_SEC = 946_684_800;
    public static final long EPOCH_DIFF_IN_MS = EPOCH_DIFF_IN_SEC * 1000L;
    public static final int EPOCH_DIFF_IN_DAY = EPOCH_DIFF_IN_SEC / SECS_PER_DAY;

    /**
     * Convert a trino date (unix timestamp in day) into a postgres date
     * (int days since 2000-01-01)
     */
    public static int toPgDate(int unixTsInDay)
    {
        return (unixTsInDay - EPOCH_DIFF_IN_DAY);
    }

    /**
     * Convert a postgres date (days since 2000-01-01) into a trino
     * date (unix timestamp in days).
     */
    public static int toTrinoDate(int daysSince2k)
    {
        return daysSince2k + EPOCH_DIFF_IN_DAY;
    }

    /**
     * Convert a presto timestamp (unix timestamp in ms) into a postgres timestamp
     * (long microseconds since 2000-01-01)
     */
    public static long toPgTimestamp(long unixTsInMs)
    {
        return (unixTsInMs - EPOCH_DIFF_IN_MS) * 1000;
    }

    /**
     * Convert a postgres timestamp (seconds since 2000-01-01) into a presto
     * timestamp (unix timestamp in ms).
     */
    public static long toTrinoTimestamp(long microSecondsSince2k)
    {
        return (microSecondsSince2k / 1000) + EPOCH_DIFF_IN_MS;
    }
}
