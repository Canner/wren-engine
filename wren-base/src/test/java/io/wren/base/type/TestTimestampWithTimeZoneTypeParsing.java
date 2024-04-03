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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.wren.base.type;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThatNoException;

public class TestTimestampWithTimeZoneTypeParsing
{
    @DataProvider
    public Object[][] timestampWithTimeZone()
    {
        return new Object[][] {
                {"2004-10-19 10:23:54 +02"},
                {"1999-01-08 04:05:06 PST"},
                {"1999-01-08 04:05:06 PST8PDT"},
                {"1999-01-08 04:05:06 zulu"},
                {"1999-01-08 04:05:06 z"},
                {"1999-01-08 04:05:06 America/New_York"},
                {"2004-10-19 10:23:54+02"},
                {"1999-01-08 04:05:06PST"},
                {"1999-01-08 04:05:06PST8PDT"},
                {"1999-01-08 04:05:06zulu"},
                {"1999-01-08 04:05:06z"},

                // TODO: unsupported pg pattern
                // {"1999-01-08 04:05:06 -8:00"},
                // {"1999-01-08 04:05:06-8:00"},
                // {"1999-01-08 04:05:06 -8:00:00"},
                // {"1999-01-08 04:05:06 -8:00"},
                // {"1999-01-08 04:05:06 -800"},
                // {"1999-01-08 04:05:06-8:00:00"},
                // {"1999-01-08 04:05:06-8:00"},
                // {"1999-01-08 04:05:06-800"},
                // {"1999-01-08 04:05:06-8"},
                // {"1999-01-08 04:05:06 -8"},
                // {"1999-01-08 04:05:06Americ/New_York"},
        };
    }

    @Test(dataProvider = "timestampWithTimeZone")
    public void testParsing(String timestampString)
    {
        assertThatNoException()
                .isThrownBy(() -> TimestampWithTimeZoneType.PG_TIMESTAMP.parse(timestampString));
    }
}
