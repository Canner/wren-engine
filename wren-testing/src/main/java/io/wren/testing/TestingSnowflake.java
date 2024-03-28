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

import com.google.common.collect.ImmutableMap;
import com.snowflake.snowpark_java.DataFrame;
import com.snowflake.snowpark_java.Session;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import static java.lang.System.getenv;

public class TestingSnowflake
        implements Closeable
{
    private final Session session;

    public TestingSnowflake()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("URL", getenv("SNOWFLAKE_URL"))
                .put("USER", getenv("SNOWFLAKE_USER"))
                .put("PASSWORD", getenv("SNOWFLAKE_PASSWORD"))
                .put("DB", "SNOWFLAKE_SAMPLE_DATA")
                .put("SCHEMA", "TPCH_SF1")
                .build();
        session = Session.builder().configs(properties).create();
    }

    public void exec(String sql)
    {
        DataFrame df = session.sql(sql);
        df.collect();
    }

    @Override
    public void close()
            throws IOException
    {
        session.close();
    }
}
