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

package io.wren.testing.snowflake;

import com.google.common.io.Closer;
import io.wren.base.SessionContext;
import io.wren.base.config.SQLGlotConfig;
import io.wren.main.connector.snowflake.SnowflakeSqlConverter;
import io.wren.main.sqlglot.SQLGlot;
import io.wren.testing.AbstractSqlConverterTest;
import io.wren.testing.TestingSQLGlotServer;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.wren.base.config.SQLGlotConfig.createConfigWithFreePort;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSnowflakeSqlConverter
        extends AbstractSqlConverterTest
{
    private static final SessionContext DEFAULT_SESSION_CONTEXT = SessionContext.builder().build();

    private SnowflakeSqlConverter sqlConverter;
    private final Closer closer = Closer.create();

    @BeforeClass
    public void setup()
    {
        SQLGlotConfig config = createConfigWithFreePort();
        closer.register(new TestingSQLGlotServer(config));
        SQLGlot sqlGlot = closer.register(new SQLGlot(config));
        sqlConverter = new SnowflakeSqlConverter(sqlGlot);
    }

    @AfterClass
    public void tearDown()
            throws Exception
    {
        closer.close();
    }

    @Test
    public void testRewriteCastStringAsBytea()
    {
        assertRewrite("SELECT CAST('123' AS BYTEA)",
                "SELECT CAST(HEX_ENCODE('123') AS VARBINARY)");

        assertRewrite("SELECT CAST(ARRAY['123'] AS ARRAY(BYTEA))",
                "SELECT CAST([HEX_ENCODE('123')] AS ARRAY(VARBINARY))");

        // We don't rewrite expression of cast is function call
        assertRewrite("SELECT CAST(REVERSE('123') AS BYTEA)",
                "SELECT CAST(REVERSE('123') AS VARBINARY)");
    }

    private void assertRewrite(@Language("sql") String sql, @Language("sql") String expected)
    {
        assertThat(sqlConverter.convert(sql, DEFAULT_SESSION_CONTEXT)).isEqualTo(expected);
    }
}
