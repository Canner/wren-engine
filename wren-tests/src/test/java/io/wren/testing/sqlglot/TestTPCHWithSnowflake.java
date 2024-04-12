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

package io.wren.testing.sqlglot;

import io.wren.base.WrenMDL;
import io.wren.base.config.SnowflakeConfig;
import io.wren.main.connector.snowflake.SnowflakeClient;
import io.wren.main.sqlglot.SQLGlotConverter;
import io.wren.testing.TPCH;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.wren.main.sqlglot.SQLGlot.Dialect.SNOWFLAKE;
import static java.lang.System.getenv;
import static org.assertj.core.api.Assertions.assertThatCode;

@Test
public class TestTPCHWithSnowflake
        extends AbstractTPCHTest
{
    private SQLGlotConverter sqlGlotConverter;
    private SnowflakeClient snowflakeClient;

    @BeforeClass
    public void setup()
    {
        super.init();
        super.prepareSQLGlot();

        sqlGlotConverter = SQLGlotConverter.builder()
                .setSQLGlot(sqlglot)
                .setWriteDialect(SNOWFLAKE)
                .build();

        SnowflakeConfig config = new SnowflakeConfig()
                .setJdbcUrl(getenv("SNOWFLAKE_JDBC_URL"))
                .setUser(getenv("SNOWFLAKE_USER"))
                .setPassword(getenv("SNOWFLAKE_PASSWORD"))
                .setDatabase("SNOWFLAKE_SAMPLE_DATA")
                .setSchema("TPCH_SF1");

        snowflakeClient = new SnowflakeClient(config);
    }

    @Override
    protected WrenMDL buildWrenMDL()
    {
        return WrenMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(TPCH.getModels("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1"))
                .build());
    }

    @Test(dataProvider = "queries")
    public void testQuery(String query)
    {
        String rewrite = wrenRewrite(query);
        @Language("SQL") String convert = sqlGlotConverter.convert(rewrite, DEFAULT_SESSION_CONTEXT);
        assertThatCode(() -> snowflakeClient.execute(convert)).doesNotThrowAnyException();
    }
}
