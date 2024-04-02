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
import io.wren.main.sqlglot.SQLGlotConverter;
import io.wren.testing.TPCH;
import io.wren.testing.TestingSnowflake;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static io.wren.main.sqlglot.SQLGlot.Dialect.SNOWFLAKE;
import static org.assertj.core.api.Assertions.assertThatCode;

@Test(singleThreaded = true, enabled = false, description = "It requires a Snowflake account.")
public class TestTPCHWithSnowflake
        extends AbstractTPCHTest
{
    private SQLGlotConverter sqlGlotConverter;
    private TestingSnowflake testingSnowflake;

    @BeforeClass
    public void setup()
    {
        super.init();
        super.prepareSQLGlot();

        sqlGlotConverter = SQLGlotConverter.builder()
                .setSQLGlot(sqlglot)
                .setWriteDialect(SNOWFLAKE)
                .build();

        testingSnowflake = new TestingSnowflake();
    }

    @Override
    protected WrenMDL buildWrenMDL()
    {
        return WrenMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(TPCH.getModels("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1"))
                .build());
    }

    @Override
    protected void cleanup()
    {
        super.cleanup();
        try {
            testingSnowflake.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test(dataProvider = "queries")
    public void testQuery(String query)
    {
        String rewrite = wrenRewrite(query);
        @Language("SQL") String convert = sqlGlotConverter.convert(rewrite, DEFAULT_SESSION_CONTEXT);
        assertThatCode(() -> testingSnowflake.exec(convert)).doesNotThrowAnyException();
    }
}
