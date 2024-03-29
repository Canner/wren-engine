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

import io.wren.base.AnalyzedMDL;
import io.wren.base.WrenMDL;
import io.wren.base.sqlrewrite.WrenPlanner;
import io.wren.testing.AbstractTestFramework;
import io.wren.testing.TPCH;
import io.wren.testing.TestingSQLGlotServer;
import org.testng.annotations.DataProvider;

public abstract class AbstractTPCHTest
        extends AbstractTestFramework
{
    private final TestingSQLGlotServer testingSQLGlotServer = new TestingSQLGlotServer();

    private final WrenMDL wrenMDL;

    protected AbstractTPCHTest()
    {
        this.wrenMDL = buildWrenMDL();
    }

    protected abstract WrenMDL buildWrenMDL();

    @Override
    protected void cleanup()
    {
        testingSQLGlotServer.close();
    }

    @DataProvider
    protected Object[][] queries()
    {
        return TPCH.QUERIES.values().stream()
                .map(query -> new Object[] {query})
                .toArray(Object[][]::new);
    }

    protected String wrenRewrite(String sql)
    {
        return WrenPlanner.rewrite(sql, DEFAULT_SESSION_CONTEXT, new AnalyzedMDL(wrenMDL, null));
    }
}
