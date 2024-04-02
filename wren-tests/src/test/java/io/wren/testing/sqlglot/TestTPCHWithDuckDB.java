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
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.wren.main.sqlglot.SQLGlot.Dialect.DUCKDB;
import static org.assertj.core.api.Assertions.assertThatCode;

public class TestTPCHWithDuckDB
        extends AbstractTPCHTest
{
    private SQLGlotConverter sqlGlotConverter;

    @BeforeClass()
    public void setup()
    {
        super.init();
        super.prepareSQLGlot();

        sqlGlotConverter = SQLGlotConverter.builder()
                .setSQLGlot(sqlglot)
                .setWriteDialect(DUCKDB)
                .build();

        exec("create table orders as select * from '" + TPCH.ORDERS_PATH + "'");
        exec("create table lineitem as select * from '" + TPCH.LINEITEM_PATH + "'");
        exec("create table customer as select * from '" + TPCH.CUSTOMER_PATH + "'");
        exec("create table part as select * from '" + TPCH.PART_PATH + "'");
        exec("create table partsupp as select * from '" + TPCH.PARTSUPP_PATH + "'");
        exec("create table supplier as select * from '" + TPCH.SUPPLIER_PATH + "'");
        exec("create table nation as select * from '" + TPCH.NATION_PATH + "'");
        exec("create table region as select * from '" + TPCH.REGION_PATH + "'");
    }

    @Override
    protected WrenMDL buildWrenMDL()
    {
        return WrenMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(TPCH.getModels("main"))
                .build());
    }

    @Test(dataProvider = "queries")
    public void testQuery(String query)
    {
        String rewrite = wrenRewrite(query);
        @Language("SQL") String convert = sqlGlotConverter.convert(rewrite, DEFAULT_SESSION_CONTEXT);
        assertThatCode(() -> exec(convert)).doesNotThrowAnyException();
    }
}
