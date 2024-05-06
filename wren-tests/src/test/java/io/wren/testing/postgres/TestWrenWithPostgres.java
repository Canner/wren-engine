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

package io.wren.testing.postgres;

import com.google.common.collect.ImmutableList;
import io.wren.base.WrenTypes;
import io.wren.base.client.duckdb.DuckDBConfig;
import io.wren.base.client.duckdb.DuckDBSettingSQL;
import io.wren.base.client.duckdb.DuckdbClient;
import io.wren.base.dto.Column;
import io.wren.base.dto.Manifest;
import io.wren.base.dto.Model;
import io.wren.main.web.dto.PreviewDto;
import org.testng.annotations.Test;

import java.util.List;

import static io.wren.base.dto.Column.caluclatedColumn;
import static io.wren.base.dto.Column.column;
import static io.wren.base.dto.TableReference.tableReference;
import static io.wren.testing.WebApplicationExceptionAssert.assertWebApplicationException;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class TestWrenWithPostgres
        extends AbstractWireProtocolTestWithPostgres
{
    private final Model customer = Model.onTableReference("Customer",
            tableReference("tpch", "tpch", "customer"),
            List.of(
                    column("custkey", WrenTypes.INTEGER, null, true, "c_custkey"),
                    column("name", WrenTypes.VARCHAR, null, true, "c_name"),
                    column("address", WrenTypes.VARCHAR, null, true, "c_address"),
                    column("nationkey", WrenTypes.INTEGER, null, true, "c_nationkey"),
                    column("phone", WrenTypes.VARCHAR, null, true, "c_phone"),
                    column("acctbal", WrenTypes.INTEGER, null, true, "c_acctbal"),
                    column("mktsegment", WrenTypes.VARCHAR, null, true, "c_mktsegment"),
                    column("comment", WrenTypes.VARCHAR, null, true, "c_comment")),
            "custkey");

    @Override
    protected void prepareData()
    {
        String customer = requireNonNull(getClass().getClassLoader().getResource("tpch/data/customer.parquet")).getPath();
        injectTableToPostgres(List.of(new TestingData("customer", customer)));
    }

    private void injectTableToPostgres(List<TestingData> dataList)
    {
        DuckDBConfig duckDBConfig = new DuckDBConfig();
        DuckDBSettingSQL duckDBSettingSQL = new DuckDBSettingSQL();

        String init = format("""
                        INSTALL postgres;
                        LOAD postgres;
                        ATTACH 'dbname=%s user=%s host=%s password=%s port=%s' AS db (TYPE POSTGRES);
                        """,
                testingPostgreSqlServer.getDatabase(),
                testingPostgreSqlServer.getUser(),
                testingPostgreSqlServer.getHost(),
                testingPostgreSqlServer.getPassword(),
                testingPostgreSqlServer.getPort());
        duckDBSettingSQL.setInitSQL(init);
        duckDBConfig.setHomeDirectory("/tmp");
        DuckdbClient duckdbClient = DuckdbClient.builder()
                .setCacheStorageConfig(null)
                .setDuckDBConfig(duckDBConfig)
                .setDuckDBSettingSQL(duckDBSettingSQL)
                .build();
        try {
            for (TestingData data : dataList) {
                duckdbClient.executeDDL(format("create table db.tpch.%s as select * from '%s'", data.tableName, data.resourcePath));
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            duckdbClient.close();
        }
    }

    private record TestingData(String tableName, String resourcePath) {}

    @Test
    public void testCalculatedScope()
    {
        // The scope of calculated field is the normal columns of the same model
        assertThatNoException().isThrownBy(() -> preview(new PreviewDto(Manifest.builder()
                .setCatalog("wren")
                .setSchema("tpch")
                .setModels(List.of(addColumnsToModel(customer, caluclatedColumn("name_address", WrenTypes.VARCHAR, "concat(name, '_', address)"))))
                .build(),
                "select name_address from \"Customer\"", 100L)));

        assertWebApplicationException(() -> preview(new PreviewDto(Manifest.builder()
                .setCatalog("wren")
                .setSchema("tpch")
                .setModels(List.of(addColumnsToModel(customer, caluclatedColumn("name_address", WrenTypes.VARCHAR, "concat(name, '_', address)"),
                        caluclatedColumn("name_name_address", WrenTypes.VARCHAR, "concat(name, '_', name_address)"))))
                .build(),
                "select name_name_address from \"Customer\"", 100L)))
                .hasHTTPStatus(500)
                .hasErrorMessageMatches(".*column \"name_address\" does not exist(.|\\n)*");

        // The scope of the normal column is the columns of the native table.
        assertWebApplicationException(() -> preview(new PreviewDto(Manifest.builder()
                .setCatalog("wren")
                .setSchema("tpch")
                .setModels(List.of(addColumnsToModel(customer, column("name_address", WrenTypes.VARCHAR, null, false, "concat(name, '_', address)"))))
                .build(),
                "select name_address from \"Customer\"", 100L)))
                .hasHTTPStatus(500)
                .hasErrorMessageMatches(".*column \"name\" does not exist(.|\\n)*");
    }

    private static Model addColumnsToModel(Model model, Column... columns)
    {
        return new Model(
                model.getName(),
                model.getRefSql(),
                model.getBaseObject(),
                model.getTableReference(),
                ImmutableList.<Column>builder()
                        .addAll(model.getColumns())
                        .add(columns)
                        .build(),
                model.getPrimaryKey(),
                model.isCached(),
                model.getRefreshTime(),
                model.getProperties());
    }
}
