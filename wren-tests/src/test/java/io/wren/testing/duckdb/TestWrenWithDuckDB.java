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

package io.wren.testing.duckdb;

import com.google.common.collect.ImmutableMap;
import io.wren.base.dto.Manifest;
import io.wren.main.web.dto.QueryResultDto;
import io.wren.testing.AbstractTestFramework;
import io.wren.testing.TestingWrenServer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static io.wren.base.Column.column;
import static io.wren.base.config.WrenConfig.DataSourceType.DUCKDB;
import static io.wren.base.config.WrenConfig.WREN_DATASOURCE_TYPE;
import static io.wren.base.config.WrenConfig.WREN_ENABLE_DYNAMIC_FIELDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class TestWrenWithDuckDB
        extends AbstractTestFramework
{
    private Manifest manifest;

    @Override
    protected TestingWrenServer createWrenServer()
            throws Exception
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put(WREN_DATASOURCE_TYPE, DUCKDB.name())
                .put(WREN_ENABLE_DYNAMIC_FIELDS, "true");

        try {
            manifest = MANIFEST_JSON_CODEC.fromJson(Files.readString(Path.of(getClass().getClassLoader().getResource("duckdb/mdl.json").getPath())));
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        return TestingWrenServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
    }

    @DataProvider
    public Object[][] queryModel()
    {
        return new Object[][] {
                {"SELECT * FROM Orders"},
                {"SELECT * FROM Orders WHERE orderkey > 100"},
                {"SELECT * FROM Orders a JOIN Customer b ON a.custkey = b.custkey"},
                {"SELECT * FROM Orders WHERE nation_name IS NOT NULL"},
                {"SELECT sum(orderkey) FROM Orders"}, // DuckDB always returns HUGEINT when aggregating integers
        };
    }

    @Test(dataProvider = "queryModel")
    public void testQueryModel(String sql)
    {
        assertThatNoException().isThrownBy(() -> {
            query(manifest, sql);
        });
    }

    @Test
    public void testQueryOnlyModelColumn()
    {
        QueryResultDto queryResultDto = query(manifest, "select * from Orders limit 100");
        assertThat(queryResultDto.getColumns())
                .isEqualTo(List.of(column("orderkey", "integer"),
                        column("custkey", "integer"),
                        column("orderstatus", "varchar"),
                        column("totalprice", "DECIMAL(15,2)"),
                        column("orderdate", "date")));
        assertThat(queryResultDto.getData().size()).isEqualTo(100);
    }

    @Test
    public void testQueryMetric()
    {
        // test the TO_ONE relationship
        QueryResultDto queryResultDto = query(manifest, "select customer, totalprice from Revenue limit 100");
        assertThat(queryResultDto.getColumns())
                .isEqualTo(List.of(column("customer", "varchar"), column("totalprice", "DECIMAL(38,2)")));
        assertThat(queryResultDto.getData().size()).isEqualTo(100);

        // test the TO_MANY relationship
        queryResultDto = query(manifest, "select custkey, totalprice from CustomerRevenue limit 100");
        assertThat(queryResultDto.getColumns())
                .isEqualTo(List.of(column("custkey", "integer"),
                        column("totalprice", "DECIMAL(38,2)")));
        assertThat(queryResultDto.getData().size()).isEqualTo(100);

        queryResultDto = query(manifest, "select customer, month, totalprice from CustomerMonthlyRevenue limit 100");
        assertThat(queryResultDto.getColumns())
                .isEqualTo(List.of(column("customer", "varchar"),
                        column("month", "date"),
                        column("totalprice", "DECIMAL(38,2)")));
        assertThat(queryResultDto.getData().size()).isEqualTo(100);
    }

    @Test
    public void testEnum()
    {
        QueryResultDto queryResultDto = query(manifest, "select Status.F as f1");
        assertThat(queryResultDto.getColumns())
                .isEqualTo(List.of(column("f1", "VARCHAR")));
        assertThat(queryResultDto.getData().get(0)[0]).isEqualTo("F");

        queryResultDto = query(manifest, "select count(*) as totalcount from Orders where orderstatus = Status.O");
        assertThat(queryResultDto.getColumns())
                .isEqualTo(List.of(column("totalcount", "BIGINT")));
        assertThat(queryResultDto.getData().get(0)[0]).isEqualTo(7333);
    }

    @Test
    public void testView()
    {
        QueryResultDto queryResultDto = query(manifest, "select * from useModel limit 100");
        assertThat(queryResultDto.getColumns())
                .isEqualTo(List.of(column("orderkey", "integer"),
                        column("custkey", "integer"),
                        column("orderstatus", "varchar"),
                        column("totalprice", "DECIMAL(15,2)"),
                        column("orderdate", "date")));
        assertThat(queryResultDto.getData().size()).isEqualTo(100);

        queryResultDto = query(manifest, "select * from useMetric limit 100");
        assertThat(queryResultDto.getColumns())
                .isEqualTo(List.of(column("customer", "varchar"),
                        column("totalprice", "DECIMAL(38,2)")));
        assertThat(queryResultDto.getData().size()).isEqualTo(100);

        queryResultDto = query(manifest, "select * from useUseMetric limit 100");
        assertThat(queryResultDto.getColumns())
                .isEqualTo(List.of(column("customer", "varchar"),
                        column("totalprice", "DECIMAL(38,2)")));
        assertThat(queryResultDto.getData().size()).isEqualTo(100);

        queryResultDto = query(manifest, "select * from sameCte limit 100");
        assertThat(queryResultDto.getColumns())
                .isEqualTo(List.of(column("orderkey", "integer"),
                        column("custkey", "integer"),
                        column("orderstatus", "varchar"),
                        column("totalprice", "DECIMAL(15,2)"),
                        column("orderdate", "date")));
        assertThat(queryResultDto.getData().size()).isEqualTo(100);
    }

    @Test
    public void testQuerySqlReservedWord()
    {
        QueryResultDto queryResultDto = query(manifest, "select \"order\" from Lineitem limit 100");
        assertThat(queryResultDto.getColumns())
                .isEqualTo(List.of(column("order", "integer")));
        assertThat(queryResultDto.getData().size()).isEqualTo(100);
    }

    @Test
    public void testQueryMacro()
    {
        QueryResultDto queryResultDto = query(manifest, "select custkey_name, custkey_call_concat from Customer limit 100");
        assertThat(queryResultDto.getColumns())
                .isEqualTo(List.of(column("custkey_name", "varchar"),
                        column("custkey_call_concat", "varchar")));
        assertThat(queryResultDto.getData().size()).isEqualTo(100);
    }

    @Test
    public void testQueryJoinAliased()
    {
        QueryResultDto queryResultDto = query(manifest, "select totalprice from (Orders o join Customer c on o.custkey = c.custkey) join_relation limit 100");
        assertThat(queryResultDto.getColumns())
                .isEqualTo(List.of(column("totalprice", "DECIMAL(15,2)")));
        assertThat(queryResultDto.getData().size()).isEqualTo(100);

        queryResultDto = query(manifest, "select totalprice from ((Orders o join Customer c on o.custkey = c.custkey) j join Lineitem l on j.orderkey = l.orderkey) limit 100");
        assertThat(queryResultDto.getColumns())
                .isEqualTo(List.of(column("totalprice", "DECIMAL(15,2)")));
        assertThat(queryResultDto.getData().size()).isEqualTo(100);
    }

    @Test
    public void testCountWithCalculatedFieldFilter()
    {
        QueryResultDto queryResultDto = query(manifest, "select count(*) from \"Orders\" where nation_name = 'ALGERIA'");
        assertThat(queryResultDto.getData().getFirst()[0]).isEqualTo(691);
    }

    @Test
    public void testCount()
    {
        QueryResultDto queryResultDto = query(manifest, "select count(*) from Orders a");
        assertThat(queryResultDto.getData().getFirst()[0]).isEqualTo(15000);

        queryResultDto = query(manifest, "select count(*) from Orders, Customer");
        assertThat(queryResultDto.getData().getFirst()[0]).isEqualTo(22500000);

        queryResultDto = query(manifest, "select count(*) from Orders a, Customer b");
        assertThat(queryResultDto.getData().getFirst()[0]).isEqualTo(22500000);

        queryResultDto = query(manifest, "select count(*) from Orders a JOIN Customer b ON a.custkey = b.custkey");
        assertThat(queryResultDto.getData().getFirst()[0]).isEqualTo(15000);

        queryResultDto = query(manifest, "select count(*) from (select * from Orders) t1");
        assertThat(queryResultDto.getData().getFirst()[0]).isEqualTo(15000);

        queryResultDto = query(manifest, "select count(*) from (Orders a JOIN Customer b ON a.custkey = b.custkey) t1");
        assertThat(queryResultDto.getData().getFirst()[0]).isEqualTo(15000);

        queryResultDto = query(manifest, "with t1 as (select * from Orders) select count(*) from t1");
        assertThat(queryResultDto.getData().getFirst()[0]).isEqualTo(15000);

        queryResultDto = query(manifest, "with t1 as (select * from Orders), t2 as (select count(*) from t1) select * from t2");
        assertThat(queryResultDto.getData().getFirst()[0]).isEqualTo(15000);

        queryResultDto = query(manifest, "with t1 as (select * from Orders) select * from (select count(*) from t1) s1");
        assertThat(queryResultDto.getData().getFirst()[0]).isEqualTo(15000);

        queryResultDto = query(manifest, "with t1 as (select * from Orders) select 15000 = (select count(*) from t1)");
        assertThat(queryResultDto.getData().getFirst()[0]).isEqualTo(true);

        queryResultDto = query(manifest, "with t1 as (select * from Orders) select 1 from Orders where 15000 = (select count(*) from t1)");
        assertThat(queryResultDto.getData().getFirst()[0]).isEqualTo(1);
    }

    @Test
    public void testSelectAllExcludeCalculatedField()
    {
        QueryResultDto queryResultDto = query(manifest, "select * from Orders limit 100");
        assertThat(queryResultDto.getColumns())
                .isEqualTo(List.of(column("orderkey", "INTEGER"),
                        column("custkey", "INTEGER"),
                        column("orderstatus", "VARCHAR"),
                        column("totalprice", "DECIMAL(15,2)"),
                        column("orderdate", "DATE")));
    }

    @Test
    public void testUnionDifferentModel()
    {
        QueryResultDto queryResultDto = query(manifest, "select custkey from Orders union select custkey from Customer limit 100");
        assertThat(queryResultDto.getColumns().size()).isEqualTo(1);
    }
}
