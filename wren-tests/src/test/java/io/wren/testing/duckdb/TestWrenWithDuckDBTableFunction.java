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
import io.wren.base.dto.Model;
import io.wren.main.web.dto.PreviewDto;
import io.wren.main.web.dto.QueryResultDto;
import io.wren.testing.RequireWrenServer;
import io.wren.testing.TestingWrenServer;
import org.testng.annotations.Test;

import java.util.List;

import static io.wren.base.config.WrenConfig.DataSourceType.DUCKDB;
import static io.wren.base.config.WrenConfig.WREN_DATASOURCE_TYPE;
import static io.wren.base.config.WrenConfig.WREN_ENABLE_DYNAMIC_FIELDS;
import static io.wren.base.dto.Column.column;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestWrenWithDuckDBTableFunction
        extends RequireWrenServer
{
    @Override
    protected TestingWrenServer createWrenServer()
            throws Exception
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put(WREN_DATASOURCE_TYPE, DUCKDB.name())
                .put(WREN_ENABLE_DYNAMIC_FIELDS, "true");
        TestingWrenServer testing = TestingWrenServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
        return testing;
    }

    @Test
    public void testQueryFile()
    {
        String url = requireNonNull(getClass().getClassLoader().getResource("tpch/data/orders.parquet")).getPath();
        Manifest manifest = Manifest.builder()
                .setCatalog("wren")
                .setSchema("test")
                .setModels(List.of(Model.model(
                        "orders",
                        format("select * from read_parquet('%s')", url),
                        List.of(column("orderkey", "integer", null, false, "o_orderkey"),
                                column("custkey", "integer", null, false, "o_custkey")))))
                .build();

        PreviewDto testDefaultDto = new PreviewDto(manifest, "select custkey from orders", null);
        QueryResultDto testDefault = preview(testDefaultDto);
        assertThat(testDefault.getData().size()).isEqualTo(100);
        assertThat(testDefault.getColumns().size()).isEqualTo(1);
        assertThat(testDefault.getColumns().get(0).getName()).isEqualTo("custkey");
        assertThat(testDefault.getColumns().get(0).getType()).isEqualTo("INTEGER");
    }

    @Test
    public void testQueryFileWithParam()
    {
        String url = requireNonNull(getClass().getClassLoader().getResource("csv/orders/orders.csv")).getPath();
        Manifest manifest = Manifest.builder()
                .setCatalog("wren")
                .setSchema("test")
                .setModels(List.of(Model.model(
                        "orders",
                        format("select * from read_csv('%s', header = true)", url),
                        List.of(column("order_id", "varchar", null, false),
                                column("customer_id", "varchar", null, false)))))
                .build();

        PreviewDto testDefaultDto = new PreviewDto(manifest, "select order_id from orders", null);
        QueryResultDto testDefault = preview(testDefaultDto);
        assertThat(testDefault.getData().size()).isEqualTo(99);
        assertThat(testDefault.getColumns().size()).isEqualTo(1);
        assertThat(testDefault.getColumns().get(0).getName()).isEqualTo("order_id");
        assertThat(testDefault.getColumns().get(0).getType()).isEqualTo("VARCHAR");

        String folder = requireNonNull(getClass().getClassLoader().getResource("csv/orders")).getPath();
        manifest = Manifest.builder()
                .setCatalog("wren")
                .setSchema("test")
                .setModels(List.of(Model.model(
                        "orders",
                        format("select * from read_csv(['%s', '%s'], header = true)", folder + "/orders.csv", folder + "/orders-2.csv"),
                        List.of(column("order_id", "varchar", null, false),
                                column("customer_id", "varchar", null, false)))))
                .build();

        testDefaultDto = new PreviewDto(manifest, "select order_id from orders", 200L);
        testDefault = preview(testDefaultDto);
        assertThat(testDefault.getData().size()).isEqualTo(181);
        assertThat(testDefault.getColumns().size()).isEqualTo(1);
        assertThat(testDefault.getColumns().get(0).getName()).isEqualTo("order_id");
        assertThat(testDefault.getColumns().get(0).getType()).isEqualTo("VARCHAR");
    }

    @Test
    public void testQueryWithFromStringLiteral()
    {
        String url = requireNonNull(getClass().getClassLoader().getResource("csv/orders/orders.csv")).getPath();
        Manifest manifest = Manifest.builder()
                .setCatalog("wren")
                .setSchema("test")
                .setModels(List.of(Model.model(
                        "orders",
                        format("select * from '%s'", url),
                        List.of(column("order_id", "varchar", null, false),
                                column("customer_id", "varchar", null, false)))))
                .build();

        PreviewDto testDefaultDto = new PreviewDto(manifest, "select order_id from orders", null);
        QueryResultDto testDefault = preview(testDefaultDto);
        assertThat(testDefault.getData().size()).isEqualTo(99);
        assertThat(testDefault.getColumns().size()).isEqualTo(1);
        assertThat(testDefault.getColumns().get(0).getName()).isEqualTo("order_id");
        assertThat(testDefault.getColumns().get(0).getType()).isEqualTo("VARCHAR");
    }

    @Test
    public void testQueryNestedType()
    {
        setDuckDBInitSQL("create table nested_table as select * from (values ({'f1':'2'})) t(a1)");
        Manifest manifest = Manifest.builder()
                .setCatalog("wren")
                .setSchema("public")
                .setModels(List.of(
                        Model.model(
                                "nested_table",
                                "select * from main.nested_table",
                                List.of(column("a1", "array<int>", null, false)))))
                .build();

        PreviewDto previewDto = new PreviewDto(manifest, "select a1.f1 from nested_table", null);
        QueryResultDto testDefault = preview(previewDto);
        assertThat(testDefault.getColumns().get(0).getType()).isEqualTo("VARCHAR");
        assertThat(testDefault.getData().get(0)[0]).isEqualTo("2");

        setDuckDBInitSQL("create table nested_table as select * from (values ([{'f1':'2'}, {'f1':'3'}, {'f1':'4'}])) t(a1)");
        manifest = Manifest.builder()
                .setCatalog("wren")
                .setSchema("public")
                .setModels(List.of(
                        Model.model(
                                "nested_table",
                                "select * from main.nested_table",
                                List.of(column("a1", "array<struct<f1, varchar>>", null, false)))))
                .build();

        previewDto = new PreviewDto(manifest, "select a1[1].f1 from nested_table", null);
        testDefault = preview(previewDto);
        assertThat(testDefault.getColumns().get(0).getType()).isEqualTo("VARCHAR");
        assertThat(testDefault.getData().get(0)[0]).isEqualTo("2");

        setDuckDBInitSQL("create table nested_table as select * from (values ([1,2,3])) t(a1)");

        previewDto = new PreviewDto(manifest, "select * from nested_table n, unnest(n.a1)", null);
        testDefault = preview(previewDto);
        assertThat(testDefault.getColumns().get(0).getType()).isEqualTo("INTEGER[]");
        assertThat(testDefault.getData().get(0).length).isEqualTo(2);
        assertThat(testDefault.getData().size()).isEqualTo(3);

        previewDto = new PreviewDto(manifest, "select * from nested_table n, unnest(n.a1) u(a1)", null);
        testDefault = preview(previewDto);
        assertThat(testDefault.getColumns().get(0).getType()).isEqualTo("INTEGER[]");
        assertThat(testDefault.getData().get(0).length).isEqualTo(2);
        assertThat(testDefault.getData().size()).isEqualTo(3);

        previewDto = new PreviewDto(manifest, "select u.a1 from nested_table n, unnest(n.a1) u(a1)", null);
        testDefault = preview(previewDto);
        assertThat(testDefault.getColumns().get(0).getType()).isEqualTo("INTEGER");
        assertThat(testDefault.getData().get(0)[0]).isEqualTo(1);
        assertThat(testDefault.getData().get(1)[0]).isEqualTo(2);
        assertThat(testDefault.getData().get(2)[0]).isEqualTo(3);

        previewDto = new PreviewDto(manifest, "select * from nested_table n cross join lateral (select n.a1[1])", null);
        testDefault = preview(previewDto);
        assertThat(testDefault.getData().get(0).length).isEqualTo(2);
        assertThat(testDefault.getData().size()).isEqualTo(1);

        previewDto = new PreviewDto(manifest, "select * from nested_table n cross join lateral (select n.a1[1]) l(a1)", null);
        testDefault = preview(previewDto);
        assertThat(testDefault.getData().get(0).length).isEqualTo(2);
        assertThat(testDefault.getData().size()).isEqualTo(1);

        previewDto = new PreviewDto(manifest, "select l.a1 from nested_table n cross join lateral (select n.a1[1]) l(a1)", null);
        testDefault = preview(previewDto);
        assertThat(testDefault.getColumns().get(0).getType()).isEqualTo("INTEGER");
        assertThat(testDefault.getData().get(0)[0]).isEqualTo(1);
    }
}
