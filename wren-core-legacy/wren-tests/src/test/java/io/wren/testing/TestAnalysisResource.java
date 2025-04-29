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
import io.trino.sql.tree.SortItem;
import io.wren.base.WrenTypes;
import io.wren.base.dto.Column;
import io.wren.base.dto.Manifest;
import io.wren.base.sqlrewrite.analyzer.decisionpoint.FilterAnalysis;
import io.wren.base.sqlrewrite.analyzer.decisionpoint.RelationAnalysis;
import io.wren.main.web.dto.QueryAnalysisDto;
import io.wren.main.web.dto.SqlAnalysisInputBatchDto;
import io.wren.main.web.dto.SqlAnalysisInputDto;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Set;

import static io.wren.base.dto.Model.onTableReference;
import static io.wren.base.dto.TableReference.tableReference;
import static io.wren.main.web.dto.NodeLocationDto.nodeLocationDto;
import static io.wren.testing.AbstractTestFramework.DEFAULT_SESSION_CONTEXT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAnalysisResource
        extends RequireWrenServer
{
    private Manifest manifest;

    @Override
    protected TestingWrenServer createWrenServer()
    {
        initData();

        Path mdlDir;
        try {
            mdlDir = Files.createTempDirectory("wrenmdls");
            Path wrenMDLFilePath = mdlDir.resolve("wrenmdl.json");
            Files.write(wrenMDLFilePath, MANIFEST_JSON_CODEC.toJsonBytes(manifest));
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("wren.directory", mdlDir.toAbsolutePath().toString())
                .put("wren.datasource.type", "duckdb");

        return TestingWrenServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
    }

    private void initData()
    {
        List<Column> customerColumns = List.of(
                Column.column("custkey", WrenTypes.INTEGER, null, true),
                Column.column("name", WrenTypes.VARCHAR, null, true),
                Column.column("address", WrenTypes.VARCHAR, null, true),
                Column.column("nationkey", WrenTypes.INTEGER, null, true),
                Column.column("phone", WrenTypes.VARCHAR, null, true),
                Column.column("acctbal", WrenTypes.INTEGER, null, true),
                Column.column("mktsegment", WrenTypes.VARCHAR, null, true),
                Column.column("comment", WrenTypes.VARCHAR, null, true));
        List<Column> ordersColumns = List.of(
                Column.column("orderkey", WrenTypes.INTEGER, null, true),
                Column.column("custkey", WrenTypes.INTEGER, null, true),
                Column.column("orderstatus", WrenTypes.VARCHAR, null, true),
                Column.column("totalprice", WrenTypes.INTEGER, null, true),
                Column.column("orderdate", WrenTypes.DATE, null, true),
                Column.column("orderpriority", WrenTypes.VARCHAR, null, true),
                Column.column("clerk", WrenTypes.VARCHAR, null, true),
                Column.column("shippriority", WrenTypes.INTEGER, null, true),
                Column.column("comment", WrenTypes.VARCHAR, null, true));
        List<Column> lineitemColumns = List.of(
                Column.column("orderkey", WrenTypes.INTEGER, null, true),
                Column.column("partkey", WrenTypes.INTEGER, null, true),
                Column.column("suppkey", WrenTypes.INTEGER, null, true),
                Column.column("linenumber", WrenTypes.INTEGER, null, true),
                Column.column("quantity", WrenTypes.INTEGER, null, true),
                Column.column("extendedprice", WrenTypes.INTEGER, null, true),
                Column.column("discount", WrenTypes.INTEGER, null, true),
                Column.column("tax", WrenTypes.INTEGER, null, true),
                Column.column("returnflag", WrenTypes.VARCHAR, null, true),
                Column.column("linestatus", WrenTypes.VARCHAR, null, true),
                Column.column("shipdate", WrenTypes.DATE, null, true),
                Column.column("commitdate", WrenTypes.DATE, null, true),
                Column.column("receiptdate", WrenTypes.DATE, null, true),
                Column.column("shipinstruct", WrenTypes.VARCHAR, null, true),
                Column.column("shipmode", WrenTypes.VARCHAR, null, true),
                Column.column("comment", WrenTypes.VARCHAR, null, true));

        manifest = Manifest.builder()
                .setCatalog(DEFAULT_SESSION_CONTEXT.getCatalog().orElseThrow())
                .setSchema(DEFAULT_SESSION_CONTEXT.getSchema().orElseThrow())
                .setModels(List.of(onTableReference("customer", tableReference(null, "main", "customer"), customerColumns, "custkey"),
                        onTableReference("orders", tableReference(null, "main", "orders"), ordersColumns, "orderkey"),
                        onTableReference("lineitem", tableReference(null, "main", "lineitem"), lineitemColumns, null)))
                .build();
    }

    @Test
    public void testBasic()
    {
        List<QueryAnalysisDto> result = getSqlAnalysis(new SqlAnalysisInputDto(manifest, "select * from customer"));
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getRelation().getType()).isEqualTo(RelationAnalysis.Type.TABLE.name());
        assertThat(result.get(0).getRelation().getAlias()).isNull();
        assertThat(result.get(0).getSelectItems().size()).isEqualTo(8);
        // all of select item match the star symbol position
        assertThat(result.get(0).getSelectItems().get(0).getNodeLocation()).isEqualTo(nodeLocationDto(1, 8));
        assertThat(result.get(0).getSelectItems().get(0).getExprSources())
                .isEqualTo(List.of(new QueryAnalysisDto.ExprSourceDto("custkey", "customer", "custkey", nodeLocationDto(1, 8))));
        assertThat(result.get(0).getSelectItems().get(1).getNodeLocation()).isEqualTo(nodeLocationDto(1, 8));
        assertThat(result.get(0).getRelation().getTableName()).isEqualTo("customer");
        assertThat(result.get(0).getRelation().getNodeLocation()).isEqualTo(nodeLocationDto(1, 15));

        result = getSqlAnalysis(new SqlAnalysisInputDto(manifest, "select custkey, count(*) from customer group by 1"));
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getRelation().getType()).isEqualTo(RelationAnalysis.Type.TABLE.name());
        assertThat(result.get(0).getRelation().getAlias()).isNull();
        assertThat(result.get(0).getSelectItems().size()).isEqualTo(2);
        assertThat(result.get(0).getSelectItems().get(0).getNodeLocation()).isEqualTo(nodeLocationDto(1, 8));
        assertThat(result.get(0).getSelectItems().get(1).getNodeLocation()).isEqualTo(nodeLocationDto(1, 17));
        assertThat(result.get(0).getRelation().getTableName()).isEqualTo("customer");
        assertThat(result.get(0).getRelation().getNodeLocation()).isEqualTo(nodeLocationDto(1, 31));
        assertThat(result.get(0).getGroupByKeys().size()).isEqualTo(1);
        assertThat(result.get(0).getGroupByKeys().get(0).get(0)).isEqualTo(
                new QueryAnalysisDto.GroupByKeyDto("custkey",
                nodeLocationDto(1, 49),
                List.of(new QueryAnalysisDto.ExprSourceDto("custkey", "customer", "custkey", nodeLocationDto(1, 8)))));

        result = getSqlAnalysis(new SqlAnalysisInputDto(manifest, "select * from customer c join orders o on c.custkey = o.custkey"));
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getRelation().getType()).isEqualTo(RelationAnalysis.Type.INNER_JOIN.name());
        assertThat(result.get(0).getRelation().getAlias()).isNull();
        assertThat(result.get(0).getRelation().getTableName()).isNull();
        assertThat(result.get(0).getRelation().getNodeLocation()).isEqualTo(nodeLocationDto(1, 15));
        assertThat(result.get(0).getSelectItems().size()).isEqualTo(17);
        assertThat(result.get(0).getRelation().getLeft().getType()).isEqualTo(RelationAnalysis.Type.TABLE.name());
        assertThat(result.get(0).getRelation().getLeft().getNodeLocation()).isEqualTo(nodeLocationDto(1, 15));
        assertThat(result.get(0).getRelation().getRight().getType()).isEqualTo(RelationAnalysis.Type.TABLE.name());
        assertThat(result.get(0).getRelation().getRight().getNodeLocation()).isEqualTo(nodeLocationDto(1, 31));
        assertThat(result.get(0).getRelation().getCriteria().getExpression()).isEqualTo("ON (c.custkey = o.custkey)");
        assertThat(result.get(0).getRelation().getCriteria().getNodeLocation()).isEqualTo(nodeLocationDto(1, 43));
        assertThat(Set.copyOf(result.get(0).getRelation().getExprSources()))
                .isEqualTo(Set.of(new QueryAnalysisDto.ExprSourceDto("c.custkey", "customer", "custkey", nodeLocationDto(1, 43)),
                        new QueryAnalysisDto.ExprSourceDto("o.custkey", "orders", "custkey", nodeLocationDto(1, 55))));

        result = getSqlAnalysis(new SqlAnalysisInputDto(manifest, "SELECT * FROM customer WHERE custkey = 1 OR (name = 'test' AND address = 'test')"));
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getRelation().getType()).isEqualTo(RelationAnalysis.Type.TABLE.name());
        assertThat(result.get(0).getRelation().getAlias()).isNull();
        assertThat(result.get(0).getSelectItems().size()).isEqualTo(8);
        assertThat(result.get(0).getRelation().getTableName()).isEqualTo("customer");

        assertThat(result.get(0).getFilter().getType()).isEqualTo(FilterAnalysis.Type.OR.name());
        assertThat(result.get(0).getFilter().getLeft().getType()).isEqualTo(FilterAnalysis.Type.EXPR.name());
        assertThat(assertThat(result.get(0).getFilter().getLeft().getExprSources())
                .isEqualTo(List.of(new QueryAnalysisDto.ExprSourceDto("custkey", "customer", "custkey", nodeLocationDto(1, 30)))));
        assertThat(result.get(0).getFilter().getRight().getType()).isEqualTo(FilterAnalysis.Type.AND.name());

        result = getSqlAnalysis(new SqlAnalysisInputDto(manifest, "SELECT custkey, count(*), name FROM customer GROUP BY 1, 3, nationkey"));
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getGroupByKeys().size()).isEqualTo(3);
        assertThat(result.get(0).getGroupByKeys().get(0).get(0)).isEqualTo(new QueryAnalysisDto.GroupByKeyDto("custkey",
                nodeLocationDto(1, 55),
                List.of(new QueryAnalysisDto.ExprSourceDto("custkey", "customer", "custkey", nodeLocationDto(1, 8)))));
        assertThat(result.get(0).getGroupByKeys().get(1).get(0)).isEqualTo(new QueryAnalysisDto.GroupByKeyDto("name",
                nodeLocationDto(1, 58),
                List.of(new QueryAnalysisDto.ExprSourceDto("name", "customer", "name", nodeLocationDto(1, 27)))));
        assertThat(result.get(0).getGroupByKeys().get(2).get(0)).isEqualTo(new QueryAnalysisDto.GroupByKeyDto("nationkey",
                nodeLocationDto(1, 61),
                List.of(new QueryAnalysisDto.ExprSourceDto("nationkey", "customer", "nationkey", nodeLocationDto(1, 61)))));

        result = getSqlAnalysis(new SqlAnalysisInputDto(manifest, "SELECT custkey, name FROM customer ORDER BY 1 ASC, 2 DESC"));
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getSortings().size()).isEqualTo(2);
        assertThat(result.get(0).getSortings().get(0).getExpression()).isEqualTo("custkey");
        assertThat(result.get(0).getSortings().get(0).getOrdering()).isEqualTo(SortItem.Ordering.ASCENDING.name());
        assertThat(result.get(0).getSortings().get(0).getExprSources()).isEqualTo(List.of(new QueryAnalysisDto.ExprSourceDto("custkey", "customer", "custkey", nodeLocationDto(1, 8))));
        assertThat(result.get(0).getSortings().get(0).getNodeLocation()).isEqualTo(nodeLocationDto(1, 45));
        assertThat(result.get(0).getSortings().get(1).getExpression()).isEqualTo("name");
        assertThat(result.get(0).getSortings().get(1).getOrdering()).isEqualTo(SortItem.Ordering.DESCENDING.name());
        assertThat(result.get(0).getSortings().get(1).getNodeLocation()).isEqualTo(nodeLocationDto(1, 52));
    }

    @Test
    public void testBatchAnalysis()
    {
        SqlAnalysisInputBatchDto inputBatchDto = new SqlAnalysisInputBatchDto(
                base64Encode(toJson(manifest)),
                List.of("select * from customer",
                        "select custkey, count(*) from customer group by 1",
                        "with t1 as (select * from customer) select * from t1",
                        "select * from orders where orderstatus = 'O' union select * from orders where orderstatus = 'F'"));

        List<List<QueryAnalysisDto>> results = getSqlAnalysisBatch(inputBatchDto);
        assertThat(results.size()).isEqualTo(4);
        assertThat(results.get(0).size()).isEqualTo(1);
        assertThat(results.get(1).size()).isEqualTo(1);
        assertThat(results.get(2).size()).isEqualTo(2);
        assertThat(results.get(3).size()).isEqualTo(2);
    }

    private String toJson(Manifest manifest)
    {
        return MANIFEST_JSON_CODEC.toJson(manifest);
    }

    private String base64Encode(String str)
    {
        return Base64.getEncoder().encodeToString(str.getBytes(UTF_8));
    }
}
