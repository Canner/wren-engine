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
import com.google.common.io.Resources;
import io.wren.base.WrenTypes;
import io.wren.base.dto.Column;
import io.wren.base.dto.Model;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertFalse;

public class TPCH
{
    public static final Map<Integer, String> QUERIES = ImmutableMap.<Integer, String>builder()
            .put(1, getTpchQuery(1, 3))
            .put(2, getTpchQuery(2, 33, "part type like", "region name"))
            .put(3, getTpchQuery(3, "market segment", "2013-03-05"))
            .put(4, getTpchQuery(4, "2013-03-05"))
            .put(5, getTpchQuery(5, "region name", "2013-03-05"))
            .put(6, getTpchQuery(6, "2013-03-05", 33, 44))
            .put(7, getTpchQuery(7, "nation name 1", "nation name 2"))
            .put(8, getTpchQuery(8, "nation name", "region name", "part type"))
            .put(9, getTpchQuery(9, "part name like"))
            .put(10, getTpchQuery(10, "2013-03-05"))
            .put(11, getTpchQuery(11, "nation name", 33))
            .put(12, getTpchQuery(12, "ship mode 1", "ship mode 2", "2013-03-05"))
            .put(13, getTpchQuery(13, "comment like 1", "comment like 2"))
            .put(14, getTpchQuery(14, "2013-03-05"))
            // query 15: views not supported
            .put(16, getTpchQuery(16, "part brand", "part type like", 3, 4, 5, 6, 7, 8, 9, 10))
            .put(17, getTpchQuery(17, "part brand", "part container"))
            .put(18, getTpchQuery(18, 33))
            .put(19, getTpchQuery(19, "part brand 1", "part brand 2", "part brand 3", 11, 22, 33))
            .put(20, getTpchQuery(20, "part name like", "2013-03-05", "nation name"))
            .put(21, getTpchQuery(21, "nation name"))
            .put(22, getTpchQuery(22,
                    "phone 1",
                    "phone 2",
                    "phone 3",
                    "phone 4",
                    "phone 5",
                    "phone 6",
                    "phone 7"))
            .build();

    public static final String ORDERS_PATH = requireNonNull(TPCH.class.getClassLoader().getResource("tpch/data/orders.parquet")).getPath();
    public static final String LINEITEM_PATH = requireNonNull(TPCH.class.getClassLoader().getResource("tpch/data/lineitem.parquet")).getPath();
    public static final String CUSTOMER_PATH = requireNonNull(TPCH.class.getClassLoader().getResource("tpch/data/customer.parquet")).getPath();
    public static final String NATION_PATH = requireNonNull(TPCH.class.getClassLoader().getResource("tpch/data/nation.parquet")).getPath();
    public static final String REGION_PATH = requireNonNull(TPCH.class.getClassLoader().getResource("tpch/data/region.parquet")).getPath();
    public static final String PART_PATH = requireNonNull(TPCH.class.getClassLoader().getResource("tpch/data/part.parquet")).getPath();
    public static final String SUPPLIER_PATH = requireNonNull(TPCH.class.getClassLoader().getResource("tpch/data/supplier.parquet")).getPath();
    public static final String PARTSUPP_PATH = requireNonNull(TPCH.class.getClassLoader().getResource("tpch/data/partsupp.parquet")).getPath();

    private static final Model ORDERS = Model.model("orders",
            "select * from tablePrefix.orders",
            List.of(
                    Column.column("o_orderkey", WrenTypes.INTEGER, null, false, "o_orderkey"),
                    Column.column("o_custkey", WrenTypes.INTEGER, null, false, "o_custkey"),
                    Column.column("o_orderstatus", WrenTypes.VARCHAR, null, false, "o_orderstatus"),
                    Column.column("o_totalprice", WrenTypes.INTEGER, null, false, "o_totalprice"),
                    Column.column("o_orderdate", WrenTypes.DATE, null, false, "o_orderdate"),
                    Column.column("o_orderpriority", WrenTypes.VARCHAR, null, false, "o_orderpriority"),
                    Column.column("o_clerk", WrenTypes.VARCHAR, null, false, "o_clerk"),
                    Column.column("o_shippriority", WrenTypes.INTEGER, null, false, "o_shippriority"),
                    Column.column("o_comment", WrenTypes.VARCHAR, null, false, "o_comment")),
            "o_orderkey");

    private static final Model LINEITEM = Model.model("lineitem",
            "select * from tablePrefix.lineitem",
            List.of(
                    Column.column("l_orderkey", WrenTypes.INTEGER, null, false, "l_orderkey"),
                    Column.column("l_partkey", WrenTypes.INTEGER, null, false, "l_partkey"),
                    Column.column("l_suppkey", WrenTypes.INTEGER, null, false, "l_suppkey"),
                    Column.column("l_linenumber", WrenTypes.INTEGER, null, false, "l_linenumber"),
                    Column.column("l_quantity", WrenTypes.INTEGER, null, false, "l_quantity"),
                    Column.column("l_extendedprice", WrenTypes.INTEGER, null, false, "l_extendedprice"),
                    Column.column("l_discount", WrenTypes.INTEGER, null, false, "l_discount"),
                    Column.column("l_tax", WrenTypes.INTEGER, null, false, "l_tax"),
                    Column.column("l_returnflag", WrenTypes.VARCHAR, null, false, "l_returnflag"),
                    Column.column("l_linestatus", WrenTypes.VARCHAR, null, false, "l_linestatus"),
                    Column.column("l_shipdate", WrenTypes.DATE, null, false, "l_shipdate"),
                    Column.column("l_commitdate", WrenTypes.DATE, null, false, "l_commitdate"),
                    Column.column("l_receiptdate", WrenTypes.DATE, null, false, "l_receiptdate"),
                    Column.column("l_shipinstruct", WrenTypes.VARCHAR, null, false, "l_shipinstruct"),
                    Column.column("l_shipmode", WrenTypes.VARCHAR, null, false, "l_shipmode"),
                    Column.column("l_comment", WrenTypes.VARCHAR, null, false, "l_comment"),
                    Column.column("orderkey_linenumber", WrenTypes.VARCHAR, null, true, "concat(l_orderkey, '-', l_linenumber)")),
            "orderkey_linenumber");

    private static final Model CUSTOMER = Model.model("customer",
            "select * from tablePrefix.customer",
            List.of(
                    Column.column("c_custkey", WrenTypes.INTEGER, null, false, "c_custkey"),
                    Column.column("c_name", WrenTypes.VARCHAR, null, false, "c_name"),
                    Column.column("c_address", WrenTypes.VARCHAR, null, false, "c_address"),
                    Column.column("c_nationkey", WrenTypes.INTEGER, null, false, "c_nationkey"),
                    Column.column("c_phone", WrenTypes.VARCHAR, null, false, "c_phone"),
                    Column.column("c_acctbal", WrenTypes.DOUBLE, null, false, "c_acctbal"),
                    Column.column("c_mktsegment", WrenTypes.VARCHAR, null, false, "c_mktsegment"),
                    Column.column("c_comment", WrenTypes.VARCHAR, null, false, "c_comment")),
            "c_custkey");

    private static final Model NATION = Model.model("nation",
            "select * from tablePrefix.nation",
            List.of(
                    Column.column("n_nationkey", WrenTypes.INTEGER, null, false, "n_nationkey"),
                    Column.column("n_name", WrenTypes.VARCHAR, null, false, "n_name"),
                    Column.column("n_regionkey", WrenTypes.INTEGER, null, false, "n_regionkey"),
                    Column.column("n_comment", WrenTypes.VARCHAR, null, false, "n_comment")),
            "n_nationkey");

    private static final Model REGION = Model.model("region",
            "select * from tablePrefix.region",
            List.of(
                    Column.column("r_regionkey", WrenTypes.INTEGER, null, false, "r_regionkey"),
                    Column.column("r_name", WrenTypes.VARCHAR, null, false, "r_name"),
                    Column.column("r_comment", WrenTypes.VARCHAR, null, false, "r_comment")),
            "r_regionkey");

    private static final Model PART = Model.model("part",
            "select * from tablePrefix.part",
            List.of(
                    Column.column("p_partkey", WrenTypes.INTEGER, null, false, "p_partkey"),
                    Column.column("p_name", WrenTypes.VARCHAR, null, false, "p_name"),
                    Column.column("p_mfgr", WrenTypes.VARCHAR, null, false, "p_mfgr"),
                    Column.column("p_brand", WrenTypes.VARCHAR, null, false, "p_brand"),
                    Column.column("p_type", WrenTypes.VARCHAR, null, false, "p_type"),
                    Column.column("p_size", WrenTypes.INTEGER, null, false, "p_size"),
                    Column.column("p_container", WrenTypes.VARCHAR, null, false, "p_container"),
                    Column.column("p_retailprice", WrenTypes.DOUBLE, null, false, "p_retailprice"),
                    Column.column("p_comment", WrenTypes.VARCHAR, null, false, "p_comment")),
            "p_partkey");

    private static final Model SUPPLIER = Model.model("supplier",
            "select * from tablePrefix.supplier",
            List.of(
                    Column.column("s_suppkey", WrenTypes.INTEGER, null, false, "s_suppkey"),
                    Column.column("s_name", WrenTypes.VARCHAR, null, false, "s_name"),
                    Column.column("s_address", WrenTypes.VARCHAR, null, false, "s_address"),
                    Column.column("s_nationkey", WrenTypes.INTEGER, null, false, "s_nationkey"),
                    Column.column("s_phone", WrenTypes.VARCHAR, null, false, "s_phone"),
                    Column.column("s_acctbal", WrenTypes.DOUBLE, null, false, "s_acctbal"),
                    Column.column("s_comment", WrenTypes.VARCHAR, null, false, "s_comment")),
            "s_suppkey");

    private static final Model PART_SUPP = Model.model("partsupp",
            "select * from tablePrefix.partsupp",
            List.of(
                    Column.column("ps_partkey", WrenTypes.INTEGER, null, false, "ps_partkey"),
                    Column.column("ps_suppkey", WrenTypes.INTEGER, null, false, "ps_suppkey"),
                    Column.column("ps_availqty", WrenTypes.INTEGER, null, false, "ps_availqty"),
                    Column.column("ps_supplycost", WrenTypes.DOUBLE, null, false, "ps_supplycost"),
                    Column.column("ps_comment", WrenTypes.VARCHAR, null, false, "ps_comment"),
                    Column.column("partkey_suppkey", WrenTypes.VARCHAR, null, true, "concat(ps_partkey, '-', ps_suppkey)")),
            "partkey_suppkey");

    public static final List<Model> MODELS = List.of(ORDERS, LINEITEM, CUSTOMER, NATION, REGION, PART, SUPPLIER, PART_SUPP);

    private TPCH() {}

    public static List<Model> getModels(String tablePrefix)
    {
        return MODELS.stream()
                .map(m -> Model.model(
                        m.getName(),
                        m.getRefSql().replace("tablePrefix", tablePrefix),
                        m.getColumns(),
                        m.getPrimaryKey()))
                .collect(toImmutableList());
    }

    private static String getTpchQuery(int query, Object... values)
    {
        String sql = getTpchQuery(query);

        for (int i = values.length - 1; i >= 0; i--) {
            sql = sql.replaceAll(format(":%s", i + 1), String.valueOf(values[i]));
        }

        assertFalse(sql.matches("(?s).*:[0-9].*"), "Not all bind parameters were replaced: " + sql);

        return fixTpchQuery(sql);
    }

    private static String getTpchQuery(int q)
    {
        return readResource("tpch/queries/" + q + ".sql");
    }

    private static String readResource(String name)
    {
        try {
            return Resources.toString(Resources.getResource(name), UTF_8);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String fixTpchQuery(String s)
    {
        s = s.replaceFirst("(?m);$", "");
        s = s.replaceAll("(?m)^:[xo]$", "");
        s = s.replaceAll("(?m)^:n -1$", "");
        s = s.replaceAll("(?m)^:n ([0-9]+)$", "LIMIT $1");
        s = s.replace("day (3)", "day"); // for query 1
        return s;
    }
}
