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

package io.cml.tpch;

import java.sql.Date;
import java.util.Arrays;
import java.util.List;

/**
 * A table from the TPC-H benchmark.
 */
public enum TpchTable
{
    /**
     * <pre>{@code
     * CREATE TABLE CUSTOMER (
     *   CUSTKEY    INT NOT NULL,
     *   NAME       VARCHAR(25),
     *   ADDRESS    VARCHAR(40),
     *   NATIONKEY  INT NOT NULL,
     *   PHONE      CHAR(15),
     *   ACCTBAL    DECIMAL,
     *   MKTSEGMENT CHAR(10),
     *   COMMENT    VARCHAR(117))
     * }</pre>
     */
    CUSTOMER(Arrays.asList(
            new Column("custkey", Integer.class),
            new Column("name", String.class),
            new Column("address", String.class),
            new Column("nationkey", Integer.class),
            new Column("phone", String.class),
            new Column("acctbal", Double.class),
            new Column("mktsegment", String.class),
            new Column("comment", String.class))),
    /**
     * <pre>{@code
     * CREATE TABLE LINEITEM (
     *  ORDERKEY       INT NOT NULL,
     *  PARTKEY        INT NOT NULL,
     *  SUPPKEY        INT NOT NULL,
     *  LINENUMBER INT,
     *  QUANTITY        DECIMAL,
     *  EXTENDEDPRICE DECIMAL,
     *  DISCOUNT        DECIMAL,
     *  TAX            DECIMAL,
     *  RETURNFLAG CHAR(1),
     *  LINESTATUS CHAR(1),
     *  SHIPDATE        DATE,
     *  COMMITDATE DATE,
     *  RECEIPTDATE DATE,
     *  SHIPINSTRUCT CHAR(25),
     *  SHIPMODE        CHAR(10),
     *  COMMENT        VARCHAR(44))
     * }</pre>
     */
    LINEITEM(Arrays.asList(
            new Column("orderkey", Integer.class),
            new Column("partkey", Integer.class),
            new Column("suppkey", Integer.class),
            new Column("linenumber", Integer.class),
            new Column("quantity", Integer.class),
            new Column("extendedprice", Double.class),
            new Column("discount", Double.class),
            new Column("tax", Double.class),
            new Column("returnflag", String.class),
            new Column("linestatus", String.class),
            new Column("shipdate", Date.class),
            new Column("commitdate", Date.class),
            new Column("receiptdate", Date.class),
            new Column("shipinstruct", String.class),
            new Column("shipmode", String.class),
            new Column("comment", String.class))),
    /**
     * <pre>{@code
     * CREATE TABLE ORDERS (
     *  ORDERKEY      INT NOT NULL,
     *  CUSTKEY       INT NOT NULL,
     *  ORDERSTATUS   CHAR(1),
     *  TOTALPRICE    DECIMAL,
     *  ORDERDATE     DATE,
     *  ORDERPRIORITY CHAR(15),
     *  CLERK         CHAR(15),
     *  SHIPPRIORITY  INT,
     *  COMMENT       VARCHAR(79))
     *    }</pre>
     */
    ORDERS(Arrays.asList(
            new Column("orderkey", Integer.class),
            new Column("custkey", Integer.class),
            new Column("orderstatus", String.class),
            new Column("totalprice", Double.class),
            new Column("orderdate", Date.class),
            new Column("orderpriority", String.class),
            new Column("clerk", String.class),
            new Column("shippriority", Integer.class),
            new Column("comment", String.class))),
    /**
     * <pre>{@code
     * CREATE TABLE NATION (
     *   NATIONKEY INT NOT NULL,
     *   NAME      CHAR(25),
     *   REGIONKEY INT NOT NULL,
     *   COMMENT   VARCHAR(152))
     * }</pre>
     */
    NATION(Arrays.asList(
            new Column("nationkey", Integer.class),
            new Column("name", String.class),
            new Column("regionkey", Integer.class),
            new Column("comment", String.class))),
    /**
     * <pre>{@code
     * CREATE TABLE part(
     *   partkey INT NOT NULL,
     *   name STRING,
     *   mfgr STRING,
     *   brand STRING,
     *   type STRING,
     *   size INT,
     *   container STRING,
     *   retailprice DOUBLE,
     *   comment STRING)
     * }</pre>
     */
    PART(Arrays.asList(
            new Column("partkey", Integer.class),
            new Column("name", String.class),
            new Column("mfgr", String.class),
            new Column("brand", String.class),
            new Column("type", String.class),
            new Column("size", Integer.class),
            new Column("container", String.class),
            new Column("retailprice", Double.class),
            new Column("comment", String.class))),
    /**
     * <pre>{@code
     *   CREATE TABLE PARTSUPP(
     *   PARTKEY    INT NOT NULL,
     *   SUPPKEY    INT NOT NULL,
     *   AVAILQTY   INT,
     *   SUPPLYCOST DECIMAL,
     *   COMMENT    VARCHAR(199))
     * }</pre>
     */
    PARTSUPP(Arrays.asList(
            new Column("partkey", Integer.class),
            new Column("suppkey", Integer.class),
            new Column("availqty", Integer.class),
            new Column("supplycost", Double.class),
            new Column("comment", String.class))),
    /**
     * <pre>{@code
     * CREATE TABLE REGION (
     *   REGIONKEY INT NOT NULL,
     *   NAME      CHAR(25),
     *   COMMENT   VARCHAR(152))
     * }</pre>
     */
    REGION(Arrays.asList(
            new Column("regionkey", Integer.class),
            new Column("name", String.class),
            new Column("comment", String.class))),
    /**
     * <pre>{@code
     * CREATE TABLE SUPPLIER (
     *  SUPPKEY   INT NOT NULL,
     *  NAME      CHAR(25),
     *  ADDRESS   VARCHAR(40),
     *  NATIONKEY INT NOT NULL,
     *  PHONE     CHAR(15),
     *  ACCTBAL   DECIMAL,
     *  COMMENT   VARCHAR(101))
     * }</pre>
     */
    SUPPLIER(Arrays.asList(
            new Column("suppkey", Integer.class),
            new Column("name", String.class),
            new Column("address", String.class),
            new Column("nationkey", Integer.class),
            new Column("phone", String.class),
            new Column("acctbal", Double.class),
            new Column("comment", String.class)));

    public final List<Column> columns;

    TpchTable(final List<Column> columns)
    {
        this.columns = columns;
    }

    /**
     * A table column defined by a name and type.
     */
    public static final class Column
    {
        public final String name;
        public final Class<?> type;

        private Column(final String name, final Class<?> type)
        {
            this.name = name;
            this.type = type;
        }
    }
}
