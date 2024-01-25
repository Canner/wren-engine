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

package io.accio.base.sqlrewrite.analyzer;

import io.accio.base.AccioMDL;
import io.accio.base.CatalogSchemaTableName;
import io.accio.base.dto.Model;
import io.accio.base.sqlrewrite.AbstractTestFramework;
import io.accio.base.type.BigIntType;
import io.accio.base.type.BooleanType;
import io.accio.base.type.ByteaType;
import io.accio.base.type.DateType;
import io.accio.base.type.DoubleType;
import io.accio.base.type.IntegerType;
import io.accio.base.type.IntervalType;
import io.accio.base.type.NumericType;
import io.accio.base.type.PGArray;
import io.accio.base.type.RealType;
import io.accio.base.type.RecordType;
import io.accio.base.type.TimestampType;
import io.accio.base.type.VarcharType;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.accio.base.AccioTypes.INTEGER;
import static io.accio.base.AccioTypes.VARCHAR;
import static io.accio.base.dto.Column.column;
import static io.accio.base.dto.Model.model;
import static io.accio.base.sqlrewrite.Utils.parseExpression;
import static io.accio.base.sqlrewrite.analyzer.ExpressionTypeAnalyzer.analyze;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExpressionTypeAnalyzer
        extends AbstractTestFramework
{
    private static final Scope EMPTY_SCOPE = Scope.builder().build();
    private static final AccioMDL EMPTY_MDL = AccioMDL.fromManifest(withDefaultCatalogSchema().build());

    private final Model customer;

    public TestExpressionTypeAnalyzer()
    {
        customer = model("Customer",
                "select * from main.customer",
                List.of(
                        column("custkey", INTEGER, null, true),
                        column("name", VARCHAR, null, true),
                        column("address", VARCHAR, null, true),
                        column("nationkey", INTEGER, null, true),
                        column("phone", VARCHAR, null, true),
                        column("acctbal", INTEGER, null, true),
                        column("mktsegment", VARCHAR, null, true),
                        column("comment", VARCHAR, null, true)),
                "custkey");
    }

    @Test
    public void testLiteral()
    {
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("1"))).isEqualTo(BigIntType.BIGINT);
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("'abc'"))).isEqualTo(VarcharType.VARCHAR);
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("INTERVAL '1 month'"))).isEqualTo(IntervalType.INTERVAL);
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("NULL"))).isEqualTo(null);
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("TIMESTAMP '2023-10-29 00:00:00.000000'"))).isEqualTo(TimestampType.TIMESTAMP);
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("REAL '3.5'"))).isEqualTo(RealType.REAL);
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("x'65683F'"))).isEqualTo(ByteaType.BYTEA);
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("1.1"))).isEqualTo(NumericType.NUMERIC);
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("10.3e0"))).isEqualTo(DoubleType.DOUBLE);
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("false"))).isEqualTo(BooleanType.BOOLEAN);
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("cast(1.1 as DOUBLE)"))).isEqualTo(DoubleType.DOUBLE);
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("ROW(1, 2e0)"))).isEqualTo(RecordType.EMPTY_RECORD);
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("array[1,2,3]"))).isEqualTo(PGArray.INT8_ARRAY);
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("array['a','b','c']"))).isEqualTo(PGArray.VARCHAR_ARRAY);
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("current_user"))).isEqualTo(VarcharType.VARCHAR);
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("current_schema"))).isEqualTo(VarcharType.VARCHAR);
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("current_catalog"))).isEqualTo(VarcharType.VARCHAR);
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("current_path"))).isEqualTo(VarcharType.VARCHAR);
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("current_date"))).isEqualTo(DateType.DATE);
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("current_time"))).isEqualTo(TimestampType.TIMESTAMP);
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("current_timestamp"))).isEqualTo(TimestampType.TIMESTAMP);
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("localtime"))).isEqualTo(TimestampType.TIMESTAMP);
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("localtimestamp"))).isEqualTo(TimestampType.TIMESTAMP);
    }

    @Test
    public void testPredicate()
    {
        assertPredicate("x > 1");
        assertPredicate("x >= 1");
        assertPredicate("x < 1");
        assertPredicate("x <= 1");
        assertPredicate("x = 1");
        assertPredicate("x <> y");
        assertPredicate("x != INTERVAL '1 month'");
        assertPredicate("x IS NULL");
        assertPredicate("x IS NOT NULL");
        assertPredicate("x in (1, 2, 3)");
        assertPredicate("x like 'abc'");
        assertPredicate("x between 1 and 2");
        assertPredicate("x > 1 and y < 2");
        assertPredicate("x > 1 or y < 2");
        assertPredicate("not x > 1");
        assertPredicate("exists (select 1)");
    }

    private void assertPredicate(String expression)
    {
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression(expression))).isEqualTo(BooleanType.BOOLEAN);
    }

    @Test
    public void testFunction()
    {
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("date_trunc('day', create_date)"))).isEqualTo(DateType.DATE);
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("now()"))).isEqualTo(TimestampType.TIMESTAMP);
        assertThat(analyze(EMPTY_MDL, EMPTY_SCOPE, parseExpression("now___timestamp()"))).isEqualTo(TimestampType.TIMESTAMP);
    }

    @Test
    public void testColumns()
    {
        AccioMDL mdl = AccioMDL.fromManifest(withDefaultCatalogSchema().setModels(List.of(customer)).build());
        List<Field> fields = customer.getColumns().stream()
                .map(column -> Field.builder()
                        .modelName(new CatalogSchemaTableName(mdl.getCatalog(), mdl.getSchema(), customer.getName()))
                        .columnName(column.getName())
                        .name(column.getName())
                        .build())
                .collect(toImmutableList());
        Scope scope = Scope.builder().relationType(new RelationType(fields)).build();

        assertThat(analyze(mdl, scope, parseExpression("custkey"))).isEqualTo(IntegerType.INTEGER);
        assertThat(analyze(mdl, scope, parseExpression("name"))).isEqualTo(VarcharType.VARCHAR);
        assertThat(analyze(mdl, scope, parseExpression("address"))).isEqualTo(VarcharType.VARCHAR);
        assertThat(analyze(mdl, scope, parseExpression("nationkey"))).isEqualTo(IntegerType.INTEGER);
        assertThat(analyze(mdl, scope, parseExpression("phone"))).isEqualTo(VarcharType.VARCHAR);
        assertThat(analyze(mdl, scope, parseExpression("acctbal"))).isEqualTo(IntegerType.INTEGER);
        assertThat(analyze(mdl, scope, parseExpression("mktsegment"))).isEqualTo(VarcharType.VARCHAR);
        assertThat(analyze(mdl, scope, parseExpression("comment"))).isEqualTo(VarcharType.VARCHAR);

        assertThat(analyze(mdl, scope, parseExpression("Customer.custkey"))).isEqualTo(IntegerType.INTEGER);
        assertThat(analyze(mdl, scope, parseExpression("Customer.name"))).isEqualTo(VarcharType.VARCHAR);
        assertThat(analyze(mdl, scope, parseExpression("Customer.address"))).isEqualTo(VarcharType.VARCHAR);
        assertThat(analyze(mdl, scope, parseExpression("Customer.nationkey"))).isEqualTo(IntegerType.INTEGER);
        assertThat(analyze(mdl, scope, parseExpression("Customer.phone"))).isEqualTo(VarcharType.VARCHAR);
        assertThat(analyze(mdl, scope, parseExpression("Customer.acctbal"))).isEqualTo(IntegerType.INTEGER);
        assertThat(analyze(mdl, scope, parseExpression("Customer.mktsegment"))).isEqualTo(VarcharType.VARCHAR);
        assertThat(analyze(mdl, scope, parseExpression("Customer.comment"))).isEqualTo(VarcharType.VARCHAR);
    }
}
