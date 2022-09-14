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

package io.cml.testing;

import io.cml.metadata.Metadata;
import io.cml.metadata.TableHandle;
import io.cml.metadata.TableSchema;
import io.cml.metrics.Metric;
import io.cml.metrics.MetricHook;
import io.cml.metrics.MetricSql;
import io.cml.metrics.MetricStore;
import io.cml.spi.SessionContext;
import io.cml.spi.metadata.SchemaTableName;
import io.cml.sql.QualifiedObjectName;
import io.cml.sql.SqlConverter;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.cml.Utils.randomIntString;
import static io.cml.Utils.randomTableSuffix;
import static io.cml.Utils.swallowException;
import static io.cml.metadata.MetadataUtil.createQualifiedObjectName;
import static io.cml.metrics.Metric.Filter.Operator.GREATER_THAN;
import static io.cml.metrics.MetricSql.Status.SUCCESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class AbstractTestMetricHook
{
    protected abstract MetricHook getMetricHook();

    protected abstract MetricStore getMetricStore();

    protected abstract Metadata getMetadata();

    protected abstract SqlConverter getSqlConverter();

    protected abstract void dropTables(List<SchemaTableName> createdTables);

    @Test
    public void testCreateMetric()
    {
        String filter = randomIntString();
        Metric metric = Metric.builder()
                .setName("metric" + randomTableSuffix())
                // '-' is not allowed in database(catalog) name in pg syntax, hence we quoted catalog name here.
                .setSource("\"canner-cml\".tpch_tiny.orders")
                .setType(Metric.Type.AVG)
                .setSql("o_totalprice")
                .setDimensions(Set.of("o_orderstatus"))
                .setTimestamp("o_orderdate")
                .setTimeGrains(Set.of(Metric.TimeGrain.MONTH))
                .setFilters(Set.of(new Metric.Filter("o_orderkey", GREATER_THAN, filter)))
                .build();
        SchemaTableName schemaTableName = null;
        try {
            getMetricHook().handleCreate(metric);
            List<MetricSql> metricSqls = getMetricStore().listMetricSqls(metric.getName());
            assertThat(metricSqls.size()).isEqualTo(1);
            MetricSql metricSql = metricSqls.get(0);
            assertThat(metricSql.getStatus()).isEqualTo(SUCCESS);
            schemaTableName = new SchemaTableName(getMetadata().getMaterializedViewSchema(), metricSql.getName());

            assertThat(getSqlConverter().convert(
                    "SELECT\n" +
                            "o_orderstatus,\n" +
                            "CAST(TRUNC(EXTRACT(YEAR FROM o_orderdate)) AS INTEGER) AS _col1,\n" +
                            "CAST(TRUNC(EXTRACT(MONTH FROM o_orderdate)) AS INTEGER) AS _col2,\n" +
                            "AVG(o_totalprice) AS _col3\n" +
                            "FROM \"canner-cml\".tpch_tiny.orders\n" +
                            "WHERE o_orderkey > " + filter + "\n" +
                            "GROUP BY 1, 2, 3", SessionContext.builder().build()))
                    .isEqualTo("SELECT o_orderstatus, CAST(`_col1` AS INT64) AS `_col1`, CAST(`_col2` AS INT64) AS `_col2`, `_col3`\n" +
                            "FROM `canner-cml`." + schemaTableName);
        }
        finally {
            List<SchemaTableName> createdTable = Collections.singletonList(schemaTableName);
            swallowException(() -> dropTables(createdTable));
            swallowException(() -> getMetricStore().dropMetric(metric.getName()));
        }
    }

    @Test
    public void testDropMetric()
    {
        String filter = randomIntString();
        Metric metric = Metric.builder()
                .setName("metric" + randomTableSuffix())
                // '-' is not allowed in database(catalog) name in pg syntax, hence we quoted catalog name here.
                .setSource("\"canner-cml\".tpch_tiny.orders")
                .setType(Metric.Type.AVG)
                .setSql("o_totalprice")
                .setDimensions(Set.of("o_orderstatus"))
                .setTimestamp("o_orderdate")
                .setTimeGrains(Set.of(Metric.TimeGrain.MONTH))
                .setFilters(Set.of(new Metric.Filter("o_orderkey", GREATER_THAN, filter)))
                .build();

        SchemaTableName schemaTableName = null;
        try {
            getMetricHook().handleCreate(metric);
            List<MetricSql> metricSqls = getMetricStore().listMetricSqls(metric.getName());
            assertThat(metricSqls.size()).isEqualTo(1);
            MetricSql metricSql = metricSqls.get(0);
            assertThat(metricSql.getStatus()).isEqualTo(SUCCESS);
            schemaTableName = new SchemaTableName(getMetadata().getMaterializedViewSchema(), metricSql.getName());

            getMetricHook().handleDrop(metric.getName());
            assertThat(getMetricStore().getMetric(metric.getName())).isEmpty();
            waitTableRemoved(QualifiedName.of(getMetadata().getDefaultCatalog(), getMetadata().getMaterializedViewSchema(), metricSql.getName()));

            // Aggregate and GroupBy won't be removed but sql will be formatted by calcite
            assertThat(getSqlConverter().convert("SELECT\n" +
                    "o_orderstatus,\n" +
                    "CAST(TRUNC(EXTRACT(YEAR FROM o_orderdate)) AS INTEGER) AS _col1,\n" +
                    "CAST(TRUNC(EXTRACT(MONTH FROM o_orderdate)) AS INTEGER) AS _col2,\n" +
                    "AVG(o_totalprice) AS _col3\n" +
                    "FROM \"canner-cml\".tpch_tiny.orders\n" +
                    "WHERE o_orderkey > " + filter + "\n" +
                    "GROUP BY 1, 2, 3", SessionContext.builder().build()))
                    .isEqualTo("SELECT o_orderstatus, CAST(TRUNC(EXTRACT(YEAR FROM o_orderdate)) AS INT64) AS `_col1`, " +
                            "CAST(TRUNC(EXTRACT(MONTH FROM o_orderdate)) AS INT64) AS `_col2`, AVG(o_totalprice) AS `_col3`\n" +
                            "FROM `canner-cml`.tpch_tiny.orders\n" +
                            "WHERE o_orderkey > " + filter + "\n" +
                            "GROUP BY o_orderstatus, CAST(TRUNC(EXTRACT(YEAR FROM o_orderdate)) AS INT64), CAST(TRUNC(EXTRACT(MONTH FROM o_orderdate)) AS INT64)");
        }
        finally {
            List<SchemaTableName> createdTable = Collections.singletonList(schemaTableName);
            swallowException(() -> dropTables(createdTable));
            swallowException(() -> getMetricStore().dropMetric(metric.getName()));
        }

        assertThatThrownBy(() -> getMetricHook().handleDrop("notfound"))
                .hasMessageFindingMatch("metric .* is not found");
    }

    protected void waitTableRemoved(QualifiedName tableName)
    {
        Optional<TableSchema> tableSchema;
        do {
            tableSchema = getTableSchema(tableName);
        }
        while (tableSchema.isPresent());
    }

    protected Optional<TableSchema> getTableSchema(QualifiedName tableName)
    {
        try {
            QualifiedObjectName name = createQualifiedObjectName(tableName, "", "");
            Optional<TableHandle> tableHandle = getMetadata().getTableHandle(name);
            return Optional.of(getMetadata().getTableSchema(tableHandle.get()));
        }
        catch (Exception e) {
            return Optional.empty();
        }
    }

    @Test
    public void testRefreshMetric()
    {
        String filter = randomIntString();
        Metric metric = Metric.builder()
                .setName("metric" + randomTableSuffix())
                // '-' is not allowed in database(catalog) name in pg syntax, hence we quoted catalog name here.
                .setSource("\"canner-cml\".tpch_tiny.orders")
                .setType(Metric.Type.AVG)
                .setSql("o_totalprice")
                .setDimensions(Set.of("o_orderstatus"))
                .setTimestamp("o_orderdate")
                .setTimeGrains(Set.of(Metric.TimeGrain.MONTH))
                .setFilters(Set.of(new Metric.Filter("o_orderkey", GREATER_THAN, filter)))
                .build();

        SchemaTableName schemaTableName = null;
        try {
            {
                getMetricHook().handleCreate(metric);
                List<MetricSql> metricSqls = getMetricStore().listMetricSqls(metric.getName());
                assertThat(metricSqls.size()).isEqualTo(1);
                MetricSql metricSql = metricSqls.get(0);
                assertThat(metricSql.getStatus()).isEqualTo(SUCCESS);
                schemaTableName = new SchemaTableName(getMetadata().getMaterializedViewSchema(), metricSql.getName());

                assertThat(getSqlConverter().convert(
                        "SELECT\n" +
                                "o_orderstatus,\n" +
                                "CAST(TRUNC(EXTRACT(YEAR FROM o_orderdate)) AS INTEGER) AS _col1,\n" +
                                "CAST(TRUNC(EXTRACT(MONTH FROM o_orderdate)) AS INTEGER) AS _col2,\n" +
                                "AVG(o_totalprice) AS _col3\n" +
                                "FROM \"canner-cml\".tpch_tiny.orders\n" +
                                "WHERE o_orderkey > " + filter + "\n" +
                                "GROUP BY 1, 2, 3", SessionContext.builder().build()))
                        .isEqualTo("SELECT o_orderstatus, CAST(`_col1` AS INT64) AS `_col1`, CAST(`_col2` AS INT64) AS `_col2`, `_col3`\n" +
                                "FROM `canner-cml`." + schemaTableName);
            }
            {
                String updatedFilter = randomIntString();
                Metric updated = Metric.from(metric)
                        .setFilters(Set.of(new Metric.Filter("o_orderkey", GREATER_THAN, updatedFilter)))
                        .build();
                getMetricHook().handleUpdate(updated);
                List<MetricSql> metricSqls = getMetricStore().listMetricSqls(metric.getName());
                assertThat(metricSqls.size()).isEqualTo(1);
                MetricSql metricSql = metricSqls.get(0);
                assertThat(metricSql.getStatus()).isEqualTo(SUCCESS);
                schemaTableName = new SchemaTableName(getMetadata().getMaterializedViewSchema(), metricSql.getName());

                assertThat(getSqlConverter().convert(
                        "SELECT\n" +
                                "o_orderstatus,\n" +
                                "CAST(TRUNC(EXTRACT(YEAR FROM o_orderdate)) AS INTEGER) AS _col1,\n" +
                                "CAST(TRUNC(EXTRACT(MONTH FROM o_orderdate)) AS INTEGER) AS _col2,\n" +
                                "AVG(o_totalprice) AS _col3\n" +
                                "FROM \"canner-cml\".tpch_tiny.orders\n" +
                                "WHERE o_orderkey > " + updatedFilter + "\n" +
                                "GROUP BY 1, 2, 3", SessionContext.builder().build()))
                        .isEqualTo("SELECT o_orderstatus, CAST(`_col1` AS INT64) AS `_col1`, CAST(`_col2` AS INT64) AS `_col2`, `_col3`\n" +
                                "FROM `canner-cml`." + schemaTableName);
            }
        }
        finally {
            List<SchemaTableName> createdTable = Collections.singletonList(schemaTableName);
            swallowException(() -> dropTables(createdTable));
            swallowException(() -> getMetricStore().dropMetric(metric.getName()));
        }

        Metric notfound = Metric.builder()
                .setName("notfound")
                // '-' is not allowed in database(catalog) name in pg syntax, hence we quoted catalog name here.
                .setSource("\"canner-cml\".tpch_tiny.orders")
                .setType(Metric.Type.AVG)
                .setSql("o_totalprice")
                .setDimensions(Set.of("o_orderstatus"))
                .setTimestamp("o_orderdate")
                .setTimeGrains(Set.of(Metric.TimeGrain.MONTH))
                .setFilters(Set.of(new Metric.Filter("o_orderkey", GREATER_THAN, "1")))
                .build();
        assertThatThrownBy(() -> getMetricHook().handleUpdate(notfound))
                .hasMessageFindingMatch("metric .* is not found");
    }
}
