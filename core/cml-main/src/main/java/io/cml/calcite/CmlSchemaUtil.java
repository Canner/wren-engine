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

package io.cml.calcite;

import io.cml.metadata.ColumnSchema;
import io.cml.metadata.Metadata;
import io.cml.metadata.TableSchema;
import io.cml.spi.type.PGType;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlDialect;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.cml.calcite.BigQueryCmlSqlDialect.DEFAULT_CONTEXT;
import static io.cml.spi.type.BigIntType.BIGINT;
import static io.cml.spi.type.BooleanType.BOOLEAN;
import static io.cml.spi.type.DateType.DATE;
import static io.cml.spi.type.DoubleType.DOUBLE;
import static io.cml.spi.type.IntegerType.INTEGER;
import static io.cml.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static org.apache.calcite.jdbc.CalciteSchema.createRootSchema;

public final class CmlSchemaUtil
{
    private CmlSchemaUtil() {}

    public enum Dialect
    {
        BIGQUERY(new BigQueryCmlSqlDialect(DEFAULT_CONTEXT));

        private final SqlDialect sqlDialect;

        Dialect(SqlDialect sqlDialect)
        {
            this.sqlDialect = sqlDialect;
        }

        public SqlDialect getSqlDialect()
        {
            return sqlDialect;
        }
    }

    public static SchemaPlus schemaPlus(List<TableSchema> tableSchemas, Metadata metadata)
    {
        SchemaPlus rootSchema = createRootSchema(true, true, "").plus();
        tableSchemas.stream()
                .collect(groupingBy(tableSchema -> tableSchema.getCatalogName().getCatalogName(),
                        groupingBy(tableSchema -> tableSchema.getTableSchema().getTable().getSchemaName(),
                                mapping(tableSchema -> Map.entry(tableSchema.getTableSchema().getTable().getTableName(), toCmlTable(tableSchema, metadata)),
                                        toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)))))
                .forEach((catalogName, schemaTableMap) -> {
                    SchemaPlus secondSchema = rootSchema.add(catalogName, new AbstractSchema());
                    schemaTableMap.forEach((schemaName, cmlTableMap) -> secondSchema.add(schemaName, new CmlSchema(cmlTableMap)));
                });
        return rootSchema;
    }

    private static CmlTable toCmlTable(TableSchema tableSchema, Metadata metadata)
    {
        JavaTypeFactoryImpl typeFactory = new CustomCharsetJavaTypeFactoryImpl(UTF_8, metadata.getRelDataTypeSystem());
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        for (ColumnSchema columnSchema : tableSchema.getColumns()) {
            builder.add(columnSchema.getName(), toRelDataType(typeFactory, columnSchema.getType()));
        }

        return new CmlTable(tableSchema.getTable().getTableName(), builder.build());
    }

    // TODO: handle nested types
    private static RelDataType toRelDataType(JavaTypeFactory typeFactory, PGType<?> pgType)
    {
        if (pgType.equals(BOOLEAN)) {
            return typeFactory.createJavaType(Boolean.class);
        }
        if (pgType.equals(INTEGER)) {
            return typeFactory.createJavaType(Integer.class);
        }
        if (pgType.equals(BIGINT)) {
            return typeFactory.createJavaType(Long.class);
        }
        if (pgType.equals(VARCHAR)) {
            return typeFactory.createJavaType(String.class);
        }
        if (pgType.equals(DOUBLE)) {
            return typeFactory.createJavaType(Double.class);
        }
        if (pgType.equals(DATE)) {
            return typeFactory.createJavaType(Date.class);
        }
        throw new UnsupportedOperationException(pgType.typName() + " not supported yet");
    }
}
