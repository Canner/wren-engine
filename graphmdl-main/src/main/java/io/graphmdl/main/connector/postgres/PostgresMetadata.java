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

package io.graphmdl.main.connector.postgres;

import com.google.common.collect.ImmutableList;
import io.graphmdl.base.Column;
import io.graphmdl.base.ConnectorRecordIterator;
import io.graphmdl.base.GraphMDLException;
import io.graphmdl.base.Parameter;
import io.graphmdl.base.metadata.TableMetadata;
import io.graphmdl.connector.postgres.PostgresClient;
import io.graphmdl.connector.postgres.PostgresRecordIterator;
import io.graphmdl.main.metadata.Metadata;
import io.trino.sql.tree.QualifiedName;

import javax.inject.Inject;

import java.util.List;

import static io.graphmdl.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class PostgresMetadata
        implements Metadata
{
    private final PostgresClient postgresClient;

    @Inject
    public PostgresMetadata(PostgresClient postgresClient)
    {
        this.postgresClient = requireNonNull(postgresClient, "postgresClient is null");
    }

    @Override
    public void createSchema(String name)
    {
        postgresClient.executeDDL("CREATE SCHEMA IF NOT EXISTS " + name);
    }

    @Override
    public boolean isSchemaExist(String name)
    {
        try (PostgresRecordIterator iterator = PostgresRecordIterator.of(postgresClient, format("select count(*) = 1 from pg_catalog.pg_namespace where nspname = '%s'", name))) {
            return (boolean) iterator.next()[0];
        }
        catch (Exception e) {
            throw new GraphMDLException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    @Override
    public List<String> listSchemas()
    {
        try (PostgresRecordIterator iterator = PostgresRecordIterator.of(postgresClient, "select distinct nspanme from pg_catalog.pg_namespace'")) {
            ImmutableList.Builder<String> builder = ImmutableList.builder();
            while (iterator.hasNext()) {
                builder.add((String) iterator.next()[0]);
            }
            return builder.build();
        }
        catch (Exception e) {
            throw new GraphMDLException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    @Override
    public List<TableMetadata> listTables(String schemaName)
    {
        return postgresClient.listTable(schemaName);
    }

    @Override
    public List<String> listFunctionNames(String schemaName)
    {
        try (PostgresRecordIterator iterator = PostgresRecordIterator.of(postgresClient, "select distinct proname from pg_catalog.pg_proc where pronamespace =" +
                "(select oid from pg_catalog.pg_namespace where nspname = 'pg_catalog' OR nspname = '" + schemaName + "')")) {
            ImmutableList.Builder<String> builder = ImmutableList.builder();
            while (iterator.hasNext()) {
                builder.add((String) iterator.next()[0]);
            }
            return builder.build();
        }
        catch (Exception e) {
            throw new GraphMDLException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    @Override
    public QualifiedName resolveFunction(String functionName, int numArgument)
    {
        return QualifiedName.of(functionName);
    }

    @Override
    public String getDefaultCatalog()
    {
        try (PostgresRecordIterator iterator = PostgresRecordIterator.of(postgresClient, "select current_database()")) {
            return (String) iterator.next()[0];
        }
        catch (Exception e) {
            throw new GraphMDLException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    @Override
    public void directDDL(String sql)
    {
        postgresClient.executeDDL(sql);
    }

    @Override
    public ConnectorRecordIterator directQuery(String sql, List<Parameter> parameters)
    {
        try {
            return new PostgresConnectorRecordIterator(PostgresRecordIterator.of(postgresClient, sql, parameters));
        }
        catch (Exception e) {
            throw new GraphMDLException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    @Override
    public List<Column> describeQuery(String sql, List<Parameter> parameters)
    {
        return postgresClient.describe(sql, parameters).stream()
                .map(columnMetadata -> new Column(columnMetadata.getName(), columnMetadata.getType()))
                .collect(toList());
    }

    @Override
    public boolean isPgCompatible()
    {
        return true;
    }
}
