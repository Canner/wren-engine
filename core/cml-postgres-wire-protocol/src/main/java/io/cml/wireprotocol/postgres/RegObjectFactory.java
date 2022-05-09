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

package io.cml.wireprotocol.postgres;

import com.google.common.collect.Streams;
import io.cml.pgcatalog.PgCatalogTableManager;
import io.cml.spi.CmlException;

import java.util.Iterator;
import java.util.Optional;

import static io.cml.wireprotocol.PostgresWireProtocolErrorCode.UNDEFINED_FUNCTION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class RegObjectFactory
{
    private final PgCatalogTableManager pgCatalogTableManager;
    private final String tableName;

    protected RegObjectFactory(PgCatalogTableManager pgCatalogTableManager, String tableName)
    {
        this.pgCatalogTableManager = requireNonNull(pgCatalogTableManager, "pgCatalogTableManager is null");
        this.tableName = tableName;
    }

    public String of(String objName)
    {
        requireNonNull(objName, "obj name can't be null");
        Optional<Object[]> checkResult = Streams.stream(this::getPgTableRecords).filter(row -> row[1].equals(objName)).findFirst();
        if (checkResult.isEmpty()) {
            throw new CmlException(UNDEFINED_FUNCTION, format("%s does not exist", objName));
        }
        return objName;
    }

    public String of(int oid)
    {
        Optional<Object[]> result = Streams.stream(this::getPgTableRecords).filter(row -> row[0].equals(oid)).findFirst();
        if (result.isEmpty()) {
            throw new CmlException(UNDEFINED_FUNCTION, format("RegObject oid %s does not exist", oid));
        }
        return (String) result.get()[1];
    }

    public Optional<String> of(int oid, String objName)
    {
        // To match PostgreSQL behavior 1:1 this would need to lookup the
        // function name by oid and fallback to using the oid as name if there is
        // no match.
        // It looks like for compatibility with clients it is good enough
        // to not mirror this behavior.
        requireNonNull(objName, "obj name can't be null");
        Optional<Object[]> checkResult = Streams.stream(this::getPgTableRecords).filter(row -> row[0].equals(oid) && row[1].equals(objName)).findFirst();
        if (checkResult.isPresent()) {
            return Optional.of(objName);
        }
        return Optional.empty();
    }

    private Iterator<Object[]> getPgTableRecords()
    {
        throw new UnsupportedOperationException();
    }
}
