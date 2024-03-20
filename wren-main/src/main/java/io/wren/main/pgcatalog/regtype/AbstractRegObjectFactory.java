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

package io.wren.main.pgcatalog.regtype;

import com.google.common.collect.Streams;
import io.wren.base.WrenException;
import io.wren.main.sql.PgOidTypeTableInfo;

import java.util.Iterator;
import java.util.Optional;

import static io.wren.base.metadata.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class AbstractRegObjectFactory
{
    protected final PgMetadata pgMetadata;
    protected final PgOidTypeTableInfo pgOidTypeTableInfo;

    protected AbstractRegObjectFactory(PgMetadata pgMetadata, PgOidTypeTableInfo pgOidTypeTableInfo)
    {
        this.pgMetadata = requireNonNull(pgMetadata, "pgMetadata is null");
        this.pgOidTypeTableInfo = requireNonNull(pgOidTypeTableInfo, "pgOidTypeTableInfo is null");
    }

    public RegObject of(String objName)
    {
        requireNonNull(objName, "obj name can't be null");
        Optional<RegObject> result = Streams.stream(this::getPgTableRecords).filter(regObject -> regObject.getName().equals(objName)).findFirst();
        if (result.isEmpty()) {
            throw new WrenException(NOT_FOUND, format("%s does not exist", objName));
        }
        return result.get();
    }

    public RegObject of(int oid)
    {
        Optional<RegObject> result = Streams.stream(this::getPgTableRecords).filter(regObject -> regObject.getOid() == oid).findFirst();
        if (result.isEmpty()) {
            throw new WrenException(NOT_FOUND, format("RegObject oid %s does not exist", oid));
        }
        return result.get();
    }

    public Optional<RegObject> of(int oid, String objName)
    {
        // To match PostgreSQL behavior 1:1 this would need to lookup the
        // function name by oid and fallback to using the oid as name if there is
        // no match.
        // It looks like for compatibility with clients it is good enough
        // to not mirror this behavior.
        requireNonNull(objName, "obj name can't be null");
        return Streams.stream(this::getPgTableRecords).filter(regObject -> regObject.getOid() == oid && regObject.getName().equals(objName)).findFirst();
    }

    protected Iterator<RegObject> getPgTableRecords()
    {
        return pgMetadata.list(pgOidTypeTableInfo).iterator();
    }
}
