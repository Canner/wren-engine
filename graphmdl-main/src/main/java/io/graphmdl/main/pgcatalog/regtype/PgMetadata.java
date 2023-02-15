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

package io.graphmdl.main.pgcatalog.regtype;

import io.graphmdl.base.GraphMDLException;
import io.graphmdl.main.sql.PgOidTypeTableInfo;

import java.util.List;

import static io.graphmdl.base.metadata.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;

public abstract class PgMetadata
{
    public List<RegObject> list(PgOidTypeTableInfo pgOidTypeTableInfo)
    {
        switch (pgOidTypeTableInfo) {
            case REGCLASS:
                return listRegClass();
            case REGPROC:
                return listRegProc();
        }
        throw new GraphMDLException(NOT_FOUND, format("Undefined oid type %s", pgOidTypeTableInfo.name()));
    }

    protected abstract List<RegObject> listRegProc();

    protected abstract List<RegObject> listRegClass();
}
