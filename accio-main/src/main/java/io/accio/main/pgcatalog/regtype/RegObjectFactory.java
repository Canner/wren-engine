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

package io.accio.main.pgcatalog.regtype;

import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;

import static io.accio.main.sql.PgOidTypeTableInfo.REGCLASS;
import static io.accio.main.sql.PgOidTypeTableInfo.REGPROC;
import static java.util.Locale.ROOT;

public final class RegObjectFactory
{
    private final Map<String, AbstractRegObjectFactory> factoryMap;

    @Inject
    public RegObjectFactory(PgMetadata pgMetadata)
    {
        this.factoryMap = ImmutableMap.<String, AbstractRegObjectFactory>builder()
                .put(REGCLASS.name(), new RegClassFactory(pgMetadata))
                .put(REGPROC.name(), new RegProcFactory(pgMetadata))
                .build();
    }

    public RegObject of(String type, String value)
    {
        return factoryMap.get(type.toUpperCase(ROOT)).of(value);
    }

    public RegObject of(String type, int oid)
    {
        return factoryMap.get(type.toUpperCase(ROOT)).of(oid);
    }

    public Optional<RegObject> of(String type, int oid, String name)
    {
        return factoryMap.get(type.toUpperCase(ROOT)).of(oid, name);
    }
}
