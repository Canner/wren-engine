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

package io.cml.pgcatalog.regtype;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static io.cml.pgcatalog.OidHash.functionOid;
import static io.cml.pgcatalog.OidHash.oid;

public class TestingPgMetadata
        extends PgMetadata
{
    @Override
    protected List<RegObject> listRegProc()
    {
        return ImmutableList.of(new RegProc(functionOid("array_in"), "array_in__varchar____varchar"), new RegProc(functionOid("equals"), "equals__varchar_varchar___boolean"));
    }

    @Override
    protected List<RegObject> listRegClass()
    {
        return ImmutableList.of(new RegObjectImpl(oid("t1"), "t1"), new RegObjectImpl(oid("t2"), "t2"));
    }
}
