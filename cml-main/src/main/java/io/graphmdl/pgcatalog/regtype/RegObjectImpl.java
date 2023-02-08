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

package io.graphmdl.pgcatalog.regtype;

public class RegObjectImpl
        implements RegObject
{
    private final long oid;
    private final String name;

    public RegObjectImpl(long oid, String name)
    {
        this.oid = oid;
        this.name = name;
    }

    public long getOid()
    {
        return oid;
    }

    public String getName()
    {
        return name;
    }
}
