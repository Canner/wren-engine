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

package io.cml.spi.type;

public class RegtypeType
        extends RegObjectType
{
    public static final RegtypeType REGTYPE = new RegtypeType();

    private static final int OID = 2206;

    RegtypeType()
    {
        super(OID, "regtype");
    }

    @Override
    public int typArray()
    {
        return PGArray.REGTYPE_ARRAY.oid();
    }
}
