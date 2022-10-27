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

import static io.cml.spi.type.PGArray.REGOPERATOR_ARRAY;

public class RegoperatorType
        extends RegObjectType
{
    public static final RegoperatorType REGOPERATOR = new RegoperatorType();

    private static final int OID = 2204;

    RegoperatorType()
    {
        super(OID, "regoperator");
    }

    @Override
    public int typArray()
    {
        return REGOPERATOR_ARRAY.oid();
    }
}
