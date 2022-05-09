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

package io.cml.pgcatalog;

import java.nio.charset.StandardCharsets;

import static org.apache.lucene.util.StringHelper.murmurhash3_x86_32;

public final class OidHash
{
    public enum Type
    {
        PROC
    }

    private OidHash() {}

    public static int oid(String key)
    {
        byte[] b = key.getBytes(StandardCharsets.UTF_8);
        return murmurhash3_x86_32(b, 0, b.length, 0);
    }

    public static int functionOid(String name)
    {
        return oid(Type.PROC + name);
    }
}
