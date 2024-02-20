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

import io.accio.base.AccioException;

import java.util.regex.Matcher;

import static io.accio.base.metadata.StandardErrorCode.NOT_FOUND;
import static io.accio.base.pgcatalog.function.PgFunction.PG_FUNCTION_PATTERN;
import static java.lang.String.format;

public class RegProc
        implements RegObject
{
    private final long oid;
    private final String name;

    public RegProc(long oid, String signature)
    {
        this.oid = oid;
        Matcher matcher = PG_FUNCTION_PATTERN.matcher(signature);
        if (matcher.find()) {
            this.name = matcher.group("functionName");
        }
        else {
            throw new AccioException(NOT_FOUND, format("%s doensn't match PG_FUNCTION_PATTERN", signature));
        }
    }

    @Override
    public long getOid()
    {
        return oid;
    }

    @Override
    public String getName()
    {
        return name;
    }
}
