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
import io.cml.spi.CmlException;

import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.cml.spi.metadata.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;

public class RegProc
        implements RegObject
{
    public static final Pattern PG_FUNCTION_PATTERN = Pattern.compile("(?<functionName>[a-zA-Z]+(_[a-zA-Z0-9]+)*)(__(?<argsType>[a-zA-Z]+(_[a-zA-Z0-9]+)*))?(___(?<returnType>[a-zA-Z]+(_[a-zA-Z0-9]+)*))?");

    private final long oid;
    private final String name;
    private final String returnType;
    private final List<String> argumentTypes;

    public RegProc(long oid, String signature)
    {
        this.oid = oid;
        Matcher matcher = PG_FUNCTION_PATTERN.matcher(signature);
        if (matcher.find()) {
            this.name = matcher.group("functionName");
            this.argumentTypes = Optional.ofNullable(matcher.group("argsType")).map(args -> ImmutableList.copyOf(args.split("_"))).orElse(ImmutableList.of());
            this.returnType = matcher.group("returnType");
        }
        else {
            throw new CmlException(NOT_FOUND, format("%s is not found", signature));
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

    public String getReturnType()
    {
        return returnType;
    }

    public List<String> getArgumentTypes()
    {
        return argumentTypes;
    }
}
