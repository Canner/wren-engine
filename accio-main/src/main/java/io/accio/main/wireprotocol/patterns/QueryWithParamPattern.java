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

package io.accio.main.wireprotocol.patterns;

import io.accio.base.Parameter;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class QueryWithParamPattern
{
    private final Pattern pattern;
    private final List<Parameter> expectedParameterList;

    protected QueryWithParamPattern(Pattern pattern, List<Parameter> expectedParameterList)
    {
        this.pattern = pattern;
        this.expectedParameterList = expectedParameterList;
    }

    protected abstract String rewrite(String statement);

    protected Matcher matcher(String statement)
    {
        return this.pattern.matcher(statement);
    }

    protected boolean matchParams(List<Parameter> actualParameterList)
    {
        if (this.expectedParameterList.size() != actualParameterList.size()) {
            return false;
        }

        for (int i = 0; i < this.expectedParameterList.size(); i++) {
            if (!this.expectedParameterList.get(i).equals(actualParameterList.get(i))) {
                return false;
            }
        }
        return true;
    }
}
