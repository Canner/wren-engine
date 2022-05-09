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

package io.cml.wireprotocol.patterns;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Locale.ENGLISH;

public class SetSessionPropertyPattern
        extends QueryPattern
{
    static final QueryPattern INSTANCE = new SetSessionPropertyPattern();

    private SetSessionPropertyPattern()
    {
        super(Pattern.compile("(?i)^ *SET +SESSION +([a-zA-Z0-9_]+)( *=| +TO)"));
    }

    @Override
    protected String rewrite(String statement)
    {
        Matcher matcher = matcher(statement);
        if (matcher.find()) {
            String property = matcher.group(1).toLowerCase(ENGLISH);
            return matcher.replaceFirst("SET SESSION " + property + " =");
        }
        return statement;
    }
}
