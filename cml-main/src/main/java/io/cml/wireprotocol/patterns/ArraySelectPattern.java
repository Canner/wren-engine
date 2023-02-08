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

import io.cml.spi.CmlException;

import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.cml.spi.metadata.StandardErrorCode.SYNTAX_ERROR;
import static java.util.regex.Pattern.CASE_INSENSITIVE;

public class ArraySelectPattern
        extends QueryPattern
{
    static final QueryPattern INSTANCE = new ArraySelectPattern();

    private ArraySelectPattern()
    {
        super(Pattern.compile("(ARRAY[ \n]*\\([ \n]*SELECT)", CASE_INSENSITIVE));
    }

    @Override
    protected String rewrite(String stmt)
    {
        // This is for resolving psql command `/dg`.
        // If the user use `array(select statement)`, we will rewrite it to `array[(select statement)]`.
        // But this is only a partial support, if select statement return several rows, array can't handle it.
        Matcher matcher = matcher(stmt);
        while (matcher.find()) {
            String matchStmt = matcher.group();
            int frontBracket = stmt.indexOf(matchStmt) + matchStmt.indexOf('(');
            int backBracket = getBackBracketIndex(stmt, frontBracket);
            stmt = stmt.substring(0, frontBracket) + "[" +
                    stmt.substring(frontBracket, backBracket + 1) + "]" +
                    stmt.substring(backBracket + 1);
        }
        return stmt;
    }

    private static int getBackBracketIndex(String stmt, int frontBracket)
    {
        Stack<Character> brackets = new Stack<>();
        for (int i = frontBracket; i < stmt.length(); i++) {
            switch (stmt.charAt(i)) {
                case '(':
                    brackets.push('(');
                    break;
                case ')':
                    brackets.pop();
                    if (brackets.empty()) {
                        return i;
                    }
            }
        }
        throw new CmlException(SYNTAX_ERROR, "lack of bracket ')'");
    }
}
