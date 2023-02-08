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

import java.util.regex.Pattern;

public class PgExtensionUpdatePathsPattern
        extends QueryPattern
{
    static final QueryPattern INSTANCE = new PgExtensionUpdatePathsPattern();

    public PgExtensionUpdatePathsPattern()
    {
        // Avoiding to pop-up an error message when refreshing data source in DataGrip.
        // Trino didn't support table function and array by table subquery expression.
        // Thus, rewrite this function through hard code for now.
        super(Pattern.compile("pg_extension_update_paths"));
    }

    @Override
    protected String rewrite(String statement)
    {
        String replaced = ArraySelectPattern.INSTANCE.matcher(statement).find() ? ArraySelectPattern.INSTANCE.rewrite(statement) : statement;
        return replaced.replaceFirst(
                "(\\()?select target" +
                        "([\\n ]+)+from pg_extension_update_paths\\(extname\\)" +
                        "([\\n ]+)+where source = extversion" +
                        "([\\n ]+)+and path is not null(\\))?",
                "array[]");
    }
}
