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

package io.accio.main.wireprotocol;

import java.util.List;
import java.util.Optional;

public class PreparedStatement
{
    public static final String CANNERFLOW_RESERVED_PREPARE_NAME = "canner_f1ow";

    private final String name;
    private final String statement;
    private final Optional<String> cacheStatement;
    private final List<Integer> paramTypeOids;
    private final String originalStatement;
    private final boolean isSessionCommand;

    public PreparedStatement(
            String name,
            String statement,
            List<Integer> paramTypeOids,
            String originalStatement,
            boolean isSessionCommand)
    {
        this(name, statement, Optional.empty(), paramTypeOids, originalStatement, isSessionCommand);
    }

    public PreparedStatement(
            String name,
            String statement,
            Optional<String> cacheStatement,
            List<Integer> paramTypeOids,
            String originalStatement,
            boolean isSessionCommand)
    {
        this.name = name.isEmpty() ? CANNERFLOW_RESERVED_PREPARE_NAME : name;
        this.statement = statement;
        this.cacheStatement = cacheStatement;
        this.paramTypeOids = paramTypeOids;
        this.originalStatement = originalStatement;
        this.isSessionCommand = isSessionCommand;
    }

    public String getName()
    {
        return name;
    }

    public String getStatement()
    {
        return statement;
    }

    public List<Integer> getParamTypeOids()
    {
        return paramTypeOids;
    }

    public String getOriginalStatement()
    {
        return originalStatement;
    }

    public boolean isSessionCommand()
    {
        return isSessionCommand;
    }

    public Optional<String> getCacheStatement()
    {
        return cacheStatement;
    }
}
