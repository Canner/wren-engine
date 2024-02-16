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

import com.google.common.collect.ImmutableList;
import io.accio.base.Parameter;
import io.accio.base.type.PGArray;
import io.accio.base.type.RecordType;
import io.accio.main.wireprotocol.Portal;
import io.accio.main.wireprotocol.PreparedStatement;

import java.util.List;
import java.util.Optional;

public final class PostgreSqlRewriteUtil
{
    private PostgreSqlRewriteUtil() {}

    private static final List<QueryPattern> PATTERNS = ImmutableList.<QueryPattern>builder()
            .add(CorrelatedSubQueryPattern.INSTANCE)
            .add(ShowTransIsoPattern.INSTANCE)
            .add(SetPattern.INSTANCE)
            .add(SetSessionPattern.INSTANCE)
            .add(ShowMaxIdentifierLengthPattern.INSTANCE)
            .add(DeallocatePattern.INSTANCE)
            .add(PgExtensionUpdatePathsPattern.INSTANCE)
            .add(ArraySelectPattern.INSTANCE)
            .add(ShowDateStylePattern.INSTANCE)
            .add(ShowStandardConformingPattern.INSTANCE)
            .add(MetabaseTablePrivilegesLegacy.INSTANCE)
            .add(MetabaseTablePrivilegesV0486.INSTANCE)
            .add(ShowTimezonePattern.INSTANCE)
            .add(JdbcGetProcedureColumnsPattern.INSTANCE)
            .build();

    private static final List<QueryWithParamPattern> WITH_PARAMS_PATTERNS = ImmutableList.<QueryWithParamPattern>builder()
            .add(new JdbcQueryUnkownTypeQueryPattern(PGArray.EMPTY_RECORD_ARRAY))
            .add(new JdbcQueryUnkownTypeQueryPattern(RecordType.EMPTY_RECORD))
            .add(new JdbcGetSQLTypeQueryPattern(PGArray.EMPTY_RECORD_ARRAY))
            .add(new JdbcGetSQLTypeQueryPattern(RecordType.EMPTY_RECORD))
            .add(new JdbcGetArrayDelimiterPattern(PGArray.EMPTY_RECORD_ARRAY))
            .add(new JdbcGetArrayElementOidPattern(PGArray.EMPTY_RECORD_ARRAY))
            .build();

    public static String rewrite(String statement)
    {
        return PATTERNS.stream()
                .filter(pattern -> pattern.matcher(statement).find())
                .findFirst()
                .map(pattern -> pattern.rewrite(statement))
                .orElse(statement);
    }

    public static Portal rewriteWithParameters(Portal portal)
    {
        String statement = portal.getPreparedStatement().getStatement();
        List<Parameter> parameters = portal.getParameters();
        Optional<String> rewrittenSql = WITH_PARAMS_PATTERNS.stream()
                .filter(pattern -> pattern.matcher(statement).find() && pattern.matchParams(parameters))
                .findFirst()
                .map(pattern -> pattern.rewrite(statement));

        PreparedStatement preparedStatement = new PreparedStatement(portal.getPreparedStatement().getName(),
                rewrittenSql.orElse(statement),
                portal.getPreparedStatement().getCacheStatement(),
                rewrittenSql.isPresent() ? List.of() : portal.getPreparedStatement().getParamTypeOids(),
                portal.getPreparedStatement().getOriginalStatement(),
                portal.getPreparedStatement().isSessionCommand(),
                portal.getPreparedStatement().getQueryLevel());

        return new Portal(
                portal.getName(),
                preparedStatement,
                rewrittenSql.isPresent() ? List.of() : portal.getParametersValue(),
                portal.getResultFormatCodes(),
                preparedStatement.getQueryLevel());
    }
}
