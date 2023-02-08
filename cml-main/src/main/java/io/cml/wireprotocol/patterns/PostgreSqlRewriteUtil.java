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

import com.google.common.collect.ImmutableList;

import java.util.List;

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
            .build();

    public static String rewrite(String statement)
    {
        return PATTERNS.stream()
                .filter(pattern -> pattern.matcher(statement).find())
                .findFirst()
                .map(pattern -> pattern.rewrite(statement))
                .orElse(statement);
    }
}
