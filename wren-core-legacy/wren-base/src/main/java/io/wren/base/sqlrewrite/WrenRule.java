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

package io.wren.base.sqlrewrite;

import io.trino.sql.tree.Statement;
import io.wren.base.AnalyzedMDL;
import io.wren.base.SessionContext;
import io.wren.base.sqlrewrite.analyzer.Analysis;

public interface WrenRule
{
    Statement apply(Statement root, SessionContext sessionContext, AnalyzedMDL analyzedMDL);

    Statement apply(Statement root, SessionContext sessionContext, Analysis analysis, AnalyzedMDL analyzedMDL);
}
