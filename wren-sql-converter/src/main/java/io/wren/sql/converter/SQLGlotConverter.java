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

package io.wren.sql.converter;

import io.wren.base.SessionContext;
import io.wren.base.sql.SqlConverter;
import io.wren.sql.glot.SQLGlot;

import java.io.IOException;

public class SQLGlotConverter
        implements SqlConverter
{
    @Override
    public String convert(String sql, SessionContext sessionContext)
    {
        SQLGlot.Dialect readDialect = SQLGlot.Dialect.valueOf(sessionContext.getReadDialect().get().toUpperCase());
        SQLGlot.Dialect writeDialect = SQLGlot.Dialect.valueOf(sessionContext.getWriteDialect().get().toUpperCase());
        try {
            return SQLGlot.transpile(sql, readDialect, writeDialect);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}