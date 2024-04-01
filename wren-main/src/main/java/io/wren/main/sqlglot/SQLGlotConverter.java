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

package io.wren.main.sqlglot;

import io.airlift.log.Logger;
import io.wren.base.SessionContext;
import io.wren.base.WrenException;
import io.wren.base.sql.SqlConverter;

import java.io.IOException;

import static io.wren.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class SQLGlotConverter
        implements SqlConverter
{
    private static final Logger LOG = Logger.get(SQLGlotConverter.class);

    private final SQLGlot sqlGlot;
    private final SQLGlot.Dialect readDialect;
    private final SQLGlot.Dialect writeDialect;

    private SQLGlotConverter(
            SQLGlot sqlGlot,
            SQLGlot.Dialect readDialect,
            SQLGlot.Dialect writeDialect)
    {
        this.sqlGlot = requireNonNull(sqlGlot, "sqlGlot is null");
        this.readDialect = requireNonNull(readDialect, "readDialect is null");
        this.writeDialect = requireNonNull(writeDialect, "writeDialect is null");
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @Override
    public String convert(String sql, SessionContext sessionContext)
    {
        try {
            LOG.info("[Input sql]: %s", sql);
            String dialectSql = sqlGlot.transpile(sql, readDialect, writeDialect);
            LOG.info("[Dialect sql]: %s", dialectSql);
            return dialectSql;
        }
        catch (IOException e) {
            throw new WrenException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public static class Builder
    {
        private SQLGlot sqlGlot;
        private SQLGlot.Dialect readDialect = SQLGlot.Dialect.TRINO;
        private SQLGlot.Dialect writeDialect;

        public Builder setSQLGlot(SQLGlot sqlGlot)
        {
            this.sqlGlot = sqlGlot;
            return this;
        }

        public Builder setReadDialect(SQLGlot.Dialect readDialect)
        {
            this.readDialect = readDialect;
            return this;
        }

        public Builder setWriteDialect(SQLGlot.Dialect writeDialect)
        {
            this.writeDialect = writeDialect;
            return this;
        }

        public SQLGlotConverter build()
        {
            return new SQLGlotConverter(sqlGlot, readDialect, writeDialect);
        }
    }
}
