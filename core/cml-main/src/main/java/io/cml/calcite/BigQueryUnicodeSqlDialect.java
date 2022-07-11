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

package io.cml.calcite;

import org.apache.calcite.sql.dialect.BigQuerySqlDialect;

public class BigQueryUnicodeSqlDialect
        extends BigQuerySqlDialect
{
    /**
     * Creates a BigQuerySqlDialect.
     *
     * @param context
     */
    public BigQueryUnicodeSqlDialect(Context context)
    {
        super(context);
    }

    @Override
    public void quoteStringLiteralUnicode(StringBuilder buf, String val)
    {
        // refer to https://blog.csdn.net/weixin_39133753/article/details/115470036
        buf.append(literalQuoteString);
        buf.append(val.replace(literalEndQuoteString, literalEscapedQuote));
        buf.append(literalQuoteString);
    }
}
