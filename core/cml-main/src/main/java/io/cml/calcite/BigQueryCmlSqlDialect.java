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

import io.airlift.log.Logger;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.dialect.BigQuerySqlDialect;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

public class BigQueryCmlSqlDialect
        extends BigQuerySqlDialect
{
    private final Logger LOG = Logger.get(BigQueryCmlSqlDialect.class);

    /**
     * Creates a BigQuerySqlDialect.
     *
     * @param context
     */
    public BigQueryCmlSqlDialect(Context context)
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

    @Override
    public SqlNode getCastSpec(final RelDataType type)
    {
        if (type instanceof BasicSqlType) {
            final SqlTypeName typeName = type.getSqlTypeName();
            switch (typeName) {
                // BigQuery only supports INT64 for integer types.
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                    return createSqlDataTypeSpecByName("INT64", typeName);
                // BigQuery only supports FLOAT64(aka. Double) for floating point types.
                case FLOAT:
                case DOUBLE:
                    return createSqlDataTypeSpecByName("FLOAT64", typeName);
                case DECIMAL:
                    if (type.getPrecision() <= 38) {
                        return createSqlDataTypeSpecByName("NUMERIC", typeName);
                    }
                    return createSqlDataTypeSpecByName("BIGNUMERIC", typeName);
                case BOOLEAN:
                    return createSqlDataTypeSpecByName("BOOL", typeName);
                case CHAR:
                case VARCHAR:
                    return createSqlDataTypeSpecByName("STRING", typeName);
                case BINARY:
                case VARBINARY:
                    return createSqlDataTypeSpecByName("BYTES", typeName);
                case DATE:
                    return createSqlDataTypeSpecByName("DATE", typeName);
                case TIME:
                    return createSqlDataTypeSpecByName("TIME", typeName);
                case TIMESTAMP:
                    return createSqlDataTypeSpecByName("TIMESTAMP", typeName);
                default:
                    break;
            }
        }
        return super.getCastSpec(type);
    }

    @Override
    public SqlNode rewriteSingleValueExpr(SqlNode aggCall)
    {
        final SqlNode operand = ((SqlBasicCall) aggCall).operand(0);
        final SqlNode anyValue = SqlStdOperatorTable.ANY_VALUE.createCall(null, SqlParserPos.ZERO, ((SqlBasicCall) aggCall).getOperandList());
        final SqlLiteral nullLiteral = SqlLiteral.createNull(SqlParserPos.ZERO);
        // For BigQuery, generate
        //   CASE COUNT(*)
        //   WHEN 0 THEN NULL
        //   WHEN 1 THEN ANY_VALUE(<result>)
        //   ELSE NULL
        //   END
        final SqlNode caseExpr =
                new SqlCase(SqlParserPos.ZERO,
                        SqlStdOperatorTable.COUNT.createCall(SqlParserPos.ZERO, operand),
                        SqlNodeList.of(
                                SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO),
                                SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)),
                        SqlNodeList.of(
                                nullLiteral,
                                anyValue),
                        nullLiteral);

        LOG.debug("SINGLE_VALUE rewritten into [{}]", caseExpr);

        return caseExpr;
    }

    @Override
    public StringBuilder quoteIdentifier(
            StringBuilder buf,
            String val)
    {
        if (val.startsWith("$")) {
            val = "_" + val;
        }
        if (identifierQuoteString == null // quoting is not supported
                || identifierEndQuoteString == null
                || identifierEscapedQuote == null
                || !identifierNeedsQuote(val)) {
            buf.append(val);
        }
        else {
            buf.append(identifierQuoteString);
            buf.append(val.replace(identifierEndQuoteString, identifierEscapedQuote));
            buf.append(identifierEndQuoteString);
        }
        return buf;
    }

    private static SqlDataTypeSpec createSqlDataTypeSpecByName(String typeAlias,
            SqlTypeName typeName)
    {
        SqlAlienSystemTypeNameSpec typeNameSpec = new SqlAlienSystemTypeNameSpec(
                typeAlias, typeName, SqlParserPos.ZERO);
        return new SqlDataTypeSpec(typeNameSpec, SqlParserPos.ZERO);
    }
}
