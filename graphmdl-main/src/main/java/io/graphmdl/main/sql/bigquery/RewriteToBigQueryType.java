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

package io.graphmdl.main.sql.bigquery;

import io.graphmdl.main.metadata.Metadata;
import io.graphmdl.main.sql.SqlRewrite;
import io.graphmdl.sqlrewrite.BaseRewriter;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CharLiteral;
import io.trino.sql.tree.DataTypeParameter;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.NumericParameter;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.StringLiteral;

import java.util.List;
import java.util.Optional;

import static io.graphmdl.sqlrewrite.Utils.parseType;
import static java.lang.Integer.parseInt;

public class RewriteToBigQueryType
        implements SqlRewrite
{
    public static final RewriteToBigQueryType INSTANCE = new RewriteToBigQueryType();

    private RewriteToBigQueryType() {}

    @Override
    public Node rewrite(Node node, Metadata metadata)
    {
        RewriteToBigQueryTypeRewriter rewriter = new RewriteToBigQueryTypeRewriter();
        return rewriter.process(node);
    }

    private static class RewriteToBigQueryTypeRewriter
            extends BaseRewriter<Void>
    {
        @Override
        protected Node visitGenericDataType(GenericDataType genericDataType, Void context)
        {
            return toBigQueryGenericDataType(genericDataType);
        }

        @Override
        protected Node visitGenericLiteral(GenericLiteral node, Void context)
        {
            // Queries with a SELECT statement that includes [type] [literal] cannot be executed in BigQuery.
            // To overcome this limitation, we convert the query to CAST([literal] AS [type]).
            // When working with JSON data, it is necessary to first use SAFE.PARSE_JSON to parse StringLiteral.
            if (node.getType().equalsIgnoreCase("JSON")) {
                // If there is the '\"' character in the JSON string, BigQuery will try to find another '\"' in the following string, so we need to escape it.
                String value = node.getValue().replace("\"", "\\\"");
                return new Cast(
                        new FunctionCall(
                                QualifiedName.of("SAFE", "PARSE_JSON"),
                                List.of(new StringLiteral(value))),
                        new GenericDataType(Optional.empty(), new Identifier("JSON"), List.of()));
            }
            return new Cast(
                    visitAndCast(new StringLiteral(node.getValue()), context),
                    visitAndCast(parseType(node.getType()), context));
        }

        @Override
        protected Node visitBinaryLiteral(BinaryLiteral node, Void context)
        {
            // PostgreSQL uses the following format to represent binary data: \x[hexadecimal string], but BigQuery don't support this format.
            // To overcome this limitation, we convert the query to CAST(FROM_HEX(hex string) AS BYTES).
            return new Cast(
                    new FunctionCall(QualifiedName.of("FROM_HEX"), List.of(new StringLiteral(node.toHexString()))),
                    new GenericDataType(Optional.empty(), new Identifier("BYTES"), List.of()));
        }

        @Override
        protected Node visitDecimalLiteral(DecimalLiteral node, Void context)
        {
            return new Cast(
                    new StringLiteral(node.getValue()),
                    new GenericDataType(Optional.empty(), new Identifier("NUMERIC"), List.of()));
        }

        @Override
        protected Node visitCharLiteral(CharLiteral node, Void context)
        {
            return new Cast(
                    new StringLiteral(node.getValue()),
                    new GenericDataType(Optional.empty(), new Identifier("STRING"), List.of()));
        }

        private GenericDataType toBigQueryGenericDataType(GenericDataType genericDataType)
        {
            Optional<NodeLocation> nodeLocation = genericDataType.getLocation();
            String typeName = genericDataType.getName().getCanonicalValue();
            List<DataTypeParameter> parameters = visitNodes(genericDataType.getArguments(), null);
            switch (typeName) {
                // BigQuery only supports INT64 for integer types.
                case "TINYINT":
                case "SMALLINT":
                case "INTEGER":
                case "BIGINT":
                    return new GenericDataType(nodeLocation, new Identifier("INT64"), parameters);
                // BigQuery only supports FLOAT64(aka. Double) for floating point types.
                case "REAL":
                case "FLOAT":
                case "DOUBLE":
                    return new GenericDataType(nodeLocation, new Identifier("FLOAT64"), parameters);
                case "DECIMAL":
                    if (genericDataType.getArguments().size() == 2
                            && genericDataType.getArguments().get(0) instanceof NumericParameter) {
                        NumericParameter precision = (NumericParameter) genericDataType.getArguments().get(0);
                        NumericParameter scale = (NumericParameter) genericDataType.getArguments().get(1);
                        if (parseInt(precision.getValue()) - parseInt(scale.getValue()) <= 29 && parseInt(scale.getValue()) <= 9) {
                            return new GenericDataType(nodeLocation, new Identifier("NUMERIC"), parameters);
                        }
                    }
                    return new GenericDataType(nodeLocation, new Identifier("BIGNUMERIC"), parameters);
                case "BOOLEAN":
                    return new GenericDataType(nodeLocation, new Identifier("BOOL"), parameters);
                case "UUID":
                case "NAME":
                case "TEXT":
                case "CHAR":
                case "VARCHAR":
                    return new GenericDataType(nodeLocation, new Identifier("STRING"), parameters);
                case "BYTEA":
                case "BINARY":
                case "VARBINARY":
                    return new GenericDataType(nodeLocation, new Identifier("BYTES"), parameters);
                case "JSON":
                    return new GenericDataType(nodeLocation, new Identifier("JSON"), parameters);
                case "ARRAY":
                    return new GenericDataType(nodeLocation, new Identifier("ARRAY"), parameters);
                case "DATE":
                    return new GenericDataType(nodeLocation, new Identifier("DATE"), parameters);
                case "INTERVAL":
                    return new GenericDataType(nodeLocation, new Identifier("INTERVAL"), parameters);
                default:
                    throw new UnsupportedOperationException("Unsupported type: " + typeName);
            }
        }
    }
}
