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

package io.wren.main.sql.bigquery;

import io.trino.sql.tree.ArrayConstructor;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CharLiteral;
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.DataTypeParameter;
import io.trino.sql.tree.DateTimeDataType;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.NumericParameter;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.TypeParameter;
import io.wren.base.WrenException;
import io.wren.base.sqlrewrite.BaseRewriter;
import io.wren.base.type.PGArray;
import io.wren.main.metadata.Metadata;
import io.wren.main.sql.SqlRewrite;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.wren.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.wren.base.sqlrewrite.Utils.parseType;
import static io.wren.base.type.PGArray.allArray;
import static io.wren.connector.bigquery.BigQueryType.toBqType;
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
        protected Node visitCast(Cast node, Void context)
        {
            // Cast the value of the array first, because BigQuery is strict, for example we can't cast array<decimal> to array<float64>.
            // So we do the thing like, CAST(ARRAY[true, false] AS ARRAY(boolean)) -> CAST(ARRAY[CAST(true AS BOOLEAN, CAST(false AS BOOLEAN)] AS ARRAY<BOOLEAN>)
            if (node.getExpression() instanceof ArrayConstructor arrayConstructor &&
                    isArray(node.getType())) {
                DataType innerType = getArrayInnerType(node.getType());
                DataType bqType = getBqType(innerType);
                List<Expression> values = arrayConstructor.getValues().stream()
                        .map(value -> new Cast(
                                visitAndCast(value, context),
                                bqType))
                        .collect(Collectors.toList());
                if (arrayConstructor.getLocation().isPresent()) {
                    return new Cast(
                            arrayConstructor.getLocation().get(),
                            new ArrayConstructor(values),
                            visitAndCast(node.getType(), context));
                }
                return new Cast(
                        new ArrayConstructor(values),
                        visitAndCast(node.getType(), context));
            }
            return super.visitCast(node, context);
        }

        private boolean isArray(DataType type)
        {
            if (type instanceof GenericDataType genericDataType) {
                return genericDataType.getName().getCanonicalValue().equalsIgnoreCase("ARRAY");
            }
            return false;
        }

        private DataType getArrayInnerType(DataType type)
        {
            if (type instanceof GenericDataType genericDataType &&
                    genericDataType.getArguments().getFirst() instanceof TypeParameter typeParameter) {
                return typeParameter.getValue();
            }
            throw new WrenException(GENERIC_INTERNAL_ERROR, "Invalid array type: " + type);
        }

        private DataType getBqType(DataType type)
        {
            String typeName = type.toString();
            if (type instanceof GenericDataType gdType) {
                typeName = toBigQueryGenericDataType(gdType).getName().getCanonicalValue();
            }
            if (type instanceof DateTimeDataType dtType) {
                typeName = dtType.getType().name();
            }
            return parseType(typeName);
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
                case "INT2":
                case "INTEGER":
                case "INT4":
                case "BIGINT":
                case "INT8":
                    return new GenericDataType(nodeLocation, new Identifier("INT64"), parameters);
                // BigQuery only supports FLOAT64(aka. Double) for floating point types.
                case "FLOAT":
                case "REAL":
                case "FLOAT4":
                case "DOUBLE":
                case "FLOAT8":
                    return new GenericDataType(nodeLocation, new Identifier("FLOAT64"), parameters);
                case "DECIMAL":
                case "NUMERIC":
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
                case "BOOL":
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
                    DataType innerType = getArrayInnerType(genericDataType);
                    DataType bqType = getBqType(innerType);
                    return new GenericDataType(nodeLocation, new Identifier("ARRAY"), List.of(new TypeParameter(bqType)));
                case "DATE":
                    return new GenericDataType(nodeLocation, new Identifier("DATE"), parameters);
                case "INTERVAL":
                    return new GenericDataType(nodeLocation, new Identifier("INTERVAL"), parameters);
                case "TIMESTAMP":
                    return new GenericDataType(nodeLocation, new Identifier("TIMESTAMP"), parameters);
                default:
                    if (typeName.startsWith("_")) {
                        PGArray pgArray = getPgArrayType(typeName);
                        return new GenericDataType(nodeLocation, new Identifier("ARRAY"),
                                List.of(
                                        new TypeParameter(new GenericDataType(nodeLocation, new Identifier(toBqType(pgArray.getInnerType()).name()), List.of()))));
                    }
                    throw new UnsupportedOperationException("Unsupported type: " + typeName);
            }
        }

        private static PGArray getPgArrayType(String arrayTypeName)
        {
            for (PGArray pgArray : allArray()) {
                if (arrayTypeName.equalsIgnoreCase(pgArray.typName())) {
                    return pgArray;
                }
            }
            throw new UnsupportedOperationException("Unsupported array type: " + arrayTypeName);
        }
    }
}
