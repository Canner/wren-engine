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
package io.trino.type;

import io.trino.sql.parser.CaseInsensitiveStream;
import io.trino.sql.parser.ParsingException;
import io.trino.type.TypeCalculationParser.ArithmeticBinaryContext;
import io.trino.type.TypeCalculationParser.ArithmeticUnaryContext;
import io.trino.type.TypeCalculationParser.BinaryFunctionContext;
import io.trino.type.TypeCalculationParser.IdentifierContext;
import io.trino.type.TypeCalculationParser.NullLiteralContext;
import io.trino.type.TypeCalculationParser.NumericLiteralContext;
import io.trino.type.TypeCalculationParser.ParenthesizedExpressionContext;
import io.trino.type.TypeCalculationParser.TypeCalculationContext;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.math.BigInteger;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.type.TypeCalculationParser.ASTERISK;
import static io.trino.type.TypeCalculationParser.MAX;
import static io.trino.type.TypeCalculationParser.MIN;
import static io.trino.type.TypeCalculationParser.MINUS;
import static io.trino.type.TypeCalculationParser.PLUS;
import static io.trino.type.TypeCalculationParser.SLASH;
import static java.util.Objects.requireNonNull;

public final class TypeCalculation
{
    private static final BaseErrorListener ERROR_LISTENER = new BaseErrorListener()
    {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String message, RecognitionException e)
        {
            throw new ParsingException(message, e, line, charPositionInLine + 1);
        }
    };

    private TypeCalculation() {}

    public static Long calculateLiteralValue(
            String calculation,
            Map<String, Long> inputs)
    {
        try {
            ParserRuleContext tree = parseTypeCalculation(calculation);
            CalculateTypeVisitor visitor = new CalculateTypeVisitor(inputs);
            BigInteger result = visitor.visit(tree);
            return result.longValueExact();
        }
        catch (StackOverflowError e) {
            throw new ParsingException("Type calculation is too large (stack overflow while parsing)");
        }
    }

    private static ParserRuleContext parseTypeCalculation(String calculation)
    {
        TypeCalculationLexer lexer = new TypeCalculationLexer(new CaseInsensitiveStream(CharStreams.fromString(calculation)));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        TypeCalculationParser parser = new TypeCalculationParser(tokenStream);

        lexer.removeErrorListeners();
        lexer.addErrorListener(ERROR_LISTENER);

        parser.removeErrorListeners();
        parser.addErrorListener(ERROR_LISTENER);

        ParserRuleContext tree;
        try {
            // first, try parsing with potentially faster SLL mode
            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            tree = parser.typeCalculation();
        }
        catch (ParseCancellationException ex) {
            // if we fail, parse with LL mode
            tokenStream.seek(0); // rewind input stream
            parser.reset();

            parser.getInterpreter().setPredictionMode(PredictionMode.LL);
            tree = parser.typeCalculation();
        }
        return tree;
    }

    private static class IsSimpleExpressionVisitor
            extends TypeCalculationBaseVisitor<Boolean>
    {
        @Override
        public Boolean visitArithmeticBinary(ArithmeticBinaryContext ctx)
        {
            return false;
        }

        @Override
        public Boolean visitArithmeticUnary(ArithmeticUnaryContext ctx)
        {
            return false;
        }

        @Override
        protected Boolean defaultResult()
        {
            return true;
        }

        @Override
        protected Boolean aggregateResult(Boolean aggregate, Boolean nextResult)
        {
            return aggregate && nextResult;
        }
    }

    private static class CalculateTypeVisitor
            extends TypeCalculationBaseVisitor<BigInteger>
    {
        private final Map<String, Long> inputs;

        public CalculateTypeVisitor(Map<String, Long> inputs)
        {
            this.inputs = requireNonNull(inputs);
        }

        @Override
        public BigInteger visitTypeCalculation(TypeCalculationContext ctx)
        {
            return visit(ctx.expression());
        }

        @Override
        public BigInteger visitArithmeticBinary(ArithmeticBinaryContext ctx)
        {
            BigInteger left = visit(ctx.left);
            BigInteger right = visit(ctx.right);
            switch (ctx.operator.getType()) {
                case PLUS:
                    return left.add(right);
                case MINUS:
                    return left.subtract(right);
                case ASTERISK:
                    return left.multiply(right);
                case SLASH:
                    return left.divide(right);
                default:
                    throw new IllegalStateException("Unsupported binary operator " + ctx.operator.getText());
            }
        }

        @Override
        public BigInteger visitArithmeticUnary(ArithmeticUnaryContext ctx)
        {
            BigInteger value = visit(ctx.expression());
            switch (ctx.operator.getType()) {
                case PLUS:
                    return value;
                case MINUS:
                    return value.negate();
                default:
                    throw new IllegalStateException("Unsupported unary operator " + ctx.operator.getText());
            }
        }

        @Override
        public BigInteger visitBinaryFunction(BinaryFunctionContext ctx)
        {
            BigInteger left = visit(ctx.left);
            BigInteger right = visit(ctx.right);
            switch (ctx.binaryFunctionName().name.getType()) {
                case MIN:
                    return left.min(right);
                case MAX:
                    return left.max(right);
                default:
                    throw new IllegalArgumentException("Unsupported binary function " + ctx.binaryFunctionName().getText());
            }
        }

        @Override
        public BigInteger visitNumericLiteral(NumericLiteralContext ctx)
        {
            return new BigInteger(ctx.INTEGER_VALUE().getText());
        }

        @Override
        public BigInteger visitNullLiteral(NullLiteralContext ctx)
        {
            return BigInteger.ZERO;
        }

        @Override
        public BigInteger visitIdentifier(IdentifierContext ctx)
        {
            String identifier = ctx.getText();
            Long value = inputs.get(identifier);
            checkState(value != null, "value for variable '%s' is not specified in the inputs", identifier);
            return BigInteger.valueOf(value);
        }

        @Override
        public BigInteger visitParenthesizedExpression(ParenthesizedExpressionContext ctx)
        {
            return visit(ctx.expression());
        }
    }
}
