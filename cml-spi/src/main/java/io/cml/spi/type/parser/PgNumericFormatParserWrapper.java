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

package io.cml.spi.type.parser;

import io.cml.spi.CmlException;
import io.cml.spi.type.parser.antlr.v4.PgNumericFormatBaseVisitor;
import io.cml.spi.type.parser.antlr.v4.PgNumericFormatLexer;
import io.cml.spi.type.parser.antlr.v4.PgNumericFormatParser;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.tree.ParseTree;

import static io.cml.spi.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public final class PgNumericFormatParserWrapper
{
    private PgNumericFormatParserWrapper() {}

    private static final BaseErrorListener ERROR_LISTENER = new BaseErrorListener()
    {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer,
                Object offendingSymbol,
                int line,
                int charPositionInLine,
                String message,
                RecognitionException e)
        {
            throw new CmlException(GENERIC_INTERNAL_ERROR, e);
        }
    };

    public static String parse(String origin)
    {
        Lexer lexer = new PgNumericFormatLexer(CharStreams.fromString(origin));
        lexer.removeErrorListeners();
        lexer.addErrorListener(ERROR_LISTENER);

        PgNumericFormatParser parser = new PgNumericFormatParser(new CommonTokenStream(lexer));
        parser.removeErrorListeners();
        parser.addErrorListener(ERROR_LISTENER);

        ParseTree tree = parser.format();
        return new AstBuilder().visit(tree);
    }

    private static class AstBuilder
            extends PgNumericFormatBaseVisitor<String>
    {
        @Override
        public String visitFormat(PgNumericFormatParser.FormatContext ctx)
        {
            StringBuilder builder = new StringBuilder();
            ctx.pattern().forEach(pattern -> builder.append(visit(pattern)));
            return builder.toString();
        }

        @Override
        public String visitDigitPattern(PgNumericFormatParser.DigitPatternContext ctx)
        {
            if (ctx.getText().equals("9")) {
                return "#";
            }
            return "0";
        }

        @Override
        public String visitDecimalPointPattern(PgNumericFormatParser.DecimalPointPatternContext ctx)
        {
            return ".";
        }

        @Override
        public String visitGroupSeparatorPattern(PgNumericFormatParser.GroupSeparatorPatternContext ctx)
        {
            return ",";
        }

        @Override
        public String visitCurrencySymbolPattern(PgNumericFormatParser.CurrencySymbolPatternContext ctx)
        {
            return "\u00A4";
        }

        @Override
        public String visitExponentPattern(PgNumericFormatParser.ExponentPatternContext ctx)
        {
            return "E00";
        }

        @Override
        public String visitNonReservedPattern(PgNumericFormatParser.NonReservedPatternContext ctx)
        {
            throw new CmlException(GENERIC_INTERNAL_ERROR, String.format("we didn't support the pattern %s.", ctx.getText()));
        }
    }
}
