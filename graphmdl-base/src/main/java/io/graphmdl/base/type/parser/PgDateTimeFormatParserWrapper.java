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

package io.graphmdl.base.type.parser;

import io.graphmdl.base.GraphMDLException;
import io.graphmdl.base.type.parser.antlr.v4.PgDateTimeFormatBaseVisitor;
import io.graphmdl.base.type.parser.antlr.v4.PgDateTimeFormatLexer;
import io.graphmdl.base.type.parser.antlr.v4.PgDateTimeFormatParser;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.Locale;

import static io.graphmdl.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;

/**
 * this parser adapts patterns from PostgreSQL to Joda.
 * <p>
 * for more info about patterns of PostgreSQL, refer to https://www.postgresql.org/docs/13/functions-formatting.html
 * for more info about patterns of Joda, refer to https://help.gooddata.com/cloudconnect/manual/date-and-time-format.html
 */
public final class PgDateTimeFormatParserWrapper
{
    private PgDateTimeFormatParserWrapper() {}

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
            throw new GraphMDLException(GENERIC_INTERNAL_ERROR, e);
        }
    };

    public static String parse(String origin)
    {
        Lexer lexer = new PgDateTimeFormatLexer(CharStreams.fromString(origin));
        lexer.removeErrorListeners();
        lexer.addErrorListener(ERROR_LISTENER);

        PgDateTimeFormatParser parser = new PgDateTimeFormatParser(new CommonTokenStream(lexer));
        parser.removeErrorListeners();
        parser.addErrorListener(ERROR_LISTENER);

        ParseTree tree = parser.format();
        return new AstBuilder().visit(tree);
    }

    private static class AstBuilder
            extends PgDateTimeFormatBaseVisitor<String>
    {
        @Override
        public String visitFormat(PgDateTimeFormatParser.FormatContext ctx)
        {
            StringBuilder builder = new StringBuilder();
            ctx.symbol().forEach(symbol -> builder.append(visit(symbol)));
            return builder.toString();
        }

        @Override
        public String visitSeparator(PgDateTimeFormatParser.SeparatorContext ctx)
        {
            return ctx.getText();
        }

        @Override
        public String visitHourLiteral(PgDateTimeFormatParser.HourLiteralContext ctx)
        {
            if (ctx.getText().endsWith("24")) {
                return "HH";
            }
            return "hh";
        }

        @Override
        public String visitMinuteLiteral(PgDateTimeFormatParser.MinuteLiteralContext ctx)
        {
            return "mm";
        }

        @Override
        public String visitSecondLiteral(PgDateTimeFormatParser.SecondLiteralContext ctx)
        {
            return "ss";
        }

        @Override
        public String visitMilliSecondLiteral(PgDateTimeFormatParser.MilliSecondLiteralContext ctx)
        {
            return "SSS";
        }

        @Override
        public String visitMeridiemMarkerLiteral(PgDateTimeFormatParser.MeridiemMarkerLiteralContext ctx)
        {
            return "a";
        }

        @Override
        public String visitEraDesignatorLiteral(PgDateTimeFormatParser.EraDesignatorLiteralContext ctx)
        {
            return "G";
        }

        @Override
        public String visitTimeZoneLiteral(PgDateTimeFormatParser.TimeZoneLiteralContext ctx)
        {
            return "z";
        }

        @Override
        public String visitYearLiteral(PgDateTimeFormatParser.YearLiteralContext ctx)
        {
            return ctx.getText().toUpperCase(Locale.ROOT);
        }

        @Override
        public String visitMonthLiteral(PgDateTimeFormatParser.MonthLiteralContext ctx)
        {
            String month = ctx.getText();
            if (month.equals("Month")) {
                return "MMMM";
            }
            if (month.equals("Mon")) {
                return "MMM";
            }
            return month.toUpperCase(Locale.ROOT);
        }

        @Override
        public String visitWeekLiteral(PgDateTimeFormatParser.WeekLiteralContext ctx)
        {
            return ctx.getText().toLowerCase(Locale.ROOT);
        }

        @Override
        public String visitDayLiteral(PgDateTimeFormatParser.DayLiteralContext ctx)
        {
            String day = ctx.getText();
            if (day.equals("Day")) {
                return "EEEE";
            }
            if (day.equals("Dy")) {
                return "EEE";
            }
            if (day.length() == 2) {
                return "dd";
            }
            if (day.length() == 1) {
                return "e";
            }
            return day.toUpperCase(Locale.ROOT);
        }
    }
}
