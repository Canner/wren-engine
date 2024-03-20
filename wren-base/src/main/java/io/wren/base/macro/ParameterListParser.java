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

package io.wren.base.macro;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class ParameterListParser
{
    private static final BaseErrorListener LEXER_ERROR_LISTENER = new BaseErrorListener()
    {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String message, RecognitionException e)
        {
            throw new ParsingException(message, e, line, charPositionInLine + 1);
        }
    };
    private static final BiConsumer<ParameterListBaseLexer, ParameterListBaseParser> DEFAULT_PARSER_INITIALIZER = (ParameterListBaseLexer lexer, ParameterListBaseParser parser) -> {};

    private final BiConsumer<ParameterListBaseLexer, ParameterListBaseParser> initializer;

    public ParameterListParser()
    {
        this(DEFAULT_PARSER_INITIALIZER);
    }

    public ParameterListParser(BiConsumer<ParameterListBaseLexer, ParameterListBaseParser> initializer)
    {
        this.initializer = requireNonNull(initializer, "initializer is null");
    }

    public List<Parameter> parse(String listString)
    {
        return parse(listString, ParameterListBaseParser::parameterList);
    }

    private List<Parameter> parse(String listString, Function<ParameterListBaseParser, ParserRuleContext> parseFunction)
    {
        try {
            ParameterListBaseLexer lexer = new ParameterListBaseLexer(CharStreams.fromString(listString));
            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            ParameterListBaseParser parser = new ParameterListBaseParser(tokenStream);
            initializer.accept(lexer, parser);

            // Override the default error strategy to not attempt inserting or deleting a token.
            // Otherwise, it messes up error reporting
            parser.setErrorHandler(new DefaultErrorStrategy()
            {
                @Override
                public Token recoverInline(Parser recognizer)
                        throws RecognitionException
                {
                    if (nextTokensContext == null) {
                        throw new InputMismatchException(recognizer);
                    }
                    else {
                        throw new InputMismatchException(recognizer, nextTokensState, nextTokensContext);
                    }
                }
            });

            parser.addParseListener(new PostProcessor());

            lexer.removeErrorListeners();
            lexer.addErrorListener(LEXER_ERROR_LISTENER);

            ParserRuleContext tree;
            try {
                // first, try parsing with potentially faster SLL mode
                parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
                tree = parseFunction.apply(parser);
            }
            catch (ParseCancellationException ex) {
                // if we fail, parse with LL mode
                tokenStream.seek(0); // rewind input stream
                parser.reset();

                parser.getInterpreter().setPredictionMode(PredictionMode.LL);
                tree = parseFunction.apply(parser);
            }

            return new ParameterCollector().collect(tree);
        }
        catch (StackOverflowError e) {
            throw new RuntimeException("ParameterList is too large (stack overflow while parsing)");
        }
    }

    private static class PostProcessor
            extends ParameterListBaseBaseListener
    {
        @Override
        public void exitParameter(ParameterListBaseParser.ParameterContext ctx)
        {
            if (ctx.paraName() == null) {
                throw new ParsingException(format("parse parameter failed: %s", ctx.getText()), ctx.getStart().getLine(), ctx.getStart().getCharPositionInLine());
            }

            if (ctx.typeName() == null) {
                throw new ParsingException(format("typeName is null: %s", ctx.getText()),
                        ctx.paraName().getStart().getLine(), ctx.paraName().getStart().getCharPositionInLine() + 1);
            }

            try {
                Parameter.TYPE.valueOf(ctx.typeName().getText().toUpperCase(ENGLISH));
            }
            catch (IllegalArgumentException e) {
                throw new ParsingException(format("typeName is invalid: %s", ctx.getText()),
                        ctx.typeName().getStart().getLine(), ctx.typeName().getStart().getCharPositionInLine() + 1);
            }
        }
    }
}
