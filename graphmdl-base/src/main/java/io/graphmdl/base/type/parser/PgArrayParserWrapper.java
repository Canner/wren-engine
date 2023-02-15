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
import io.graphmdl.base.type.parser.antlr.v4.PgArrayLexer;
import io.graphmdl.base.type.parser.antlr.v4.PgArrayParser;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import static io.graphmdl.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class PgArrayParserWrapper
{
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

    private static final PgArrayParserWrapper INSTANCE = new PgArrayParserWrapper();

    public static Object parse(byte[] bytes, Function<byte[], Object> convert)
    {
        return INSTANCE.invokeParser(
                new ByteArrayInputStream(bytes),
                PgArrayParser::array,
                convert);
    }

    private Object invokeParser(InputStream inputStream,
            Function<PgArrayParser, ParserRuleContext> parseFunction,
            Function<byte[], Object> convert)
    {
        try {
            PgArrayLexer lexer = new PgArrayLexer(CharStreams.fromStream(
                    inputStream,
                    StandardCharsets.UTF_8));
            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            PgArrayParser parser = new PgArrayParser(tokenStream);

            lexer.removeErrorListeners();
            lexer.addErrorListener(ERROR_LISTENER);

            parser.removeErrorListeners();
            parser.addErrorListener(ERROR_LISTENER);

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
            return tree.accept(new PgArrayASTVisitor(convert));
        }
        catch (StackOverflowError e) {
            throw new GraphMDLException(GENERIC_INTERNAL_ERROR, "stack overflow while parsing: " + e.getLocalizedMessage());
        }
        catch (IOException e) {
            return new IllegalArgumentException(e);
        }
    }
}
