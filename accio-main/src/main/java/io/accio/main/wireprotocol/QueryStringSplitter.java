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

package io.accio.main.wireprotocol;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits a query string by semicolon into multiple statements.
 */
class QueryStringSplitter
{
    enum CommentType
    {
        NO,
        LINE,
        MULTI_LINE
    }

    enum QuoteType
    {
        NONE,
        SINGLE,
        DOUBLE
    }

    private QueryStringSplitter() {}

    public static List<String> splitQuery(String query)
    {
        final List<String> queries = new ArrayList<>(2);

        CommentType commentType = CommentType.NO;
        QuoteType quoteType = QuoteType.NONE;
        boolean restoreSingleQuoteIfEscaped = false;

        char[] chars = query.toCharArray();

        boolean isCurrentlyEmpty = true;
        int offset = 0;
        char lastChar = ' ';
        for (int i = 0; i < chars.length; i++) {
            char aChar = chars[i];
            if (isCurrentlyEmpty && !Character.isWhitespace(aChar)) {
                isCurrentlyEmpty = false;
            }
            switch (aChar) {
                case '\'':
                    if (commentType == CommentType.NO && quoteType != QuoteType.DOUBLE) {
                        if (lastChar == '\'') {
                            // Escaping of ' via '', e.g.: 'hello ''Joe''!'
                            // When we set single quotes state to NONE,
                            // but we find out this was due to an escaped single quote (''),
                            // we restore the previous single quote state here.
                            quoteType = restoreSingleQuoteIfEscaped ? QuoteType.SINGLE : QuoteType.NONE;
                            restoreSingleQuoteIfEscaped = false;
                        }
                        else {
                            restoreSingleQuoteIfEscaped = quoteType == QuoteType.SINGLE;
                            quoteType = quoteType == QuoteType.SINGLE ? QuoteType.NONE : QuoteType.SINGLE;
                        }
                    }
                    break;
                case '"':
                    if (commentType == CommentType.NO && quoteType != QuoteType.SINGLE) {
                        quoteType = quoteType == QuoteType.DOUBLE ? QuoteType.NONE : QuoteType.DOUBLE;
                    }
                    break;
                case '-':
                    if (commentType == CommentType.NO && quoteType == QuoteType.NONE && lastChar == '-') {
                        commentType = CommentType.LINE;
                    }
                    break;
                case '*':
                    if (commentType == CommentType.NO && quoteType == QuoteType.NONE && lastChar == '/') {
                        commentType = CommentType.MULTI_LINE;
                    }
                    break;
                case '/':
                    if (commentType == CommentType.MULTI_LINE && lastChar == '*') {
                        commentType = CommentType.NO;
                        offset = i + 1;
                    }
                    break;
                case '\n':
                    if (commentType == CommentType.LINE) {
                        commentType = CommentType.NO;
                        offset = i + 1;
                    }
                    break;
                case ';':
                    if (commentType == CommentType.NO && quoteType == QuoteType.NONE) {
                        queries.add(new String(chars, offset, i - offset + 1));
                        offset = i + 1;
                        isCurrentlyEmpty = true;
                    }
                    break;

                default:
            }
            lastChar = aChar;
        }
        // statement might not be terminated by semicolon
        if (!isCurrentlyEmpty && offset < chars.length && commentType == CommentType.NO) {
            queries.add(new String(chars, offset, chars.length - offset));
        }
        if (queries.isEmpty()) {
            queries.add("");
        }

        return queries;
    }
}
