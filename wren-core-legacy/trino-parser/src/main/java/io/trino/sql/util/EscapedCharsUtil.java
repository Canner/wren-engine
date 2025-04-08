/*
 * Copyright (C) Canner, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Canner dev team contact@canner.io, Nov 2021
 */

package io.trino.sql.util;

import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Handle string with C-Style escapes. For more information, refer to
 * https://www.postgresql.org/docs/13/sql-syntax-lexical.html#SQL-SYNTAX-CONSTANTS
 */
public final class EscapedCharsUtil
{
    private static final String ESCAPED_UNICODE_ERROR = "Invalid Unicode escape (must be \\uXXXX or \\UXXXXXXXX (X = 0–9, A–F))";

    private EscapedCharsUtil() {}

    public static String replaceEscapedChars(String input)
    {
        if (input.length() <= 1) {
            return input;
        }

        EscapedChars chars = EscapedChars.of(input);

        while (chars.hasNext()) {
            char current = chars.getCurrent();
            if (current == '\\' && chars.getIndex() + 1 < chars.getLength()) {
                Handlers.get(chars.getNext()).accept(chars);
            }
            else {
                chars.getBuilder().append(current);
            }
            chars.incrementIndex();
        }
        return chars.toString();
    }

    public static boolean isOctalDigit(char ch)
    {
        return ch >= '0' && ch <= '7';
    }

    public static boolean isHexDigit(char ch)
    {
        return (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'F') || (ch >= 'a' && ch <= 'f');
    }

    public static int calculateIndexInSequence(CharSequence seq,
            int beginIndex,
            int maxCharsToMatch,
            Predicate<Character> predicate)
    {
        int index = beginIndex;
        int end = Math.min(seq.length(), beginIndex + maxCharsToMatch);
        while (index < end && predicate.test(seq.charAt(index))) {
            index++;
        }
        return index;
    }

    public static void handleUnicode(EscapedChars chars)
    {
        // handle unicode case, e.g. \u1000, \U00001000
        int charsToConsume = (chars.getNext() == 'u') ? 4 : 8;
        if (chars.getIndex() + 1 + charsToConsume >= chars.getLength()) {
            throw new IllegalArgumentException(ESCAPED_UNICODE_ERROR);
        }
        int endIndex = calculateIndexInSequence(chars.getValue(),
                chars.getIndex() + 2,
                charsToConsume,
                EscapedCharsUtil::isHexDigit);
        if (endIndex != chars.getIndex() + 2 + charsToConsume) {
            throw new IllegalArgumentException(ESCAPED_UNICODE_ERROR);
        }
        // skip the backslash and the unicode prefix here, e.g. "\u1000" get string "1000"
        String substring = chars.getValue().substring(chars.getIndex() + 2, endIndex);
        chars.getBuilder().appendCodePoint(Integer.parseInt(substring, 16));
        chars.setIndex(endIndex - 1); // skip already consumed chars
    }

    public static void handleHex(EscapedChars chars)
    {
        StringBuilder builder = chars.getBuilder();

        // handle hex byte case - up to 2 chars for hex value
        int endIndex = calculateIndexInSequence(chars.getValue(),
                chars.getIndex() + 2,
                2,
                EscapedCharsUtil::isHexDigit);
        if (endIndex > chars.getIndex() + 2) {
            // skip the backslash and the hexadecimal prefix here, e.g. "\xAA" get string "AA"
            String substring = chars.getValue().substring(chars.getIndex() + 2, endIndex);
            builder.appendCodePoint(Integer.parseInt(substring, 16));
            chars.setIndex(endIndex - 1); // skip already consumed chars
        }
        else {
            // hex sequence unmatched - output original char
            builder.append(chars.getNext());
            chars.incrementIndex();
        }
    }

    public static void handleOctal(EscapedChars chars)
    {
        // handle octal case - up to 3 chars
        int endIndex = calculateIndexInSequence(chars.getValue(),
                chars.getIndex() + 2,
                2,      // first char is already "consumed"
                EscapedCharsUtil::isOctalDigit);
        // skip the backslash here, e.g. "\100" get string "100"
        String substring = chars.getValue().substring(chars.getIndex() + 1, endIndex);
        chars.getBuilder().appendCodePoint(Integer.parseInt(substring, 8));
        chars.setIndex(endIndex - 1); // skip already consumed chars
    }

    private enum Handlers
    {
        BACKSPACE(chars -> chars.appendAndIncrement('\b')),
        FORM_FEED(chars -> chars.appendAndIncrement('\f')),
        NEWLINE(chars -> chars.appendAndIncrement('\n')),
        CARRIAGE_RETURN(chars -> chars.appendAndIncrement('\r')),
        TAB(chars -> chars.appendAndIncrement('\t')),
        OCTAL_BYTE_VALUE(EscapedCharsUtil::handleOctal),
        HEXADECIMAL_BYTE_VALUE(EscapedCharsUtil::handleHex),
        HEXADECIMAL_UNICODE_CHARACTER_VALUE(EscapedCharsUtil::handleUnicode),
        BACK_SLASH(chars -> chars.appendAndIncrement(chars.getNext())),
        DEFAULT(chars -> chars.appendAndIncrement(chars.getNext()));

        private final Consumer<EscapedChars> handler;

        Handlers(Consumer<EscapedChars> handler)
        {
            this.handler = handler;
        }

        public static Consumer<EscapedChars> get(char ch)
        {
            switch (ch) {
                case 'b':
                    return BACKSPACE.handler;
                case 'f':
                    return FORM_FEED.handler;
                case 'n':
                    return NEWLINE.handler;
                case 'r':
                    return CARRIAGE_RETURN.handler;
                case 't':
                    return TAB.handler;
                case '\\':
                    return BACK_SLASH.handler;
                case 'u':
                case 'U':
                    return HEXADECIMAL_UNICODE_CHARACTER_VALUE.handler;
                case 'x':
                    return HEXADECIMAL_BYTE_VALUE.handler;
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                    return OCTAL_BYTE_VALUE.handler;
            }
            // non-valid escaped char sequence
            return DEFAULT.handler;
        }
    }
}
