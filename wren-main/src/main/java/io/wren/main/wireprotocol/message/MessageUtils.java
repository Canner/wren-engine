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

package io.wren.main.wireprotocol.message;

import io.netty.channel.Channel;
import io.wren.main.wireprotocol.PostgresSessionProperties;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Locale.ENGLISH;

public class MessageUtils
{
    // set (session) property { = | to } { value | 'value' }
    private static final Pattern SET_STMT_PATTERN = Pattern.compile("(?i)^ *SET( +SESSION)* +(?<property>[a-zA-Z0-9_]+)( *= *| +TO +)(?<value>(.*))");
    private static final Pattern SET_TRANSACTION_PATTERN = Pattern.compile("SET +(SESSION CHARACTERISTICS AS )? *TRANSACTION");
    private static final Pattern SET_SESSION_AUTHORIZATION = Pattern.compile("SET (SESSION |LOCAL )?SESSION AUTHORIZATION");

    private static final Set<String> IGNORED_COMMAND = Set.of(
            "BEGIN",
            "COMMIT",
            "DISCARD",
            "RESET",
            "CLOSE",
            "UNLISTEN");

    private MessageUtils() {}

    private static Optional<Pair<String, String>> parseSetStmt(String statement)
    {
        Matcher matcher = SET_STMT_PATTERN.matcher(statement.split(";")[0]);
        if (matcher.find()) {
            String property = matcher.group("property");
            String val = matcher.group("value");
            return Optional.of(Pair.of(property, PostgresSessionProperties.formatValue(val, PostgresSessionProperties.UNQUOTE_STRATEGY)));
        }
        return Optional.empty();
    }

    public static void sendHardWiredSessionProperty(Channel channel, String statement)
    {
        Optional<Pair<String, String>> property = parseSetStmt(statement);
        if (property.isPresent() && PostgresSessionProperties.isHardWiredSessionProperty(property.get().getKey())) {
            ResponseMessages.sendParameterStatus(channel, property.get().getKey(), property.get().getValue());
        }
    }

    public static boolean isIgnoredCommand(String statement)
    {
        Optional<String> command = Arrays.stream(statement.toUpperCase(ENGLISH).split(" |;"))
                .filter(split -> !split.isEmpty())
                .findFirst();

        if ((command.isPresent() && IGNORED_COMMAND.contains(command.get())) ||
                SET_TRANSACTION_PATTERN.matcher(statement).find() || SET_SESSION_AUTHORIZATION.matcher(statement).find()) {
            return true;
        }

        Matcher matcher = SET_STMT_PATTERN.matcher(statement);
        return matcher.find() && PostgresSessionProperties.isIgnoredSessionProperties(matcher.group("property"));
    }
}
