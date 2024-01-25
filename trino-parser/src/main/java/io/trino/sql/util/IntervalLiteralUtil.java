/*
 * Copyright (C) Canner, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Canner dev team contact@canner.io, Feb 2022
 */

package io.trino.sql.util;

import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.NodeLocation;

import java.util.Locale;
import java.util.Optional;

import static io.trino.sql.tree.IntervalLiteral.Sign.NEGATIVE;
import static io.trino.sql.tree.IntervalLiteral.Sign.POSITIVE;

public final class IntervalLiteralUtil
{
    private IntervalLiteralUtil() {}

    /**
     * we use this method to parse PostgreSQL style interval string to IntervalLiteral.
     * <p>
     * e.g.
     * when client sends a query like
     * select CAST((CAST(now() AS timestamp) + (INTERVAL '-30 day')) AS date);
     * will get the same result as
     * select CAST((CAST(now() AS timestamp) + (INTERVAL - '30' day)) AS date);
     */
    public static IntervalLiteral parse(NodeLocation location, String text)
    {
        String[] strings = text.split(" ");
        String value = strings[0].replaceFirst("[-+]", "");
        IntervalLiteral.Sign sign = (strings[0].startsWith("-")) ? NEGATIVE : POSITIVE;
        if (strings[1].equalsIgnoreCase("week")) {
            return new IntervalLiteral(location, Long.toString(Long.parseLong(value) * 7), sign, IntervalLiteral.IntervalField.DAY, Optional.empty());
        }
        IntervalLiteral.IntervalField field = IntervalLiteral.IntervalField.valueOf(strings[1].toUpperCase(Locale.ROOT));

        return new IntervalLiteral(location, value, sign, field, Optional.empty());
    }
}
