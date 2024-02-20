package io.accio.base.metadata;

import com.google.common.collect.ImmutableList;

import static io.accio.base.metadata.Function.Argument.argument;
import static io.accio.base.metadata.Function.builder;
import static io.accio.base.type.TimestampType.TIMESTAMP;
import static io.accio.base.type.VarcharType.VARCHAR;

public class BasicFunctions
{
    private BasicFunctions() {}

    public static final Function DATE_TRUNC = builder()
            .setName("date_trunc")
            .setArguments(ImmutableList.of(argument("field", VARCHAR), argument("source", TIMESTAMP)))
            .setReturnType(TIMESTAMP)
            .build();
}
