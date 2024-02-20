package io.accio.base.metadata;

import io.accio.base.type.PGType;

import java.util.Optional;

public interface Function
{
    String getName();

    String getRemoteName();

    Optional<PGType> getReturnType();
}
