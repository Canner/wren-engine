/*
 * Copyright (C) Canner, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Canner dev team <contact@cannerdata.com>, Sep 2022
 */
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class FetchCursor
        extends Statement
{
    private final Integer rowCount;
    private final Identifier cursor;

    public FetchCursor(NodeLocation location, Integer rowCount, Identifier cursor)
    {
        this(Optional.of(location), rowCount, cursor);
    }

    public FetchCursor(Integer rowCount, Identifier cursor)
    {
        this(Optional.empty(), rowCount, cursor);
    }

    protected FetchCursor(Optional<NodeLocation> location, Integer rowCount, Identifier cursor)
    {
        super(location);
        this.rowCount = requireNonNull(rowCount, "rowCount is null");
        this.cursor = requireNonNull(cursor, "cursor is null");
    }

    public Integer getRowCount()
    {
        return rowCount;
    }

    public Identifier getCursor()
    {
        return cursor;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitFetchCursor(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rowCount, cursor);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        FetchCursor o = (FetchCursor) obj;
        return Objects.equals(rowCount, o.rowCount)
                && Objects.equals(cursor, o.cursor);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("rowCount", rowCount)
                .add("cursor", cursor)
                .toString();
    }
}
