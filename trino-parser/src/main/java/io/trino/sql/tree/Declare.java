/*
 * Copyright (C) Canner, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Canner dev team <contact@cannerdata.com>, Sep 2022
 */
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;
import io.trino.sql.SqlFormatter;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Declare
        extends Statement
{
    private final Identifier name;
    private final Query body;

    public Declare(NodeLocation location, Identifier name, Query body)
    {
        this(Optional.of(location), name, body);
    }

    public Declare(Identifier name, Query body)
    {
        this(Optional.empty(), name, body);
    }

    protected Declare(Optional<NodeLocation> location, Identifier name, Query body)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.body = requireNonNull(body, "body is null");
    }

    public Identifier getName()
    {
        return name;
    }

    public Query getBody()
    {
        return body;
    }

    public String toStatement()
    {
        return SqlFormatter.formatSql(body);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDeclareCursor(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, body);
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
        Declare o = (Declare) obj;
        return Objects.equals(name, o.name)
                && Objects.equals(body, o.body);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("body", body)
                .toString();
    }
}
