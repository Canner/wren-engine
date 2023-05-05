/*
 * Copyright (C) Canner, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Canner dev team <contact@cannerdata.com>, Jan 2023
 */
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ImpersonateUser
        extends Statement
{
    private final Expression expression;

    public ImpersonateUser(NodeLocation location, Expression expression)
    {
        this(Optional.of(location), expression);
    }

    public ImpersonateUser(Expression expression)
    {
        this(Optional.empty(), expression);
    }

    protected ImpersonateUser(Optional<NodeLocation> location, Expression expression)
    {
        super(location);
        this.expression = requireNonNull(expression, "expression is null");
    }

    public Expression getExpression()
    {
        return expression;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitImpersonateUser(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression);
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
        ImpersonateUser o = (ImpersonateUser) obj;
        return Objects.equals(expression, o.expression);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("expression", expression)
                .toString();
    }
}
