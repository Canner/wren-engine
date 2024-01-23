package io.accio.base.sqlrewrite.analyzer.matcher;

import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.Node;

public class PredicateMatcher
        implements Matcher
{
    public static final PredicateMatcher PREDICATE_MATCHER = new PredicateMatcher();

    private PredicateMatcher() {}

    @Override
    public boolean shapeMatches(Node node)
    {
        if (!(node instanceof ComparisonExpression)) {
            return false;
        }

        Expression left = ((ComparisonExpression) node).getLeft();
        Expression right = ((ComparisonExpression) node).getRight();

        return (left instanceof DereferenceExpression || left instanceof Identifier) && right instanceof Literal;
    }
}
