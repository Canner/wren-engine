package io.accio.base.sqlrewrite.analyzer.matcher;

import io.trino.sql.tree.Node;

public interface Matcher
{
    boolean shapeMatches(Node node);
}
