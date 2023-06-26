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
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public class Extract
        extends Expression
{
    private final Expression expression;
    private final Field field;

    public static class Field
    {
        private final String name;

        public Field(String name)
        {
            this.name = name;
        }

        public String getName()
        {
            return name;
        }

        @Override
        public String toString()
        {
            return name;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Field that = (Field) o;
            return name.equals(that.name);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name);
        }
    }

    public Extract(Expression expression, Field field)
    {
        this(Optional.empty(), expression, field);
    }

    public Extract(NodeLocation location, Expression expression, Field field)
    {
        this(Optional.of(location), expression, field);
    }

    private Extract(Optional<NodeLocation> location, Expression expression, Field field)
    {
        super(location);
        requireNonNull(expression, "expression is null");
        requireNonNull(field, "field is null");

        this.expression = expression;
        this.field = field;
    }

    public Expression getExpression()
    {
        return expression;
    }

    public Field getField()
    {
        return field;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitExtract(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(expression);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Extract that = (Extract) o;
        return Objects.equals(expression, that.expression) &&
                Objects.equals(field, that.field);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, field);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        Extract otherExtract = (Extract) other;
        return field.equals(otherExtract.field);
    }
}
