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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class FunctionCall
        extends Expression
{
    private final QualifiedName name;
    private final Optional<Window> window;
    private final Optional<Expression> filter;
    private final Optional<OrderBy> orderBy;
    private final boolean distinct;
    private final Optional<NullTreatment> nullTreatment;
    private final Optional<ProcessingMode> processingMode;
    private final List<Expression> arguments;

    public FunctionCall(QualifiedName name, List<Expression> arguments)
    {
        this(Optional.empty(), name, Optional.empty(), Optional.empty(), Optional.empty(), false, Optional.empty(), Optional.empty(), arguments);
    }

    public FunctionCall(NodeLocation location, QualifiedName name, List<Expression> arguments)
    {
        this(Optional.of(location), name, Optional.empty(), Optional.empty(), Optional.empty(), false, Optional.empty(), Optional.empty(), arguments);
    }

    public FunctionCall(
            Optional<NodeLocation> location,
            QualifiedName name,
            Optional<Window> window,
            Optional<Expression> filter,
            Optional<OrderBy> orderBy,
            boolean distinct,
            Optional<NullTreatment> nullTreatment,
            Optional<ProcessingMode> processingMode,
            List<Expression> arguments)
    {
        super(location);
        requireNonNull(name, "name is null");
        requireNonNull(window, "window is null");
        window.ifPresent(node -> checkArgument(node instanceof WindowReference || node instanceof WindowSpecification, "unexpected window: " + node.getClass().getSimpleName()));
        requireNonNull(filter, "filter is null");
        requireNonNull(orderBy, "orderBy is null");
        requireNonNull(nullTreatment, "nullTreatment is null");
        requireNonNull(processingMode, "processingMode is null");
        requireNonNull(arguments, "arguments is null");

        this.name = name;
        this.window = window;
        this.filter = filter;
        this.orderBy = orderBy;
        this.distinct = distinct;
        this.nullTreatment = nullTreatment;
        this.processingMode = processingMode;
        this.arguments = arguments;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public Optional<Window> getWindow()
    {
        return window;
    }

    public Optional<OrderBy> getOrderBy()
    {
        return orderBy;
    }

    public boolean isDistinct()
    {
        return distinct;
    }

    public Optional<NullTreatment> getNullTreatment()
    {
        return nullTreatment;
    }

    public Optional<ProcessingMode> getProcessingMode()
    {
        return processingMode;
    }

    public List<Expression> getArguments()
    {
        return arguments;
    }

    public Optional<Expression> getFilter()
    {
        return filter;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitFunctionCall(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        window.ifPresent(window -> nodes.add((Node) window));
        filter.ifPresent(nodes::add);
        orderBy.map(OrderBy::getSortItems).ifPresent(nodes::addAll);
        nodes.addAll(arguments);
        return nodes.build();
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
        FunctionCall o = (FunctionCall) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(window, o.window) &&
                Objects.equals(filter, o.filter) &&
                Objects.equals(orderBy, o.orderBy) &&
                Objects.equals(distinct, o.distinct) &&
                Objects.equals(nullTreatment, o.nullTreatment) &&
                Objects.equals(processingMode, o.processingMode) &&
                Objects.equals(arguments, o.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, distinct, nullTreatment, processingMode, window, filter, orderBy, arguments);
    }

    // TODO: make this a proper Tree node so that we can report error
    // locations more accurately
    public enum NullTreatment
    {
        IGNORE, RESPECT
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        FunctionCall otherFunction = (FunctionCall) other;

        return name.equals(otherFunction.name) &&
                distinct == otherFunction.distinct &&
                nullTreatment.equals(otherFunction.nullTreatment) &&
                processingMode.equals(otherFunction.processingMode);
    }

    public static Builder builder(FunctionCall functionCall)
    {
        return new Builder()
                .location(functionCall.getLocation())
                .name(functionCall.getName())
                .window(functionCall.getWindow())
                .filter(functionCall.getFilter())
                .orderBy(functionCall.getOrderBy())
                .distinct(functionCall.isDistinct())
                .nullTreatment(functionCall.getNullTreatment())
                .processingMode(functionCall.getProcessingMode())
                .arguments(functionCall.getArguments());
    }

    public static class Builder
    {
        private Optional<NodeLocation> location = Optional.empty();
        private QualifiedName name;
        private Optional<Window> window = Optional.empty();
        private Optional<Expression> filter = Optional.empty();
        private Optional<OrderBy> orderBy = Optional.empty();
        private boolean distinct;
        private Optional<NullTreatment> nullTreatment = Optional.empty();
        private Optional<ProcessingMode> processingMode = Optional.empty();
        private List<Expression> arguments = ImmutableList.of();

        private Builder() {}

        public Builder location(Optional<NodeLocation> location)
        {
            this.location = location;
            return this;
        }

        public Builder name(QualifiedName name)
        {
            this.name = name;
            return this;
        }

        public Builder window(Optional<Window> window)
        {
            this.window = window;
            return this;
        }

        public Builder filter(Optional<Expression> filter)
        {
            this.filter = filter;
            return this;
        }

        public Builder orderBy(Optional<OrderBy> orderBy)
        {
            this.orderBy = orderBy;
            return this;
        }

        public Builder distinct(boolean distinct)
        {
            this.distinct = distinct;
            return this;
        }

        public Builder nullTreatment(Optional<NullTreatment> nullTreatment)
        {
            this.nullTreatment = nullTreatment;
            return this;
        }

        public Builder processingMode(Optional<ProcessingMode> processingMode)
        {
            this.processingMode = processingMode;
            return this;
        }

        public Builder arguments(List<Expression> arguments)
        {
            this.arguments = arguments;
            return this;
        }

        public FunctionCall build()
        {
            return new FunctionCall(location, name, window, filter, orderBy, distinct, nullTreatment, processingMode, arguments);
        }
    }
}
