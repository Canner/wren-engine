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

package io.cml.sql.analyzer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Streams;
import io.cml.metadata.ColumnHandle;
import io.cml.metadata.TableSchema;
import io.cml.spi.type.PGType;
import io.cml.sql.QualifiedObjectName;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GroupingOperation;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Statement;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class Analysis
{
    private Statement root;
    private final Map<NodeRef<Node>, Expression> where = new LinkedHashMap<>();

    private final Map<NodeRef<Expression>, PGType<?>> types = new LinkedHashMap<>();

    private final Map<NodeRef<QuerySpecification>, GroupingSetAnalysis> groupingSets = new LinkedHashMap<>();

    private final Map<NodeRef<Node>, List<SelectExpression>> selectExpressions = new LinkedHashMap<>();

    private final Map<NodeRef<Node>, Scope> scopes = new LinkedHashMap<>();

    private final Multimap<Field, SourceColumn> originColumnDetails = ArrayListMultimap.create();

    private final Multimap<NodeRef<Expression>, Field> fieldLineage = ArrayListMultimap.create();

    private final Map<NodeRef<Node>, List<Expression>> orderByExpressions = new LinkedHashMap<>();

    private final Map<NodeRef<QuerySpecification>, Scope> implicitFromScopes = new LinkedHashMap<>();

    private final Map<Field, ColumnHandle> columns = new LinkedHashMap<>();

    private final Map<NodeRef<QuerySpecification>, List<FunctionCall>> aggregates = new LinkedHashMap<>();

    private final Map<NodeRef<OrderBy>, List<Expression>> orderByAggregates = new LinkedHashMap<>();

    private final Map<NodeRef<QuerySpecification>, List<GroupingOperation>> groupingOperations = new LinkedHashMap<>();

    private final List<TableSchema> visitedTables = new ArrayList<>();

    public Analysis(Statement root)
    {
        this.root = root;
    }

    public void setRoot(Statement root)
    {
        this.root = root;
    }

    public Statement getRoot()
    {
        return root;
    }

    public Map<NodeRef<Node>, Expression> getWhere()
    {
        return where;
    }

    public void setWhere(Node node, Expression expression)
    {
        where.put(NodeRef.of(node), expression);
    }

    public Expression getWhere(QuerySpecification node)
    {
        return where.get(NodeRef.<Node>of(node));
    }

    public void setSelectExpressions(Node node, List<SelectExpression> expressions)
    {
        selectExpressions.put(NodeRef.of(node), ImmutableList.copyOf(expressions));
    }

    public List<SelectExpression> getSelectExpressions(Node node)
    {
        return selectExpressions.get(NodeRef.of(node));
    }

    public void addTypes(Map<NodeRef<Expression>, PGType<?>> types)
    {
        this.types.putAll(types);
    }

    public Map<NodeRef<Expression>, PGType<?>> getTypes()
    {
        return unmodifiableMap(types);
    }

    public PGType<?> getType(Expression expression)
    {
        PGType<?> type = types.get(NodeRef.of(expression));
        checkArgument(type != null, "Expression not analyzed: %s", expression);
        return type;
    }

    public void setColumn(Field field, ColumnHandle handle)
    {
        columns.put(field, handle);
    }

    public void setGroupingSets(QuerySpecification node, GroupingSetAnalysis groupingSets)
    {
        this.groupingSets.put(NodeRef.of(node), groupingSets);
    }

    public GroupingSetAnalysis getGroupingSet(QuerySpecification node)
    {
        return this.groupingSets.get(NodeRef.of(node));
    }

    public Optional<Scope> tryGetScope(Node node)
    {
        NodeRef<Node> key = NodeRef.of(node);
        if (scopes.containsKey(key)) {
            return Optional.of(scopes.get(key));
        }

        return Optional.empty();
    }

    public void setScope(Node node, Scope scope)
    {
        scopes.put(NodeRef.of(node), scope);
    }

    public void addSourceColumns(Field field, Set<Analysis.SourceColumn> sourceColumn)
    {
        originColumnDetails.putAll(field, sourceColumn);
    }

    public void addExpressionFields(Expression expression, Collection<Field> fields)
    {
        fieldLineage.putAll(NodeRef.of(expression), fields);
    }

    public Set<SourceColumn> getExpressionSourceColumns(Expression expression)
    {
        return fieldLineage.get(NodeRef.of(expression)).stream()
                .flatMap(field -> getSourceColumns(field).stream())
                .collect(toImmutableSet());
    }

    public Set<SourceColumn> getSourceColumns(Field field)
    {
        return ImmutableSet.copyOf(originColumnDetails.get(field));
    }

    public void setOrderByExpressions(Node node, List<Expression> items)
    {
        orderByExpressions.put(NodeRef.of(node), ImmutableList.copyOf(items));
    }

    public void setImplicitFromScope(QuerySpecification node, Scope scope)
    {
        implicitFromScopes.put(NodeRef.of(node), scope);
    }

    public Scope getImplicitFromScope(QuerySpecification node)
    {
        return implicitFromScopes.get(NodeRef.of(node));
    }

    public void setAggregates(QuerySpecification node, List<FunctionCall> aggregates)
    {
        this.aggregates.put(NodeRef.of(node), ImmutableList.copyOf(aggregates));
    }

    public List<FunctionCall> getAggregates(QuerySpecification query)
    {
        return aggregates.get(NodeRef.of(query));
    }

    public void setOrderByAggregates(OrderBy node, List<Expression> aggregates)
    {
        this.orderByAggregates.put(NodeRef.of(node), ImmutableList.copyOf(aggregates));
    }

    public List<Expression> getOrderByAggregates(OrderBy node)
    {
        return orderByAggregates.get(NodeRef.of(node));
    }

    public boolean isAggregation(QuerySpecification node)
    {
        return groupingSets.containsKey(NodeRef.of(node));
    }

    public void setGroupingOperations(QuerySpecification querySpecification, List<GroupingOperation> groupingOperations)
    {
        this.groupingOperations.put(NodeRef.of(querySpecification), ImmutableList.copyOf(groupingOperations));
    }

    public List<GroupingOperation> getGroupingOperations(QuerySpecification querySpecification)
    {
        return Optional.ofNullable(groupingOperations.get(NodeRef.of(querySpecification)))
                .orElse(emptyList());
    }

    public RelationType getOutputDescriptor()
    {
        return getOutputDescriptor(root);
    }

    public RelationType getOutputDescriptor(Node node)
    {
        return getScope(node).getRelationType();
    }

    public Scope getScope(Node node)
    {
        return tryGetScope(node).orElseThrow(() -> new IllegalArgumentException(format("Analysis does not contain information for node: %s", node)));
    }

    public void addVisitedTable(TableSchema tableSchema)
    {
        this.visitedTables.add(tableSchema);
    }

    public List<TableSchema> getVisitedTables()
    {
        return this.visitedTables;
    }

    @Immutable
    public static final class SelectExpression
    {
        // expression refers to a select item, either to be returned directly, or unfolded by all-fields reference
        // unfoldedExpressions applies to the latter case, and is a list of subscript expressions
        // referencing each field of the row.
        private final Expression expression;
        private final Optional<List<Expression>> unfoldedExpressions;

        public SelectExpression(Expression expression, Optional<List<Expression>> unfoldedExpressions)
        {
            this.expression = requireNonNull(expression, "expression is null");
            this.unfoldedExpressions = requireNonNull(unfoldedExpressions);
        }

        public Expression getExpression()
        {
            return expression;
        }

        public Optional<List<Expression>> getUnfoldedExpressions()
        {
            return unfoldedExpressions;
        }
    }

    public static class GroupingSetAnalysis
    {
        private final List<Expression> originalExpressions;

        private final List<Set<FieldId>> cubes;
        private final List<List<FieldId>> rollups;
        private final List<List<Set<FieldId>>> ordinarySets;
        private final List<Expression> complexExpressions;

        public GroupingSetAnalysis(
                List<Expression> originalExpressions,
                List<Set<FieldId>> cubes,
                List<List<FieldId>> rollups,
                List<List<Set<FieldId>>> ordinarySets,
                List<Expression> complexExpressions)
        {
            this.originalExpressions = ImmutableList.copyOf(originalExpressions);
            this.cubes = ImmutableList.copyOf(cubes);
            this.rollups = ImmutableList.copyOf(rollups);
            this.ordinarySets = ImmutableList.copyOf(ordinarySets);
            this.complexExpressions = ImmutableList.copyOf(complexExpressions);
        }

        public List<Expression> getOriginalExpressions()
        {
            return originalExpressions;
        }

        public List<Set<FieldId>> getCubes()
        {
            return cubes;
        }

        public List<List<FieldId>> getRollups()
        {
            return rollups;
        }

        public List<List<Set<FieldId>>> getOrdinarySets()
        {
            return ordinarySets;
        }

        public List<Expression> getComplexExpressions()
        {
            return complexExpressions;
        }

        public Set<FieldId> getAllFields()
        {
            return Streams.concat(
                            cubes.stream().flatMap(Collection::stream),
                            rollups.stream().flatMap(Collection::stream),
                            ordinarySets.stream()
                                    .flatMap(Collection::stream)
                                    .flatMap(Collection::stream))
                    .collect(toImmutableSet());
        }
    }

    public static class SourceColumn
    {
        private final QualifiedObjectName tableName;
        private final String columnName;

        @JsonCreator
        public SourceColumn(@JsonProperty("tableName") QualifiedObjectName tableName, @JsonProperty("columnName") String columnName)
        {
            this.tableName = requireNonNull(tableName, "tableName is null");
            this.columnName = requireNonNull(columnName, "columnName is null");
        }

        @JsonProperty
        public QualifiedObjectName getTableName()
        {
            return tableName;
        }

        @JsonProperty
        public String getColumnName()
        {
            return columnName;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableName, columnName);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == this) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }
            SourceColumn entry = (SourceColumn) obj;
            return Objects.equals(tableName, entry.tableName) &&
                    Objects.equals(columnName, entry.columnName);
        }
    }
}
