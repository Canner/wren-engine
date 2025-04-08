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

package io.wren.base.sqlrewrite.analyzer;

import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.WithQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class Scope
{
    private final Optional<Scope> parent;
    private final RelationId relationId;
    private final RelationType relationType;
    private final boolean isDataSourceScope;
    private final Map<String, WithQuery> namedQueries;

    private Scope(
            Scope parent,
            RelationId relationId,
            RelationType relationType,
            boolean isDataSourceScope,
            Map<String, WithQuery> namedQueries)
    {
        this.parent = Optional.ofNullable(parent);
        this.relationId = requireNonNull(relationId, "relationId is null");
        this.relationType = requireNonNull(relationType, "relationType is null");
        this.isDataSourceScope = isDataSourceScope;
        this.namedQueries = requireNonNull(namedQueries, "namedQueries is null");
    }

    public Optional<Scope> getParent()
    {
        return parent;
    }

    public RelationType getRelationType()
    {
        return relationType;
    }

    public RelationId getRelationId()
    {
        return relationId;
    }

    public boolean isDataSourceScope()
    {
        return isDataSourceScope;
    }

    public Optional<WithQuery> getNamedQuery(String name)
    {
        if (namedQueries.containsKey(name)) {
            return Optional.of(namedQueries.get(name));
        }

        if (parent.isPresent()) {
            return parent.get().getNamedQuery(name);
        }

        return Optional.empty();
    }

    /**
     * get the columns matching the specified name in the current scope and all parent scopes
     */
    public List<Field> resolveFields(QualifiedName name)
    {
        List<Field> fields = new ArrayList<>(relationType.resolveFields(name));
        parent.ifPresent(scope -> fields.addAll(scope.resolveFields(name)));
        return ImmutableList.copyOf(fields);
    }

    /**
     * get the columns matching the specified name in the current scope and all parent scopes
     */
    public Optional<Field> resolveAnyField(QualifiedName name)
    {
        return relationType.resolveAnyField(name).or(() -> {
            if (parent.isPresent()) {
                return parent.get().resolveAnyField(name);
            }
            return Optional.empty();
        });
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private Optional<Scope> parent = Optional.empty();
        private RelationId relationId = RelationId.anonymous();
        private RelationType relationType = new RelationType();
        private boolean isDataSourceScope;
        private final Map<String, WithQuery> namedQueries = new HashMap<>();

        public Builder relationType(RelationType relationType)
        {
            this.relationType = relationType;
            return this;
        }

        public Builder relationId(RelationId relationId)
        {
            this.relationId = relationId;
            return this;
        }

        public Builder parent(Optional<Scope> parent)
        {
            checkArgument(this.parent.isEmpty(), "parent is already set");
            this.parent = requireNonNull(parent, "parent is null");
            return this;
        }

        public Builder isDataSourceScope(boolean isDataSourceScope)
        {
            this.isDataSourceScope = isDataSourceScope;
            return this;
        }

        public Builder namedQuery(String name, WithQuery withQuery)
        {
            checkArgument(!containsNamedQuery(name), "Query '%s' is already added", name);
            namedQueries.put(name, withQuery);
            return this;
        }

        public boolean containsNamedQuery(String name)
        {
            return namedQueries.containsKey(name);
        }

        public Scope build()
        {
            return new Scope(parent.orElse(null), relationId, relationType, isDataSourceScope, namedQueries);
        }
    }
}
