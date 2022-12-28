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

package io.cml.graphml.analyzer;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class Scope
{
    private final Optional<Scope> parent;
    private final Optional<RelationType> relationType;
    private final boolean isTableScope;

    private Scope(Optional<Scope> parent, Optional<RelationType> relationType, boolean isTableScope)
    {
        this.parent = requireNonNull(parent, "parent is null");
        this.relationType = requireNonNull(relationType, "relationType is null");
        this.isTableScope = isTableScope;
    }

    public Optional<Scope> getParent()
    {
        return parent;
    }

    public Optional<RelationType> getRelationType()
    {
        return relationType;
    }

    public boolean isTableScope()
    {
        return isTableScope;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private Optional<Scope> parent = Optional.empty();
        private Optional<RelationType> relationType = Optional.empty();
        private boolean isTableScope;

        public Builder relationType(Optional<RelationType> relationType)
        {
            this.relationType = requireNonNull(relationType, "relationType is null");
            return this;
        }

        public Builder parent(Optional<Scope> parent)
        {
            checkArgument(this.parent.isEmpty(), "parent is already set");
            this.parent = requireNonNull(parent, "parent is null");
            return this;
        }

        public Builder isTableScope(boolean isTableScope)
        {
            this.isTableScope = isTableScope;
            return this;
        }

        public Scope build()
        {
            return new Scope(parent, relationType, isTableScope);
        }
    }
}
