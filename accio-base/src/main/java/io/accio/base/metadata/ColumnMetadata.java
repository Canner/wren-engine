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
package io.accio.base.metadata;

import io.accio.base.type.PGType;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class ColumnMetadata
{
    private final String name;
    private final PGType<?> type;

    private ColumnMetadata(String name, PGType<?> type)
    {
        this.name = name;
        this.type = type;
    }

    public String getName()
    {
        return name;
    }

    public PGType<?> getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ColumnMetadata{");
        sb.append("name='").append(name).append('\'');
        sb.append(", type=").append(type);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ColumnMetadata other = (ColumnMetadata) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.type, other.type);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String name;
        private PGType<?> type;

        private Builder() {}

        public Builder setName(String name)
        {
            this.name = requireNonNull(name, "name is null");
            return this;
        }

        public Builder setType(PGType<?> type)
        {
            this.type = requireNonNull(type, "type is null");
            return this;
        }

        public ColumnMetadata build()
        {
            return new ColumnMetadata(name, type);
        }
    }
}
