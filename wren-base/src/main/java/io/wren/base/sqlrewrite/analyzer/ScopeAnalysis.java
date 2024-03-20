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

import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ScopeAnalysis
{
    private final List<Relation> usedWrenObjects = new ArrayList<>();
    private final Map<NodeRef<Node>, String> aliasedMap = new HashMap<>();

    public void addUsedWrenObject(Table model)
    {
        usedWrenObjects.add(new Relation(model.getName().getSuffix(), aliasedMap.get(NodeRef.of(model))));
    }

    public List<Relation> getUsedWrenObjects()
    {
        return List.copyOf(usedWrenObjects);
    }

    public void addAliasedNode(Node node, String alias)
    {
        aliasedMap.put(NodeRef.of(node), alias);
    }

    public static class Relation
    {
        private final String name;
        private final String alias;

        public Relation(String name, String alias)
        {
            this.name = requireNonNull(name, "name is null");
            this.alias = alias;
        }

        public String getName()
        {
            return name;
        }

        public Optional<String> getAlias()
        {
            return Optional.ofNullable(alias);
        }
    }
}
