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

import io.cml.graphml.RelationshipCteGenerator;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.WithQuery;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class Analysis
{
    private final Set<QualifiedName> tables = new HashSet<>();
    private final RelationshipCteGenerator relationshipCteGenerator;
    private final Map<NodeRef, DereferenceExpression> boundRelationshipNodes = new HashMap<>();

    Analysis(RelationshipCteGenerator relationshipCteGenerator)
    {
        this.relationshipCteGenerator = relationshipCteGenerator;
    }

    void addTable(QualifiedName tableName)
    {
        tables.add(tableName);
    }

    public Set<QualifiedName> getTables()
    {
        return Set.copyOf(tables);
    }

    public Optional<String> getRelationshipCTEName(String rsName)
    {
        return Optional.ofNullable(relationshipCteGenerator.getNameMapping().get(rsName));
    }

    public Map<String, WithQuery> getRelationshipCTE()
    {
        return relationshipCteGenerator.getRegisteredCte();
    }

    public void bindRelationship(NodeRef original, DereferenceExpression target)
    {
        boundRelationshipNodes.put(original, target);
    }

    public DereferenceExpression transferRelationship(NodeRef node)
    {
        return boundRelationshipNodes.get(node);
    }
}
