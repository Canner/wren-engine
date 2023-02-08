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

package io.graphmdl.graphml.analyzer;

import io.graphmdl.graphml.RelationshipCteGenerator;
import io.graphmdl.graphml.base.dto.Model;
import io.graphmdl.graphml.base.dto.Relationship;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.WithQuery;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class Analysis
{
    private final Statement root;
    private final Set<QualifiedName> tables = new HashSet<>();
    private final RelationshipCteGenerator relationshipCteGenerator;
    private final Map<NodeRef<DereferenceExpression>, DereferenceExpression> relationshipFields = new HashMap<>();
    private final Map<NodeRef<Table>, Set<String>> replaceTableWithCTEs = new HashMap<>();
    private final Set<Relationship> relationships = new HashSet<>();
    private final Set<Model> models = new HashSet<>();

    Analysis(Statement statement, RelationshipCteGenerator relationshipCteGenerator)
    {
        this.root = requireNonNull(statement, "statement is null");
        this.relationshipCteGenerator = relationshipCteGenerator;
    }

    public Statement getRoot()
    {
        return root;
    }

    void addTable(QualifiedName tableName)
    {
        tables.add(tableName);
    }

    public Set<QualifiedName> getTables()
    {
        return Set.copyOf(tables);
    }

    public Map<String, WithQuery> getRelationshipCTE()
    {
        return relationshipCteGenerator.getRegisteredCte();
    }

    public Map<String, String> getRelationshipNameMapping()
    {
        return relationshipCteGenerator.getNameMapping();
    }

    public Map<String, RelationshipCteGenerator.RelationshipCTEJoinInfo> getRelationshipInfoMapping()
    {
        return relationshipCteGenerator.getRelationshipInfoMapping();
    }

    void addRelationshipFields(Map<NodeRef<DereferenceExpression>, DereferenceExpression> relationshipFields)
    {
        this.relationshipFields.putAll(relationshipFields);
    }

    public Map<NodeRef<DereferenceExpression>, DereferenceExpression> getRelationshipFields()
    {
        return Map.copyOf(relationshipFields);
    }

    void addReplaceTableWithCTEs(NodeRef<Table> tableNodeRef, Set<String> relationshipCTENames)
    {
        this.replaceTableWithCTEs.put(tableNodeRef, relationshipCTENames);
    }

    public Map<NodeRef<Table>, Set<String>> getReplaceTableWithCTEs()
    {
        return Map.copyOf(replaceTableWithCTEs);
    }

    void addRelationships(Set<Relationship> relationships)
    {
        this.relationships.addAll(relationships);
    }

    public Set<Relationship> getRelationships()
    {
        return relationships;
    }

    void addModels(Set<Model> models)
    {
        this.models.addAll(models);
    }

    public Set<Model> getModels()
    {
        return models;
    }
}
