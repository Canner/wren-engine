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

package io.wren.base.sqlrewrite;

import io.wren.base.WrenMDL;
import io.wren.base.dto.Column;
import io.wren.base.dto.JoinType;
import io.wren.base.dto.Model;
import io.wren.base.dto.Relationable;
import io.wren.base.dto.Relationship;
import io.wren.base.sqlrewrite.analyzer.ExpressionRelationshipInfo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.wren.base.Utils.checkArgument;
import static io.wren.base.sqlrewrite.WrenDataLineage.RelationableReference;
import static java.util.Objects.requireNonNull;

// TODO: Turn this into interface
public abstract class RelationableSqlRender
{
    protected final RelationableReference reference;
    protected final WrenMDL mdl;
    protected final String refSql;
    // collect dependent models
    protected final Set<String> requiredObjects;
    // key is alias_name.column_name, value is column name, this map is used to compose select items in model sql
    protected final List<String> selectItems = new ArrayList<>();
    // calculatedRequiredRelationshipInfos collects all join condition needed in model calculated field and the original column name.
    // It is used to compose join conditions in model sql.
    protected final List<CalculatedFieldRelationshipInfo> calculatedRequiredRelationshipInfos = new ArrayList<>();
    // key is column name in model, value is column expression, this map store columns not use relationships
    protected final Map<String, String> columnWithoutRelationships = new LinkedHashMap<>();

    public RelationableSqlRender(RelationableReference reference, WrenMDL mdl)
    {
        this.reference = requireNonNull(reference);
        this.mdl = requireNonNull(mdl);
        checkArgument(reference.getRelationable().isPresent(), "relationable is not present");
        this.refSql = initRefSql(reference.getRelationable().get());
        this.requiredObjects = new HashSet<>();
        if (reference.getRelationable().get().getBaseObject() != null) {
            requiredObjects.add(reference.getRelationable().get().getBaseObject());
        }
    }

    protected abstract String initRefSql(Relationable relationable);

    protected abstract RelationInfo render();

    protected static String getRelationableAlias(String baseModelName)
    {
        return baseModelName + "_relationsub";
    }

    protected abstract List<SubQueryJoinInfo> getCalculatedSubQuery(Model baseModel, List<CalculatedFieldRelationshipInfo> calculatedFieldRelationshipInfo);

    protected abstract void collectRelationship(Column column, Model baseModel);

    protected abstract String getQuerySql(String selectItemsSql, String tableJoinsSql);

    protected abstract String getModelSubQuerySelectItemsExpression(Map<String, String> columnWithoutRelationships);

    protected abstract String getSelectItemsExpression(Column column, Optional<String> relationalBase);

    public static class CalculatedFieldRelationshipInfo
    {
        private final Column column;
        private final List<ExpressionRelationshipInfo> expressionRelationshipInfo;
        private final boolean isAggregated;

        public CalculatedFieldRelationshipInfo(Column column, List<ExpressionRelationshipInfo> expressionRelationshipInfo)
        {
            this.column = requireNonNull(column);
            this.expressionRelationshipInfo = requireNonNull(expressionRelationshipInfo);
            this.isAggregated = expressionRelationshipInfo.stream()
                    .map(ExpressionRelationshipInfo::getRelationships)
                    .flatMap(List::stream)
                    .map(Relationship::getJoinType)
                    .anyMatch(JoinType::isToMany);
        }

        public String getAlias()
        {
            return column.getName();
        }

        public Column getColumn()
        {
            return column;
        }

        public List<ExpressionRelationshipInfo> getExpressionRelationshipInfo()
        {
            return expressionRelationshipInfo;
        }

        public boolean isAggregated()
        {
            return isAggregated;
        }
    }

    public static class SubQueryJoinInfo
    {
        private final String sql;
        private final String subqueryAlias;
        private final String joinCriteria;

        public SubQueryJoinInfo(String sql, String subqueryAlias, String joinCriteria)
        {
            this.sql = sql;
            this.subqueryAlias = subqueryAlias;
            this.joinCriteria = joinCriteria;
        }

        public String getSql()
        {
            return sql;
        }

        public String getSubqueryAlias()
        {
            return subqueryAlias;
        }

        public String getJoinCriteria()
        {
            return joinCriteria;
        }
    }
}
