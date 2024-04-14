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
import io.wren.base.dto.Model;
import io.wren.base.dto.Relationable;

import java.util.Set;

import static io.wren.base.Utils.checkArgument;
import static io.wren.base.sqlrewrite.WrenDataLineage.RelationableReference;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class DerivedModelSqlRender
        extends ModelRelationSqlRender
{

    public DerivedModelSqlRender(RelationableReference reference, WrenMDL mdl, Set<String> requiredFields)
    {
        super(reference, mdl, requiredFields);
    }

    public DerivedModelSqlRender(RelationableReference reference, WrenMDL mdl)
    {
        super(reference, mdl);
    }

    @Override
    protected String initRefSql(Relationable relationable)
    {
        checkArgument(relationable instanceof Model, "relationable must be model");
        checkArgument(relationable.getBaseObject() != null, "base object is null");
        return STR."(SELECT * FROM \"\{relationable.getBaseObject()}\")";
    }

    @Override
    protected String getModelSubQuery(Model baseModel)
    {
        String modelSubQuerySelectItemsExpression = getModelSubQuerySelectItemsExpression(columnWithoutRelationships);
        return format("(SELECT %s FROM %s AS \"%s\") AS \"%s\"",
                modelSubQuerySelectItemsExpression,
                refSql,
                baseModel.getName(),
                baseModel.getName());
    }

    @Override
    protected String getBaseModelSql(Model model)
    {
        String selectItems = model.getColumns().stream()
                .filter(column -> !column.isCalculated())
                .filter(column -> column.getRelationship().isEmpty())
                .map(column -> format("%s AS \"%s\"", column.getSqlExpression(), column.getName()))
                .collect(joining(", "));
        return format("SELECT %s FROM %s AS \"%s\"", selectItems, refSql, model.getName());
    }
}
