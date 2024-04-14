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

import com.google.common.collect.ImmutableSet;
import io.trino.sql.tree.Query;
import io.wren.base.dto.Model;
import io.wren.base.dto.Relationable;

import java.util.Optional;
import java.util.Set;

import static io.wren.base.sqlrewrite.Utils.parseQuery;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class ModelInfo
        implements QueryDescriptor
{
    public static ModelInfo get(Model model)
    {
        return new ModelInfo(model);
    }

    public static String ORIGINAL_SUFFIX = "_original";

    private final String name;
    private final Query query;
    private final Model model;

    private ModelInfo(Model model)
    {
        this.model = requireNonNull(model, "model is null");
        this.name = model.getName();
        this.query = parseQuery(getBaseModelSql(model));
    }

    @Override
    public String getName()
    {
        return name + ORIGINAL_SUFFIX;
    }

    @Override
    public Set<String> getRequiredObjects()
    {
        return ImmutableSet.of();
    }

    @Override
    public Query getQuery()
    {
        return query;
    }

    @Override
    public Optional<Relationable> getRelationable()
    {
        return Optional.of(model);
    }

    private static String getBaseModelSql(Model model)
    {
        String selectItems = model.getColumns().stream()
                .filter(column -> !column.isCalculated())
                .filter(column -> column.getRelationship().isEmpty())
                .map(column -> STR."\{column.getSqlExpression()} AS \"\{column.getName()}\"")
                .collect(joining(", "));
        return STR."SELECT \{selectItems} FROM \{initRefSql(model)} AS \"\{model.getName()}\"";
    }

    private static String initRefSql(Model model)
    {
        if (model.getRefSql() != null) {
            return STR."(\{model.getRefSql()})";
        }
        else if (model.getBaseObject() != null) {
            return STR."(SELECT * FROM \"\{model.getBaseObject()}\")";
        }
        else if (model.getTableReference() != null) {
            return model.getTableReference().toQualifiedName();
        }
        else {
            throw new IllegalArgumentException("cannot get reference sql from model");
        }
    }
}
