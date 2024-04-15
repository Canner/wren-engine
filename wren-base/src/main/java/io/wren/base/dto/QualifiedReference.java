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

package io.wren.base.dto;

import static java.util.Objects.requireNonNull;

public class QualifiedReference
{
    private final Dataset dataset;
    private final Column column;

    public QualifiedReference(Dataset dataset, Column column)
    {
        this.dataset = requireNonNull(dataset, "dataset is null");
        this.column = requireNonNull(column, "column is null");
    }

    public Dataset getDataset()
    {
        return dataset;
    }

    public Column getColumn()
    {
        return column;
    }

    @Override
    public String toString()
    {
        return dataset.getName() + "." + column.getName();
    }
}
