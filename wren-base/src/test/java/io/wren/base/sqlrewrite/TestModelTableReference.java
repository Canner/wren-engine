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

import io.wren.base.dto.Model;
import io.wren.base.dto.TableReference;

public class TestModelTableReference
        extends AbstractTestModel
{
    public TestModelTableReference()
    {
        super();
        orders = Model.onTableReference("Orders", TableReference.tableReference("memory", "main", "orders"), ordersColumns, "orderkey");
        lineitem = Model.onTableReference("Lineitem", TableReference.tableReference("memory", "main", "lineitem"), lineitemColumns, "orderkey_linenumber");
        customer = Model.onTableReference("Customer", TableReference.tableReference("memory", "main", "customer"), customerColumns, "custkey");
    }
}
