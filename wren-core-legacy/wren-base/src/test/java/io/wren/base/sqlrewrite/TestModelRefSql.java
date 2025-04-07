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

import static io.wren.base.dto.Model.model;

public class TestModelRefSql
        extends AbstractTestModel
{
    public TestModelRefSql()
    {
        super();
        orders = model("Orders", "select * from main.orders", ordersColumns, "orderkey");
        lineitem = model("Lineitem", "select * from main.lineitem", lineitemColumns, "orderkey_linenumber");
        customer = model("Customer", "select * from main.customer", customerColumns, "custkey");
    }
}
