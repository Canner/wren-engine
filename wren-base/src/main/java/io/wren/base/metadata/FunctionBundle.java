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

package io.wren.base.metadata;

import io.wren.base.pgcatalog.function.DataSourceFunctionRegistry;
import io.wren.base.pgcatalog.function.PgMetastoreFunctionRegistry;

import java.util.Optional;

public class FunctionBundle
{
    private static final BasicFunctionRegistry basicFunctionRegistry;
    private static final PgMetastoreFunctionRegistry pgMetastoreFunctionRegistry;
    private static final DataSourceFunctionRegistry datSourceFunctionRegistry;

    private FunctionBundle() {}

    static {
        basicFunctionRegistry = new BasicFunctionRegistry();
        pgMetastoreFunctionRegistry = new PgMetastoreFunctionRegistry();
        datSourceFunctionRegistry = new DataSourceFunctionRegistry();
    }

    public static Optional<Function> getFunction(String name, int numArgument)
    {
        return basicFunctionRegistry.getFunction(name, numArgument)
                .or(() -> pgMetastoreFunctionRegistry.getFunction(name, numArgument))
                .or(() -> datSourceFunctionRegistry.getFunction(name, numArgument));
    }
}
