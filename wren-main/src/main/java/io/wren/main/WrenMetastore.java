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

package io.wren.main;

import io.wren.base.AnalyzedMDL;
import io.wren.base.WrenMDL;
import io.wren.base.sqlrewrite.WrenDataLineage;

import java.util.concurrent.atomic.AtomicReference;

import static io.wren.base.WrenMDL.EMPTY;

public class WrenMetastore
{
    private final AtomicReference<AnalyzedMDL> analyzed = new AtomicReference<>(new AnalyzedMDL(EMPTY, WrenDataLineage.EMPTY, "0"));

    public AnalyzedMDL getAnalyzedMDL()
    {
        return analyzed.get();
    }

    public synchronized void setWrenMDL(WrenMDL wrenMDL, String version)
    {
        this.analyzed.set(new AnalyzedMDL(wrenMDL, WrenDataLineage.analyze(wrenMDL), version));
    }
}
