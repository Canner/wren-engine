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

package io.accio.main;

import io.accio.base.AccioMDL;
import io.accio.base.AnalyzedMDL;
import io.accio.base.metadata.FunctionBundle;
import io.accio.base.sqlrewrite.AccioDataLineage;

import java.util.concurrent.atomic.AtomicReference;

import static io.accio.base.AccioMDL.EMPTY;

public class AccioMetastore
{
    private final AtomicReference<AnalyzedMDL> analyzed = new AtomicReference<>(new AnalyzedMDL(EMPTY, AccioDataLineage.EMPTY, "0"));
    private final AtomicReference<FunctionBundle> functionBundle = new AtomicReference<>(null);

    public AnalyzedMDL getAnalyzedMDL()
    {
        return analyzed.get();
    }

    public FunctionBundle getFunctionBundle()
    {
        return functionBundle.get();
    }

    public synchronized void setAccioMDL(AccioMDL accioMDL, String version)
    {
        this.analyzed.set(new AnalyzedMDL(accioMDL, AccioDataLineage.analyze(accioMDL), version));
        this.functionBundle.set(FunctionBundle.create(accioMDL.getCatalog()));
    }
}
