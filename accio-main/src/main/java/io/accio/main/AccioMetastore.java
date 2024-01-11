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
import io.accio.base.sqlrewrite.AccioDataLineage;

import java.util.concurrent.atomic.AtomicReference;

import static io.accio.base.AccioMDL.EMPTY;
import static java.util.Objects.requireNonNull;

public class AccioMetastore
{
    private final AtomicReference<AnalyzedMDL> analyzed = new AtomicReference<>(new AnalyzedMDL(EMPTY, AccioDataLineage.EMPTY));

    public AccioMDL getAccioMDL()
    {
        return analyzed.get().getAccioMDL();
    }

    public AccioDataLineage getAccioDataLineage()
    {
        return analyzed.get().getAccioDataLineage();
    }

    public synchronized void setAccioMDL(AccioMDL accioMDL)
    {
        this.analyzed.set(new AnalyzedMDL(accioMDL, AccioDataLineage.analyze(accioMDL)));
    }

    private static class AnalyzedMDL
    {
        private final AccioMDL accioMDL;
        private final AccioDataLineage accioDataLineage;

        public AnalyzedMDL(AccioMDL accioMDL, AccioDataLineage accioDataLineage)
        {
            this.accioMDL = requireNonNull(accioMDL);
            this.accioDataLineage = requireNonNull(accioDataLineage);
        }

        public AccioMDL getAccioMDL()
        {
            return accioMDL;
        }

        public AccioDataLineage getAccioDataLineage()
        {
            return accioDataLineage;
        }
    }
}
