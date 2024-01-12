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

package io.accio.base;

import io.accio.base.sqlrewrite.AccioDataLineage;

import static java.util.Objects.requireNonNull;

public class AnalyzedMDL
{
    private final AccioMDL accioMDL;
    private final AccioDataLineage accioDataLineage;

    public AnalyzedMDL(AccioMDL accioMDL)
    {
        this.accioMDL = requireNonNull(accioMDL);
        this.accioDataLineage = AccioDataLineage.analyze(accioMDL);
    }

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
