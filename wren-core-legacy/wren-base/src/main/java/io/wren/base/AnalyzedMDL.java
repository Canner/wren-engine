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

package io.wren.base;

import io.wren.base.sqlrewrite.WrenDataLineage;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

public class AnalyzedMDL
{
    private final WrenMDL wrenMDL;
    private final WrenDataLineage wrenDataLineage;
    private final String version;

    public AnalyzedMDL(WrenMDL wrenMDL, @Nullable String version)
    {
        this.wrenMDL = requireNonNull(wrenMDL);
        this.wrenDataLineage = WrenDataLineage.analyze(wrenMDL);
        this.version = version;
    }

    public AnalyzedMDL(WrenMDL wrenMDL, WrenDataLineage wrenDataLineage, @Nullable String version)
    {
        this.wrenMDL = requireNonNull(wrenMDL);
        this.wrenDataLineage = requireNonNull(wrenDataLineage);
        this.version = version;
    }

    public WrenMDL getWrenMDL()
    {
        return wrenMDL;
    }

    public WrenDataLineage getWrenDataLineage()
    {
        return wrenDataLineage;
    }

    @Nullable
    public String getVersion()
    {
        return version;
    }
}
