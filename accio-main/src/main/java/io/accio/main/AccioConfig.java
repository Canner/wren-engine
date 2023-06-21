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

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import java.io.File;

public class AccioConfig
{
    public enum DataSourceType
    {
        BIGQUERY,
        POSTGRES,
    }

    private File accioMDLFile = new File("etc/acciomdl.json");
    private DataSourceType dataSourceType;

    @NotNull
    public File getAccioMDLFile()
    {
        return accioMDLFile;
    }

    @Config("accio.file")
    public AccioConfig setAccioMDLFile(File accioMDLFile)
    {
        this.accioMDLFile = accioMDLFile;
        return this;
    }

    @NotNull
    public DataSourceType getDataSourceType()
    {
        return dataSourceType;
    }

    @Config("accio.datasource.type")
    public AccioConfig setDataSourceType(DataSourceType dataSourceType)
    {
        this.dataSourceType = dataSourceType;
        return this;
    }
}
