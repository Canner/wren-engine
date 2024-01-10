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
import java.util.Optional;

public class AccioConfig
{
    public enum DataSourceType
    {
        BIGQUERY,
        POSTGRES,
        DUCKDB,
    }

    @Deprecated
    private File accioMDLFile;
    private File accioMDLDirectory = new File("etc/mdl");
    private DataSourceType dataSourceType;

    @Deprecated
    public Optional<File> getAccioMDLFile()
    {
        return Optional.ofNullable(accioMDLFile);
    }

    @Deprecated
    @Config("accio.file")
    public AccioConfig setAccioMDLFile(File accioMDLFile)
    {
        this.accioMDLFile = accioMDLFile;
        return this;
    }

    @NotNull
    public File getAccioMDLDirectory()
    {
        return accioMDLDirectory;
    }

    @Config("accio.directory")
    public AccioConfig setAccioMDLDirectory(File accioMDLDirectory)
    {
        this.accioMDLDirectory = accioMDLDirectory;
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
