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

package io.accio.base.config;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import java.io.File;

public class AccioConfig
{
    public static final String ACCIO_DIRECTORY = "accio.directory";
    public static final String ACCIO_DATASOURCE_TYPE = "accio.datasource.type";
    public static final String ACCIO_ENABLE_DYNAMIC_FIELDS = "accio.experimental-enable-dynamic-fields";

    public enum DataSourceType
    {
        BIGQUERY,
        POSTGRES,
        DUCKDB,
    }

    private File accioMDLDirectory = new File("etc/mdl");
    private DataSourceType dataSourceType = DataSourceType.DUCKDB;
    private boolean enableDynamicFields;

    @NotNull
    public File getAccioMDLDirectory()
    {
        return accioMDLDirectory;
    }

    @Config(ACCIO_DIRECTORY)
    public AccioConfig setAccioMDLDirectory(File accioMDLDirectory)
    {
        this.accioMDLDirectory = accioMDLDirectory;
        return this;
    }

    public DataSourceType getDataSourceType()
    {
        return dataSourceType;
    }

    @Config(ACCIO_DATASOURCE_TYPE)
    public AccioConfig setDataSourceType(DataSourceType dataSourceType)
    {
        this.dataSourceType = dataSourceType;
        return this;
    }

    public boolean getEnableDynamicFields()
    {
        return enableDynamicFields;
    }

    @Config(ACCIO_ENABLE_DYNAMIC_FIELDS)
    public AccioConfig setEnableDynamicFields(boolean enableDynamicFields)
    {
        this.enableDynamicFields = enableDynamicFields;
        return this;
    }
}
