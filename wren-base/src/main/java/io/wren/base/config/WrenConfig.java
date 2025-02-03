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

package io.wren.base.config;

import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.nio.file.Paths;

public class WrenConfig
{
    public static final String WREN_DIRECTORY = "wren.directory";
    public static final String WREN_DATASOURCE_TYPE = "wren.datasource.type";
    public static final String WREN_ENABLE_DYNAMIC_FIELDS = "wren.experimental-enable-dynamic-fields";

    public enum DataSourceType
    {
        @Deprecated
        BIGQUERY,
        @Deprecated
        POSTGRES,
        DUCKDB,
        @Deprecated
        SNOWFLAKE
    }

    private File wrenMDLDirectory = Paths.get("etc/mdl").toFile();
    private DataSourceType dataSourceType = DataSourceType.DUCKDB;
    private boolean enableDynamicFields = true;

    @NotNull
    public File getWrenMDLDirectory()
    {
        return wrenMDLDirectory;
    }

    @Config(WREN_DIRECTORY)
    public WrenConfig setWrenMDLDirectory(File wrenMDLDirectory)
    {
        this.wrenMDLDirectory = wrenMDLDirectory;
        return this;
    }

    public DataSourceType getDataSourceType()
    {
        return dataSourceType;
    }

    @Config(WREN_DATASOURCE_TYPE)
    public WrenConfig setDataSourceType(DataSourceType dataSourceType)
    {
        this.dataSourceType = dataSourceType;
        return this;
    }

    public boolean getEnableDynamicFields()
    {
        return enableDynamicFields;
    }

    @Config(WREN_ENABLE_DYNAMIC_FIELDS)
    public WrenConfig setEnableDynamicFields(boolean enableDynamicFields)
    {
        this.enableDynamicFields = enableDynamicFields;
        return this;
    }
}
