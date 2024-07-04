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

import java.util.Optional;

@Deprecated
public class SnowflakeConfig
{
    public static final String SNOWFLAKE_JDBC_URL = "snowflake.jdbc.url";
    public static final String SNOWFLAKE_USER = "snowflake.user";
    public static final String SNOWFLAKE_PASSWORD = "snowflake.password";
    public static final String SNOWFLAKE_ROLE = "snowflake.role";
    public static final String SNOWFLAKE_WAREHOUSE = "snowflake.warehouse";
    public static final String SNOWFLAKE_DATABASE = "snowflake.database";
    public static final String SNOWFLAKE_SCHEMA = "snowflake.schema";

    private String jdbcUrl;
    private String user;
    private String password;
    private String role;
    private String warehouse;
    private String database;
    private String schema;

    public String getJdbcUrl()
    {
        return jdbcUrl;
    }

    @Config(SNOWFLAKE_JDBC_URL)
    public SnowflakeConfig setJdbcUrl(String jdbcUrl)
    {
        this.jdbcUrl = jdbcUrl;
        return this;
    }

    public String getUser()
    {
        return user;
    }

    @Config(SNOWFLAKE_USER)
    public SnowflakeConfig setUser(String user)
    {
        this.user = user;
        return this;
    }

    public String getPassword()
    {
        return password;
    }

    @Config(SNOWFLAKE_PASSWORD)
    public SnowflakeConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public Optional<String> getRole()
    {
        return Optional.ofNullable(role);
    }

    @Config(SNOWFLAKE_ROLE)
    public SnowflakeConfig setRole(String role)
    {
        this.role = role;
        return this;
    }

    public Optional<String> getWarehouse()
    {
        return Optional.ofNullable(warehouse);
    }

    @Config(SNOWFLAKE_WAREHOUSE)
    public SnowflakeConfig setWarehouse(String warehouse)
    {
        this.warehouse = warehouse;
        return this;
    }

    public Optional<String> getDatabase()
    {
        return Optional.ofNullable(database);
    }

    @Config(SNOWFLAKE_DATABASE)
    public SnowflakeConfig setDatabase(String database)
    {
        this.database = database;
        return this;
    }

    public Optional<String> getSchema()
    {
        return Optional.ofNullable(schema);
    }

    @Config(SNOWFLAKE_SCHEMA)
    public SnowflakeConfig setSchema(String schema)
    {
        this.schema = schema;
        return this;
    }
}
