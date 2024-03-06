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

package io.accio.connector.postgres;

import io.airlift.configuration.Config;

public class PostgresConfig
{
    private String jdbcUrl;
    private String user;
    private String password;

    public String getJdbcUrl()
    {
        return jdbcUrl;
    }

    @Config("postgres.jdbc.url")
    public PostgresConfig setJdbcUrl(String jdbcUrl)
    {
        this.jdbcUrl = jdbcUrl;
        return this;
    }

    public String getUser()
    {
        return user;
    }

    @Config("postgres.user")
    public PostgresConfig setUser(String user)
    {
        this.user = user;
        return this;
    }

    public String getPassword()
    {
        return password;
    }

    @Config("postgres.password")
    public PostgresConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }
}
