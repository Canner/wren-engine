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

package io.wren.base.client.duckdb;

import io.airlift.configuration.Config;

public class DuckDBConnectorConfig
{
    public static final String DUCKDB_CONNECTOR_INIT_SQL_PATH = "duckdb.connector.init-sql-path";
    public static final String DUCKDB_CONNECTOR_SESSION_SQL_PATH = "duckdb.connector.session-sql-path";

    private String initSQLPath = "etc/duckdb/init.sql";
    private String sessionSQLPath = "etc/duckdb/session.sql";

    @Config(DUCKDB_CONNECTOR_INIT_SQL_PATH)
    public void setInitSQLPath(String initSQLPath)
    {
        this.initSQLPath = initSQLPath;
    }

    @Config(DUCKDB_CONNECTOR_SESSION_SQL_PATH)
    public void setSessionSQLPath(String sessionSQLPath)
    {
        this.sessionSQLPath = sessionSQLPath;
    }

    public String getInitSQLPath()
    {
        return initSQLPath;
    }

    public String getSessionSQLPath()
    {
        return sessionSQLPath;
    }
}
