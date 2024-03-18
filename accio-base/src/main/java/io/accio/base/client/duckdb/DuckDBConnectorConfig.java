package io.accio.base.client.duckdb;

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
