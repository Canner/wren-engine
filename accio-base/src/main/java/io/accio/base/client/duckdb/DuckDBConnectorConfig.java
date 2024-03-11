package io.accio.base.client.duckdb;

import io.airlift.configuration.Config;

import java.nio.file.Path;

public class DuckDBConnectorConfig
{
    private Path settingDir = Path.of("etc/duckdb-connector");

    private String initSQL;

    private String sessionSQL;

    public Path getSettingDir()
    {
        return settingDir;
    }

    @Config("duckdb.connector.setting-dir")
    public void setSettingDir(String settingDir)
    {
        this.settingDir = Path.of(settingDir);
    }

    public String getInitSQL()
    {
        return initSQL;
    }

    public void setInitSQL(String initSQL)
    {
        this.initSQL = initSQL;
    }

    public Path getInitSQLPath()
    {
        return settingDir.resolve("init.sql");
    }

    public String getSessionSQL()
    {
        return sessionSQL;
    }

    public void setSessionSQL(String sessionSQL)
    {
        this.sessionSQL = sessionSQL;
    }

    public Path getSessionSQLPath()
    {
        return settingDir.resolve("session.sql");
    }
}
