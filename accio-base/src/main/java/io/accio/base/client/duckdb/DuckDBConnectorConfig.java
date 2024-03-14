package io.accio.base.client.duckdb;

import io.airlift.configuration.Config;

import java.nio.file.Path;

public class DuckDBConnectorConfig
{
    public static final String DUCKDB_SETTING_DIR = "duckdb.connector.setting-dir";

    private Path settingDir = Path.of("etc/duckdb-connector");

    public Path getSettingDir()
    {
        return settingDir;
    }

    @Config(DUCKDB_SETTING_DIR)
    public void setSettingDir(String settingDir)
    {
        this.settingDir = Path.of(settingDir);
    }

    public Path getInitSQLPath()
    {
        return settingDir.resolve("init.sql");
    }

    public Path getSessionSQLPath()
    {
        return settingDir.resolve("session.sql");
    }
}
