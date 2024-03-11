package io.accio.main.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DuckDBSettingsDto
{
    private String initSQL;

    private String sessionSQL;

    @JsonProperty
    public String getInitSQL()
    {
        return initSQL;
    }

    public void setInitSQL(String initSQL)
    {
        this.initSQL = initSQL;
    }

    @JsonProperty
    public String getSessionSQL()
    {
        return sessionSQL;
    }

    public void setSessionSQL(String sessionSQL)
    {
        this.sessionSQL = sessionSQL;
    }
}
