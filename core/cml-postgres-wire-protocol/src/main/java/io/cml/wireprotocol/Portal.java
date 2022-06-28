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

package io.cml.wireprotocol;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.cml.spi.ConnectorRecordIterable;
import io.cml.spi.Parameter;
import io.cml.spi.type.PGType;
import io.cml.spi.type.PGTypes;

import javax.annotation.Nullable;
import javax.annotation.PreDestroy;
import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

public class Portal
{
    private static final Logger LOG = Logger.get(Portal.class);

    private final PreparedStatement preparedStatement;
    private final List<Object> params;
    private ConnectorRecordIterable connectorRecordIterable;
    private long rowCount;

    @Nullable
    private final FormatCodes.FormatCode[] resultFormatCodes;

    public Portal(PreparedStatement preparedStatement, List<Object> params, @Nullable FormatCodes.FormatCode[] resultFormatCodes)
    {
        this.preparedStatement = preparedStatement;
        this.params = params;
        this.resultFormatCodes = resultFormatCodes;
    }

    public PreparedStatement getPreparedStatement()
    {
        return preparedStatement;
    }

    @Nullable
    public FormatCodes.FormatCode[] getResultFormatCodes()
    {
        return resultFormatCodes;
    }

    public String getExecuteStatement()
    {
        String name = formatName(preparedStatement.getName());
        if (params.isEmpty()) {
            return format("EXECUTE \"%s\"", name);
        }
        List<String> stringParams = IntStream.range(0, params.size())
                .mapToObj(i -> getParamsSqlString(params.get(i), preparedStatement.getParamTypeOids().get(i)))
                .collect(toImmutableList());
        return format("EXECUTE \"%s\" USING %s", name, Joiner.on(",").join(stringParams));
    }

    @NotNull
    private static String formatName(String name)
    {
        if (name.startsWith("\"") && name.endsWith("\"")) {
            return "\"" + name + "\"";
        }
        return name;
    }

    private String getParamsSqlString(Object value, int oid)
    {
        throw new UnsupportedOperationException();
    }

    public ConnectorRecordIterable getConnectorRecordIterable()
    {
        return connectorRecordIterable;
    }

    public void setResultSetSender(ConnectorRecordIterable connectorRecordIterable)
    {
        this.connectorRecordIterable = connectorRecordIterable;
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public void setRowCount(long rowCount)
    {
        this.rowCount = rowCount;
    }

    public boolean isSuspended()
    {
        return connectorRecordIterable != null;
    }

    public List<Parameter> getParameters()
    {
        List<PGType<?>> pgTypes = preparedStatement.getParamTypeOids().stream().map(PGTypes::oidToPgType).collect(Collectors.toList());
        ImmutableList.Builder<Parameter> builder = ImmutableList.builder();
        for (int i = 0; i < pgTypes.size(); i++) {
            builder.add(new Parameter(pgTypes.get(i), params.get(i) == null ? "" : params.get(i)));
        }
        return builder.build();
    }

    // TODO: make sure this annotation works.
    @PreDestroy
    protected void close()
    {
        if (connectorRecordIterable != null) {
            LOG.info("ConnectorRecordIterable is closing.");
            try {
                connectorRecordIterable.close();
            }
            catch (IOException ex) {
                LOG.error(ex, "ConnectorRecordIterable close failed");
            }
            LOG.info("ConnectorRecordIterable is closed.");
        }
    }
}
