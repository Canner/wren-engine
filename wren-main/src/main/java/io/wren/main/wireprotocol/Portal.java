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

package io.wren.main.wireprotocol;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.wren.base.ConnectorRecordIterator;
import io.wren.base.Parameter;
import io.wren.base.type.PGType;
import io.wren.base.type.PGTypes;
import jakarta.validation.constraints.NotNull;

import javax.annotation.Nullable;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

public class Portal
{
    private static final Logger LOG = Logger.get(Portal.class);

    private final String name;
    private final PreparedStatement preparedStatement;
    private final List<Object> params;
    private ConnectorRecordIterator connectorRecordIterator;
    private long rowCount;

    @Nullable
    private final FormatCodes.FormatCode[] resultFormatCodes;

    public Portal(String name, PreparedStatement preparedStatement, List<Object> params, @Nullable FormatCodes.FormatCode[] resultFormatCodes)
    {
        this.name = name;
        this.preparedStatement = preparedStatement;
        this.params = params;
        this.resultFormatCodes = resultFormatCodes;
    }

    public String getName()
    {
        return name;
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

    public ConnectorRecordIterator getConnectorRecordIterator()
    {
        return connectorRecordIterator;
    }

    public void setConnectorRecordIterator(ConnectorRecordIterator connectorRecordIterator)
    {
        this.connectorRecordIterator = connectorRecordIterator;
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
        return connectorRecordIterator != null;
    }

    public boolean isMetadataQuery()
    {
        return preparedStatement.isMetaDtaQuery();
    }

    public List<Parameter> getParameters()
    {
        List<PGType<?>> pgTypes = preparedStatement.getParamTypeOids().stream().map(PGTypes::oidToPgType).collect(Collectors.toList());
        ImmutableList.Builder<Parameter> builder = ImmutableList.builder();
        for (int i = 0; i < pgTypes.size(); i++) {
            builder.add(new Parameter(pgTypes.get(i), params.get(i).equals("null") ? pgTypes.get(i).getEmptyValue() : params.get(i)));
        }
        return builder.build();
    }

    public List<Object> getParametersValue()
    {
        return params;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("preparedStatement", preparedStatement)
                .add("params", params)
                .add("resultFormatCodes", resultFormatCodes)
                .add("connectorRecordIterator", connectorRecordIterator)
                .add("rowCount", rowCount)
                .toString();
    }

    protected void close()
    {
        if (connectorRecordIterator != null) {
            LOG.info("ConnectorRecordIterable is closing.");
            try {
                connectorRecordIterator.close();
            }
            catch (Exception ex) {
                LOG.error(ex, "ConnectorRecordIterable close failed");
            }
            LOG.info("ConnectorRecordIterable is closed.");
        }
    }
}
