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
import io.airlift.log.Logger;
import io.cml.wireprotocol.postgres.FormatCodes;

import javax.annotation.Nullable;
import javax.annotation.PreDestroy;
import javax.validation.constraints.NotNull;

import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

public class Portal
{
    private static final Logger LOG = Logger.get(Portal.class);

    private final PreparedStatement preparedStatement;
    private final List<Object> params;
    private GenericTableRecordIterable genericTableRecordIterable;
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

    public GenericTableRecordIterable getGenericTableRecordIterable()
    {
        return genericTableRecordIterable;
    }

    public void setResultSetSender(GenericTableRecordIterable genericTableRecordIterable)
    {
        this.genericTableRecordIterable = genericTableRecordIterable;
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
        return genericTableRecordIterable != null;
    }

    // TODO: make sure this annotation works.
    @PreDestroy
    protected void close()
    {
        if (genericTableRecordIterable != null) {
            LOG.info("RecordIterable is closing.");
            genericTableRecordIterable.close();
            LOG.info("RecordIterable is closed.");
        }
    }
}
