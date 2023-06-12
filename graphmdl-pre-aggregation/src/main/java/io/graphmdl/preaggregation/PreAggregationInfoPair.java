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
package io.graphmdl.preaggregation;

import io.graphmdl.base.GraphMDLException;
import io.graphmdl.base.dto.PreAggregationInfo;

import java.util.Optional;

import static io.graphmdl.base.metadata.StandardErrorCode.GENERIC_USER_ERROR;
import static java.util.Objects.requireNonNull;

public class PreAggregationInfoPair
{
    private final PreAggregationInfo preAggregationInfo;
    private final Optional<String> tableName;
    private final Optional<String> errorMessage;
    private final long createTime;

    protected PreAggregationInfoPair(PreAggregationInfo preAggregationInfo, String tableName, long createTime)
    {
        this(preAggregationInfo, Optional.of(tableName), Optional.empty(), createTime);
    }

    protected PreAggregationInfoPair(PreAggregationInfo preAggregationInfo, Optional<String> tableName, Optional<String> errorMessage, long createTime)
    {
        this.preAggregationInfo = requireNonNull(preAggregationInfo, "preAggregationInfo is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.errorMessage = requireNonNull(errorMessage, "errorMessage is null");
        this.createTime = createTime;
    }

    public PreAggregationInfo getPreAggregationInfo()
    {
        return preAggregationInfo;
    }

    public String getRequiredTableName()
    {
        return tableName.orElseThrow(() -> new GraphMDLException(GENERIC_USER_ERROR, "Mapping table name is refreshing or not exists"));
    }

    public Optional<String> getTableName()
    {
        return tableName;
    }

    public Optional<String> getErrorMessage()
    {
        return errorMessage;
    }

    public long getCreateTime()
    {
        return createTime;
    }
}
