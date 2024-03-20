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

import io.wren.base.ConnectorRecordIterator;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class Query
{
    public static Builder builder(PreparedStatement preparedStatement)
    {
        return new Builder(preparedStatement);
    }

    private final PreparedStatement preparedStatement;
    private Portal portal;
    private ConnectorRecordIterator connectorRecordIterator;

    public Query(PreparedStatement preparedStatement)
    {
        this.preparedStatement = requireNonNull(preparedStatement, "preparedStatement is null");
    }

    public PreparedStatement getPreparedStatement()
    {
        return preparedStatement;
    }

    public Optional<Portal> getPortal()
    {
        return Optional.ofNullable(portal);
    }

    public void setPortal(Portal portal)
    {
        this.portal = portal;
    }

    public Optional<ConnectorRecordIterator> getConnectorRecordIterator()
    {
        return Optional.ofNullable(connectorRecordIterator);
    }

    public void setConnectorRecordIterator(ConnectorRecordIterator connectorRecordIterator)
    {
        this.connectorRecordIterator = connectorRecordIterator;
    }

    public static class Builder
    {
        private final PreparedStatement preparedStatement;
        private Portal portal;
        private ConnectorRecordIterator connectorRecordIterator;

        public Builder(PreparedStatement preparedStatement)
        {
            this.preparedStatement = requireNonNull(preparedStatement, "preparedStatement is null");
        }

        public Builder setPortal(Portal portal)
        {
            this.portal = portal;
            return this;
        }

        public Builder setConnectorRecordIterator(ConnectorRecordIterator connectorRecordIterator)
        {
            this.connectorRecordIterator = connectorRecordIterator;
            return this;
        }

        public Query build()
        {
            Query query = new Query(preparedStatement);
            query.setPortal(portal);
            query.setConnectorRecordIterator(connectorRecordIterator);
            return query;
        }
    }
}
