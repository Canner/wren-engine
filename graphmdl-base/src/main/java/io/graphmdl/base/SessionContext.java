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

package io.graphmdl.base;

public final class SessionContext
{
    public static Builder builder()
    {
        return new Builder();
    }

    private final String catalog;
    private final String schema;

    private SessionContext(String catalog, String schema)
    {
        this.catalog = catalog;
        this.schema = schema;
    }

    public String getCatalog()
    {
        return catalog;
    }

    public String getSchema()
    {
        return schema;
    }

    public static class Builder
    {
        private String catalog;
        private String schema;

        public Builder setCatalog(String catalog)
        {
            this.catalog = catalog;
            return this;
        }

        public Builder setSchema(String schema)
        {
            this.schema = schema;
            return this;
        }

        public SessionContext build()
        {
            return new SessionContext(catalog, schema);
        }
    }
}
