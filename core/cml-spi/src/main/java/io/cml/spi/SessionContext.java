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

package io.cml.spi;

public final class SessionContext
{
    public static Builder builder()
    {
        return new Builder();
    }

    private final String defaultCatalog;
    private final String defaultSchema;

    private SessionContext(String defaultCatalog, String defaultSchema)
    {
        this.defaultCatalog = defaultCatalog;
        this.defaultSchema = defaultSchema;
    }

    public String getDefaultCatalog()
    {
        return defaultCatalog;
    }

    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    public static class Builder
    {
        private String defaultCatalog;
        private String defaultSchema;

        public Builder setDefaultCatalog(String defaultCatalog)
        {
            this.defaultCatalog = defaultCatalog;
            return this;
        }

        public Builder setDefaultSchema(String defaultSchema)
        {
            this.defaultSchema = defaultSchema;
            return this;
        }

        public SessionContext build()
        {
            return new SessionContext(defaultCatalog, defaultSchema);
        }
    }
}
