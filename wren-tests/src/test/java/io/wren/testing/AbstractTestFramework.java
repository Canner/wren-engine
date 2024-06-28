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

package io.wren.testing;

import io.wren.base.SessionContext;
import io.wren.base.dto.Manifest;
import io.wren.main.web.dto.PreviewDto;
import io.wren.main.web.dto.QueryResultDto;
import org.intellij.lang.annotations.Language;

public abstract class AbstractTestFramework
        extends RequireWrenServer
{
    public static final SessionContext DEFAULT_SESSION_CONTEXT =
            SessionContext.builder().setCatalog("wren").setSchema("test").build();

    public static Manifest.Builder withDefaultCatalogSchema()
    {
        return Manifest.builder()
                .setCatalog(DEFAULT_SESSION_CONTEXT.getCatalog().orElseThrow())
                .setSchema(DEFAULT_SESSION_CONTEXT.getSchema().orElseThrow());
    }

    @Override
    protected void prepare()
    {
        initDuckDB();
    }

    protected QueryResultDto query(Manifest manifest, @Language("SQL") String sql)
    {
        PreviewDto previewDto = new PreviewDto(manifest, sql, 100L);
        return preview(previewDto);
    }
}
