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

package io.wren.server.module;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.wren.main.PreviewService;
import io.wren.main.ValidationService;
import io.wren.main.web.AnalysisResource;
import io.wren.main.web.AnalysisResourceV2;
import io.wren.main.web.ConfigResource;
import io.wren.main.web.DuckDBResource;
import io.wren.main.web.MDLResource;
import io.wren.main.web.MDLResourceV2;
import io.wren.main.web.SystemResource;
import io.wren.main.web.WrenExceptionMapper;

import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class WebModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        jaxrsBinder(binder).bind(MDLResource.class);
        jaxrsBinder(binder).bind(MDLResourceV2.class);
        jaxrsBinder(binder).bind(AnalysisResource.class);
        jaxrsBinder(binder).bind(AnalysisResourceV2.class);
        jaxrsBinder(binder).bind(ConfigResource.class);
        jaxrsBinder(binder).bind(DuckDBResource.class);
        jaxrsBinder(binder).bind(SystemResource.class);
        jaxrsBinder(binder).bindInstance(new WrenExceptionMapper());
        binder.bind(PreviewService.class).in(Scopes.SINGLETON);
        binder.bind(ValidationService.class).in(Scopes.SINGLETON);
    }
}
