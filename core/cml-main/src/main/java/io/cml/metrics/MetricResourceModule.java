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

package io.cml.metrics;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.cml.web.CompletableFutureAsyncResponseHandler;
import io.cml.web.MetricResource;

import java.nio.file.Path;

import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class MetricResourceModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        MetricConfig metricConfig = buildConfigObject(MetricConfig.class);
        binder.bind(MetricStore.class)
                .toInstance(new FileMetricStore(Path.of(metricConfig.getRootPath())));
        binder.bind(MetricHook.class).in(Scopes.SINGLETON);

        jaxrsBinder(binder).bind(MetricResource.class);
        jaxrsBinder(binder).bindInstance(new CompletableFutureAsyncResponseHandler());
    }
}
