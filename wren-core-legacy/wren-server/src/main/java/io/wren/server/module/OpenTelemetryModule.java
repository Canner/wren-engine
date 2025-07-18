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
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;

// From airlift 305, the tracing is enabled by default. We need to bind a Opentelemetry instance explicitly
public class OpenTelemetryModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        // Create a no-op OpenTelemetry instance for environments where tracing is not needed
        OpenTelemetry openTelemetry = OpenTelemetry.noop();
        binder.bind(OpenTelemetry.class).toInstance(openTelemetry);
        binder.bind(Tracer.class).toInstance(openTelemetry.getTracer("wren-server"));
    }
}
