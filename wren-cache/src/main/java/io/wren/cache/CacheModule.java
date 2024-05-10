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

package io.wren.cache;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;

public class CacheModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(CacheManager.class).to(CacheManagerImpl.class).in(Scopes.SINGLETON);
        binder.bind(CacheTaskManager.class).in(Scopes.SINGLETON);
        binder.bind(EventLogger.class).to(Log4jEventLogger.class).in(Scopes.SINGLETON);
        binder.bind(CachedTableMapping.class).to(DefaultCachedTableMapping.class).in(Scopes.SINGLETON);
    }
}
