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

import io.airlift.log.Logger;

public class Log4jEventLogger
        implements EventLogger
{
    private static final Logger LOGGER = Logger.get("CacheEventLogger");

    @Override
    public void logEvent(Level level, String eventName, TaskInfo event)
    {
        logEvent(level, eventName, event.toString());
    }

    @Override
    public void logEvent(Level level, String eventName, String description)
    {
        switch (level) {
            case WARN:
                LOGGER.warn("Event Type: %s, Event: %s", eventName, description);
                break;
            case ERROR:
                LOGGER.error("Event Type: %s, Event: %s", eventName, description);
                break;
            default:
                LOGGER.info("Event Type: %s, Event: %s", eventName, description);
        }
    }
}
