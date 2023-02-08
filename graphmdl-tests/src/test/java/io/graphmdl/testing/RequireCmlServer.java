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

package io.graphmdl.testing;

import com.google.common.io.Closer;
import org.testng.annotations.AfterClass;

import java.io.IOException;

public abstract class RequireCmlServer
{
    private final TestingCmlServer cmlServer;
    protected final Closer closer = Closer.create();

    public RequireCmlServer()
    {
        this.cmlServer = createCmlServer();
        closer.register(cmlServer);
    }

    protected abstract TestingCmlServer createCmlServer();

    protected TestingCmlServer server()
    {
        return cmlServer;
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws IOException
    {
        closer.close();
    }
}
