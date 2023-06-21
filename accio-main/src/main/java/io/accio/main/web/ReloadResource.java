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

package io.accio.main.web;

import io.accio.main.AccioManager;

import javax.inject.Inject;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

@Path("/v1/reload")
public class ReloadResource
{
    private final AccioManager accioManager;

    @Inject
    public ReloadResource(AccioManager accioManager)
    {
        this.accioManager = requireNonNull(accioManager, "accioManager is null");
    }

    @PUT
    public void reload(@Suspended AsyncResponse asyncResponse)
            throws IOException
    {
        accioManager.loadAccioMDLFromFile();
        asyncResponse.resume(Response.ok().build());
    }
}
