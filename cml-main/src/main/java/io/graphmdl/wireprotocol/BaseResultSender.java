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

package io.graphmdl.wireprotocol;

import javax.annotation.Nonnull;
import javax.annotation.OverridingMethodsMustInvokeSuper;

import java.util.concurrent.CompletableFuture;

public abstract class BaseResultSender
{
    private CompletableFuture<Void> completionFuture = new CompletableFuture<>();

    public abstract void sendRow(Object[] row);

    public abstract void batchFinished();

    @OverridingMethodsMustInvokeSuper
    public void allFinished(boolean interrupted)
    {
        if (interrupted) {
            completionFuture.completeExceptionally(new ClientInterrupted());
        }
        else {
            completionFuture.complete(null);
        }
    }

    @OverridingMethodsMustInvokeSuper
    public void fail(@Nonnull Throwable t)
    {
        completionFuture.completeExceptionally(t);
    }

    public CompletableFuture<Void> completionFuture()
    {
        return completionFuture;
    }
}
