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

package io.graphmdl.connector.bigquery;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageBatchResult;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class GcsStorageClient
{
    private final Storage storage;

    @Inject
    public GcsStorageClient(Storage storage)
    {
        this.storage = requireNonNull(storage, "storage is null");
    }

    public boolean cleanFolders(String bucket, String prefix)
    {
        ImmutableList.Builder<StorageBatchResult<Boolean>> builder = ImmutableList.builder();
        StorageBatch storageBatch = storage.batch();
        storage.list(bucket, Storage.BlobListOption.prefix(prefix))
                .iterateAll()
                .forEach(blob -> storageBatch.delete(blob.getBlobId()));
        List<StorageBatchResult<Boolean>> results = builder.build();
        if (results.size() > 0) {
            storageBatch.submit();
        }
        return results.stream().allMatch(result -> result != null && result.get());
    }
}
