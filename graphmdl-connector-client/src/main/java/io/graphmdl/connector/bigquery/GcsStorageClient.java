package io.graphmdl.connector.bigquery;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
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
        Page<Blob> blobs = storage.list(bucket, Storage.BlobListOption.prefix(prefix));
        for (Blob blob : blobs.iterateAll()) {
            builder.add(storageBatch.delete(blob.getBlobId()));
        }
        List<StorageBatchResult<Boolean>> results = builder.build();
        if (results.size() > 0) {
            storageBatch.submit();
        }
        return results.stream().allMatch(result -> result != null && result.get());
    }
}
