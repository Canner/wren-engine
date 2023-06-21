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

package io.graphmdl.main.pgcatalog.regtype;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.graphmdl.base.ConnectorRecordIterator;
import io.graphmdl.base.GraphMDLException;
import io.graphmdl.main.metadata.Metadata;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.graphmdl.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class PostgresPgMetadata
        extends PgMetadata
{
    private final Metadata metadata;

    @Inject
    public PostgresPgMetadata(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    protected List<RegObject> listRegProc()
    {
        try (ConnectorRecordIterator iterator = metadata.directQuery("select oid, proname from pg_proc", ImmutableList.of())) {
            return Streams.stream(iterator)
                    .map(row -> new RegProc((long) row[0], (String) row[1]))
                    .collect(toImmutableList());
        }
        catch (Exception e) {
            throw new GraphMDLException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    @Override
    protected List<RegObject> listRegClass()
    {
        try (ConnectorRecordIterator iterator = metadata.directQuery("select oid, relname from pg_class", ImmutableList.of())) {
            return Streams.stream(iterator)
                    .map(row -> new RegObjectImpl((long) row[0], (String) row[1]))
                    .collect(toImmutableList());
        }
        catch (Exception e) {
            throw new GraphMDLException(GENERIC_INTERNAL_ERROR, e);
        }
    }
}
