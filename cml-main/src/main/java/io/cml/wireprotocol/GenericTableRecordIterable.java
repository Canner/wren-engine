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

package io.cml.wireprotocol;

import com.google.common.collect.ImmutableList;
import io.cml.spi.type.PGType;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class GenericTableRecordIterable
        implements Iterable<List<Optional<Object>>>, Closeable
{
    protected final List<PGType> types;

    public GenericTableRecordIterable(List<PGType> types)
    {
        this.types = types;
    }

    @Override
    public void close()
    {
    }

    @Override
    public Iterator<List<Optional<Object>>> iterator()
    {
        return ImmutableList.<List<Optional<Object>>>of().iterator();
    }

    public List<PGType> getTypes()
    {
        return types;
    }
}
