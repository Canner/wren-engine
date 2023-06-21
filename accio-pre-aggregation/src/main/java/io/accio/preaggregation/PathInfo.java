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
package io.accio.preaggregation;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class PathInfo
{
    private final String path;
    private final String filePattern;

    public static PathInfo of(String path, String filePattern)
    {
        return new PathInfo(path, filePattern);
    }

    private PathInfo(String path, String filePattern)
    {
        this.path = requireNonNull(path, "path is null");
        this.filePattern = requireNonNull(filePattern, "filePattern is null");
    }

    public String getPath()
    {
        return path;
    }

    public String getFilePattern()
    {
        return filePattern;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PathInfo pathInfo = (PathInfo) o;
        return Objects.equals(path, pathInfo.path) && Objects.equals(filePattern, pathInfo.filePattern);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path, filePattern);
    }
}
