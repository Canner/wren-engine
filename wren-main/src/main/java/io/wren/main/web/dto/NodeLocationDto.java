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

package io.wren.main.web.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class NodeLocationDto
{
    public static NodeLocationDto nodeLocationDto(int line, int column)
    {
        return new NodeLocationDto(line, column);
    }

    private final int line;
    private final int column;

    @JsonCreator
    public NodeLocationDto(
            @JsonProperty int line,
            @JsonProperty int column)
    {
        this.line = line;
        this.column = column;
    }

    @JsonProperty
    public int getLine()
    {
        return line;
    }

    @JsonProperty
    public int getColumn()
    {
        return column;
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
        NodeLocationDto that = (NodeLocationDto) o;
        return line == that.line && column == that.column;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(line, column);
    }

    @Override
    public String toString()
    {
        return "NodeLocationDto{" +
                "line=" + line +
                ", column=" + column +
                '}';
    }
}
