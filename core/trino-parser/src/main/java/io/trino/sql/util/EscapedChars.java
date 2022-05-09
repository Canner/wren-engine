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

package io.trino.sql.util;

public class EscapedChars
{
    private final String value;
    private final StringBuilder builder;
    private final int length;
    private int index;

    public static EscapedChars of(String value)
    {
        return new EscapedChars(value);
    }

    private EscapedChars(String value)
    {
        this.value = value;
        this.length = value.length();
        this.builder = new StringBuilder(value.length());
    }

    public boolean hasNext()
    {
        return index < length;
    }

    public char getCurrent()
    {
        return value.charAt(index);
    }

    public char getNext()
    {
        return value.charAt(index + 1);
    }

    public StringBuilder getBuilder()
    {
        return builder;
    }

    public String getValue()
    {
        return value;
    }

    public int getLength()
    {
        return length;
    }

    public int getIndex()
    {
        return index;
    }

    public void setIndex(int index)
    {
        this.index = index;
    }

    public void incrementIndex()
    {
        index++;
    }

    @Override
    public String toString()
    {
        return getBuilder().toString();
    }

    public void appendAndIncrement(char c)
    {
        builder.append(c);
        this.incrementIndex();
    }
}
