/*
 * Copyright (C) Canner, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Canner dev team contact@canner.io, Nov 2021
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
