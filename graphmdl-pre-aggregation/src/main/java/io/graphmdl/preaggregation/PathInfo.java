package io.graphmdl.preaggregation;

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
        this.filePattern = filePattern;
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
