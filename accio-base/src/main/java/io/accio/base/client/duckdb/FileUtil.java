package io.accio.base.client.duckdb;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static java.nio.file.StandardOpenOption.APPEND;

public class FileUtil
{
    private FileUtil() {}

    public static void createDir(Path path)
    {
        try {
            Files.createDirectories(path);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void createFile(Path path, String content)
    {
        try {
            Path dir = path.getParent();
            if (!Files.exists(dir)) {
                createDir(dir);
            }
            Path actualPath = Files.createFile(path);
            Files.writeString(actualPath, content);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void appendToFile(Path path, String content)
    {
        try {
            Files.writeString(path, System.lineSeparator() + content, APPEND);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void archiveFile(Path path)
    {
        try {
            if (!Files.exists(path)) {
                return;
            }
            Path archiveDir = path.getParent().resolve("archive");
            if (!Files.exists(archiveDir)) {
                createDir(archiveDir);
            }
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            Files.move(path, archiveDir.resolve(path.getFileName() + "." + timestamp));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
