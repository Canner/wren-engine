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

package io.wren.base.client.duckdb;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static java.nio.file.StandardOpenOption.APPEND;

public class FileUtil
{
    public static final String ARCHIVED = "archived";

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
            if (!Files.exists(path)) {
                createFile(path, content);
            }
            else {
                Files.writeString(path, System.lineSeparator() + content, APPEND);
            }
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
            Path archiveDir = path.getParent().resolve(ARCHIVED);
            if (!Files.exists(archiveDir)) {
                createDir(archiveDir);
            }
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssnnnn"));
            Files.move(path, archiveDir.resolve(path.getFileName() + "." + timestamp));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
