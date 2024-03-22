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

package io.wren.sql.glot;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class SQLGlot
{
    public enum Dialect
    {
        TRINO("trino"),
        BIGQUERY("bigquery"),
        POSTGRES("postgres"),
        DUCKDB("duckdb");

        private final String dialect;

        Dialect(String dialect)
        {
            this.dialect = dialect;
        }

        public String getDialect()
        {
            return dialect;
        }
    }

    public static String transpile(String sql, Dialect read, Dialect write)
            throws IOException
    {
        return executePython("transpile.py", sql, read, write);
    }

    private static String executePython(String pythonFile, String sql, Dialect read, Dialect write)
            throws IOException
    {
        ProcessBuilder processBuilder = new ProcessBuilder(
                "python",
                resolvePath("sqlglot/" + pythonFile),
                sql,
                read.getDialect(),
                write.getDialect());
        processBuilder.redirectErrorStream(true);

        Process process = processBuilder.start();

        int exitCode = 0;
        try {
            exitCode = process.waitFor();
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (exitCode != 0) {
            List<String> results = readProcessOutput(process.getInputStream());
            throw new RuntimeException("Failed to execute Python script: " + String.join(System.lineSeparator(), results));
        }

        List<String> results = readProcessOutput(process.getInputStream());
        return results.get(0);
    }

    private static String resolvePath(String path)
    {
        try {
            URL resource = SQLGlot.class.getClassLoader().getResource(path);
            return Paths.get(resource.toURI()).toFile().getAbsolutePath();
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<String> readProcessOutput(InputStream inputStream) {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
            return br.lines().collect(toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
