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

package io.cml.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cml.spi.CmlException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.cml.spi.metadata.StandardErrorCode.ALREADY_EXISTS;
import static io.cml.spi.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.cml.spi.metadata.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FileMetricStore
        implements MetricStore
{
    private final Path rootPath;
    private final ObjectMapper objectMapper;

    public FileMetricStore(Path rootPath)
    {
        this.rootPath = requireNonNull(rootPath);
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public List<Metric> listMetrics()
    {
        if (!Files.exists(rootPath)) {
            throw new CmlException(NOT_FOUND, format("rootPath is not found. path: %s", rootPath));
        }

        try (Stream<Path> stream = Files.list(rootPath)) {
            return stream.filter(path -> path.getFileName().toFile().isDirectory())
                    .map(path -> path.toFile().getName())
                    .map(name -> {
                        String path = format("%s/%s.json", rootPath, name);
                        try {
                            return objectMapper.readValue(Paths.get(path).toFile(), Metric.class);
                        }
                        catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toList());
        }
        catch (Exception e) {
            throw new CmlException(GENERIC_INTERNAL_ERROR, e.getMessage(), e);
        }
    }

    @Override
    public Optional<Metric> getMetric(String metricName)
    {
        Path path = rootPath.resolve(metricName).resolve(withJsonFileExtension(metricName));
        if (!Files.exists(path)) {
            return Optional.empty();
        }

        try {
            return Optional.of(objectMapper.readValue(path.toFile(), Metric.class));
        }
        catch (Exception e) {
            throw new CmlException(GENERIC_INTERNAL_ERROR, format("metric %s can't be accessed", metricName), e);
        }
    }

    @Override
    public void createMetric(Metric metric)
    {
        Path dir = rootPath.resolve(metric.getName());
        if (Files.exists(dir)) {
            throw new CmlException(ALREADY_EXISTS, format("metric %s already exists", metric.getName()));
        }

        try {
            Files.createDirectory(dir);
            Path path = dir.resolve(withJsonFileExtension(metric.getName()));
            objectMapper.writeValue(path.toFile(), metric);
        }
        catch (Exception e) {
            throw new CmlException(GENERIC_INTERNAL_ERROR, format("metric %s can't be created", metric.getName()), e);
        }
    }

    @Override
    public void dropMetric(String metricName)
    {
        Path path = rootPath.resolve(metricName);
        if (!Files.exists(path)) {
            throw new CmlException(NOT_FOUND, format("metric %s not found", metricName));
        }

        try (Stream<Path> files = Files.list(path)) {
            for (Path file : files.collect(Collectors.toList())) {
                Files.delete(file);
            }
            Files.delete(path);
        }
        catch (Exception e) {
            throw new CmlException(GENERIC_INTERNAL_ERROR, format("metric %s can't be dropped", metricName), e);
        }
    }

    @Override
    public void createMetricSql(MetricSql metricSql)
    {
        Path path = rootPath.resolve(metricSql.getBaseMetricName()).resolve(withJsonFileExtension(metricSql.getName()));
        if (Files.exists(path)) {
            throw new CmlException(ALREADY_EXISTS, format("metricSql %s already exists", metricSql.getName()));
        }

        try {
            objectMapper.writeValue(path.toFile(), metricSql);
        }
        catch (Exception e) {
            throw new CmlException(GENERIC_INTERNAL_ERROR, format("metricSql %s can't be created", metricSql.getName()), e);
        }
    }

    @Override
    public List<MetricSql> listMetricSqls(String metricName)
    {
        Path filePath = rootPath.resolve(metricName);
        if (!Files.exists(filePath)) {
            throw new CmlException(NOT_FOUND, format("metric %s not found", metricName));
        }

        try (Stream<Path> stream = Files.list(filePath)) {
            return stream
                    .filter(path -> !path.toFile().getName().equals(withJsonFileExtension(metricName)))
                    .map(path -> {
                        try {
                            return objectMapper.readValue(path.toFile(), MetricSql.class);
                        }
                        catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toList());
        }
        catch (Exception e) {
            throw new CmlException(GENERIC_INTERNAL_ERROR, e.getMessage(), e);
        }
    }

    @Override
    public Optional<MetricSql> getMetricSql(String metricName, String metricSqlName)
    {
        Path filePath = rootPath.resolve(metricName).resolve(withJsonFileExtension(metricSqlName));
        if (!Files.exists(filePath)) {
            return Optional.empty();
        }

        try {
            return Optional.of(objectMapper.readValue(filePath.toFile(), MetricSql.class));
        }
        catch (Exception e) {
            throw new CmlException(GENERIC_INTERNAL_ERROR, format("metric sql %s can't be accessed", metricSqlName), e);
        }
    }

    private static String withJsonFileExtension(String fileName)
    {
        return fileName + ".json";
    }
}
