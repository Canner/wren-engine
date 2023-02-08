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

package io.cml.graphml.analyzer;

import io.cml.graphml.base.GraphML;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import org.testng.annotations.Test;

import java.util.List;

import static io.cml.graphml.analyzer.StatementAnalyzer.analyze;
import static io.cml.graphml.base.dto.Manifest.manifest;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;

public class TestStatementAnalyzer
{
    public static final GraphML EMPTY_GRAPHML =
            GraphML.fromManifest(manifest(List.of(), List.of(), List.of(), List.of()));
    public static final SqlParser sqlParser = new SqlParser();

    @Test
    public void testValues()
    {
        analyze(sqlParser.createStatement("VALUES(1, 'a')", new ParsingOptions(AS_DECIMAL)), EMPTY_GRAPHML);
        analyze(sqlParser.createStatement("SELECT * FROM (VALUES(1, 'a'))", new ParsingOptions(AS_DECIMAL)), EMPTY_GRAPHML);
    }
}
