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

package io.accio.base.dto.macro;

import io.accio.base.AccioMDL;
import io.accio.base.dto.Macro;
import io.accio.base.dto.Manifest;
import io.accio.base.dto.Model;
import io.accio.base.macro.ParsingException;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.accio.base.AccioTypes.INTEGER;
import static io.accio.base.AccioTypes.VARCHAR;
import static io.accio.base.dto.Column.column;
import static io.accio.base.dto.Macro.macro;
import static io.accio.base.dto.Model.model;
import static io.accio.base.macro.Parameter.expressionType;
import static io.accio.base.macro.Parameter.macroType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMacro
{
    @Test
    public void testParseParameter()
    {
        Macro singleParameter = new Macro("test", "(a: Expression) => a + 1");
        assertThat(singleParameter.getParameters()).isEqualTo(List.of(expressionType("a")));

        Macro multipleParameters = new Macro("test", "(a: Expression, b: Macro) => a + b");
        assertThat(multipleParameters.getParameters()).isEqualTo(List.of(expressionType("a"), macroType("b")));

        Macro noParameter = new Macro("test", "() => 1");
        assertThat(noParameter.getParameters()).isEqualTo(List.of());
    }

    @Test
    public void testErrorHandle()
    {
        assertThatThrownBy(() -> new Macro("test", "xxxxx"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("definition is invalid");

        assertThatThrownBy(() -> new Macro("test", "(xxxxx) => a + b"))
                .isInstanceOf(ParsingException.class)
                .hasMessageContaining("typeName is null");

        assertThatThrownBy(() -> new Macro("test", "(a: Expression, xxxxx) => a + b"))
                .isInstanceOf(ParsingException.class)
                .hasMessageContaining("typeName is null");

        assertThatThrownBy(() -> new Macro("test", "(a: Expression, b: UnDefined) => a + b"))
                .isInstanceOf(ParsingException.class)
                .hasMessageContaining("typeName is invalid: b:UnDefined");
    }

    @Test
    public void testOneParameterCall()
    {
        Manifest manifest = Manifest.builder()
                .setCatalog("test")
                .setSchema("test")
                .setModels(List.of(
                        model("Customer",
                                "select * from main.customer",
                                List.of(
                                        column("custkey", INTEGER, null, true),
                                        column("normal_call", INTEGER, null, true, "addOne(custkey)"),
                                        column("custkey_addOne", INTEGER, null, true, "{{addOne('custkey')}}"),
                                        column("custkey_callAddOne", INTEGER, null, true, "{{callAddOne('custkey')}}"),
                                        column("custkey_pass1Macro", INTEGER, null, true, "{{pass1Macro('custkey', addOne)}}"),
                                        column("custkey_pass2Macro", INTEGER, null, true, "{{pass2Macro('custkey', addOne, addTwo)}}"),
                                        column("name", VARCHAR, null, true)),
                                "pk")))
                .setMacros(List.of(
                        macro("addOne", "(text: Expression) => {{ text }} + 1"),
                        macro("addTwo", "(text: Expression) => {{ text }} + 2"),
                        macro("callAddOne", "(text: Expression) => {{addOne(text)}}"),
                        macro("pass1Macro", "(text: Expression, rule: Macro) => {{rule(text)}}"),
                        macro("pass2Macro", "(text: Expression, rule1: Macro, rule2: Macro) => {{ rule1(text) }} + {{ rule2(text)}}")))
                .build();

        AccioMDL mdl = AccioMDL.fromManifest(manifest);
        Optional<Model> modelOptional = mdl.getModel("Customer");
        assertThat(modelOptional).isPresent();
        assertThat(modelOptional.get().getColumns().get(1).getExpression().get()).isEqualTo("addOne(custkey)");
        assertThat(modelOptional.get().getColumns().get(2).getExpression().get()).isEqualTo("custkey + 1");
        assertThat(modelOptional.get().getColumns().get(3).getExpression().get()).isEqualTo("custkey + 1");
        assertThat(modelOptional.get().getColumns().get(4).getExpression().get()).isEqualTo("custkey + 1");
        assertThat(modelOptional.get().getColumns().get(5).getExpression().get()).isEqualTo("custkey + 1 + custkey + 2");
    }

    @Test
    public void testTwoParameterCall()
    {
        Manifest manifest = Manifest.builder()
                .setCatalog("test")
                .setSchema("test")
                .setModels(List.of(
                        model("Customer",
                                "select * from main.customer",
                                List.of(
                                        column("custkey", INTEGER, null, true),
                                        column("name", VARCHAR, null, true),
                                        column("custkey_concat_name", INTEGER, null, true, "{{concat('custkey', 'name')}}"),
                                        column("custkey_callAddOne", INTEGER, null, true, "{{addPrefixOne('custkey')}}"),
                                        column("custkey_pass1Macro", INTEGER, null, true, "{{pass1Macro('custkey', 'name', concat)}}")),
                                "pk")))
                .setMacros(List.of(
                        macro("concat", "(text: Expression, text2: Expression) => {{ text }} || {{ text2 }}"),
                        macro("addPrefixOne", "(text: Expression) => {{concat(\"'1'\", text)}}"),
                        macro("pass1Macro", "(text: Expression, text2: Expression, cf: Macro) => {{cf(text, text2)}}")))
                .build();

        AccioMDL mdl = AccioMDL.fromManifest(manifest);
        Optional<Model> modelOptional = mdl.getModel("Customer");
        assertThat(modelOptional).isPresent();
        assertThat(modelOptional.get().getColumns().get(2).getExpression().get()).isEqualTo("custkey || name");
        assertThat(modelOptional.get().getColumns().get(3).getExpression().get()).isEqualTo("'1' || custkey");
        assertThat(modelOptional.get().getColumns().get(4).getExpression().get()).isEqualTo("custkey || name");
    }

    @Test
    public void testZeroPArameterCall()
    {
        Manifest manifest = Manifest.builder()
                .setCatalog("test")
                .setSchema("test")
                .setModels(List.of(
                        model("Customer",
                                "select * from main.customer",
                                List.of(
                                        column("custkey", INTEGER, null, true),
                                        column("name", VARCHAR, null, true),
                                        column("standardTime", INTEGER, null, true, "{{standardTime()}}"),
                                        column("callStandardTime", INTEGER, null, true, "{{callStandardTime()}}"),
                                        column("passStandardTime", INTEGER, null, true, "{{passStandardTime(standardTime)}}")),
                                "pk")))
                .setMacros(List.of(
                        macro("standardTime", "() => standardTime"),
                        macro("callStandardTime", "() => {{callStandardTime()}}"),
                        macro("passStandardTime", "(cf: Macro) => {{cf()}}")))
                .build();

        AccioMDL mdl = AccioMDL.fromManifest(manifest);
        Optional<Model> modelOptional = mdl.getModel("Customer");
        assertThat(modelOptional).isPresent();
        assertThat(modelOptional.get().getColumns().get(2).getExpression().get()).isEqualTo("standardTime");
        assertThat(modelOptional.get().getColumns().get(2).getExpression().get()).isEqualTo("standardTime");
        assertThat(modelOptional.get().getColumns().get(2).getExpression().get()).isEqualTo("standardTime");
    }
}
