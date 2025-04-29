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

package io.wren.base.dto;

import io.wren.base.WrenMDL;
import io.wren.base.WrenTypes;
import io.wren.base.macro.ParsingException;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.wren.base.macro.Parameter.expressionType;
import static io.wren.base.macro.Parameter.macroType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMacro
{
    @Test
    public void testParseParameter()
    {
        Macro singleParameter = Macro.macro("test", "(a: Expression) => a + 1");
        assertThat(singleParameter.getParameters()).isEqualTo(List.of(expressionType("a")));

        Macro multipleParameters = Macro.macro("test", "(a: Expression, b: Macro) => a + b");
        assertThat(multipleParameters.getParameters()).isEqualTo(List.of(expressionType("a"), macroType("b")));

        Macro noParameter = Macro.macro("test", "() => 1");
        assertThat(noParameter.getParameters()).isEqualTo(List.of());
    }

    @Test
    public void testErrorHandle()
    {
        assertThatThrownBy(() -> Macro.macro("test", "xxxxx"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("definition is invalid");

        assertThatThrownBy(() -> Macro.macro("test", "(xxxxx) => a + b"))
                .isInstanceOf(ParsingException.class)
                .hasMessageContaining("typeName is null");

        assertThatThrownBy(() -> Macro.macro("test", "(a: Expression, xxxxx) => a + b"))
                .isInstanceOf(ParsingException.class)
                .hasMessageContaining("typeName is null");

        assertThatThrownBy(() -> Macro.macro("test", "(a: Expression, b: UnDefined) => a + b"))
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
                        Model.model("Customer",
                                "select * from main.customer",
                                List.of(
                                        Column.column("custkey", WrenTypes.INTEGER, null, true),
                                        Column.column("normal_call", WrenTypes.INTEGER, null, true, "addOne(custkey)"),
                                        Column.column("custkey_addOne", WrenTypes.INTEGER, null, true, "{{addOne('custkey')}}"),
                                        Column.column("custkey_callAddOne", WrenTypes.INTEGER, null, true, "{{callAddOne('custkey')}}"),
                                        Column.column("custkey_pass1Macro", WrenTypes.INTEGER, null, true, "{{pass1Macro('custkey', addOne)}}"),
                                        Column.column("custkey_pass2Macro", WrenTypes.INTEGER, null, true, "{{pass2Macro('custkey', addOne, addTwo)}}"),
                                        Column.column("custkey_sum_addOne", WrenTypes.INTEGER, null, true, "{{addOne('sum(custkey)')}}"),
                                        Column.column("name", WrenTypes.VARCHAR, null, true)),
                                "pk")))
                .setMacros(List.of(
                        Macro.macro("addOne", "(text: Expression) => {{ text }} + 1"),
                        Macro.macro("addTwo", "(text: Expression) => {{ text }} + 2"),
                        Macro.macro("callAddOne", "(text: Expression) => {{addOne(text)}}"),
                        Macro.macro("pass1Macro", "(text: Expression, rule: Macro) => {{rule(text)}}"),
                        Macro.macro("pass2Macro", "(text: Expression, rule1: Macro, rule2: Macro) => {{ rule1(text) }} + {{ rule2(text)}}")))
                .build();

        WrenMDL mdl = WrenMDL.fromManifest(manifest);
        Optional<Model> modelOptional = mdl.getModel("Customer");
        assertThat(modelOptional).isPresent();
        assertThat(modelOptional.get().getColumns().get(1).getExpression().get()).isEqualTo("addOne(custkey)");
        assertThat(modelOptional.get().getColumns().get(2).getExpression().get()).isEqualTo("custkey + 1");
        assertThat(modelOptional.get().getColumns().get(3).getExpression().get()).isEqualTo("custkey + 1");
        assertThat(modelOptional.get().getColumns().get(4).getExpression().get()).isEqualTo("custkey + 1");
        assertThat(modelOptional.get().getColumns().get(5).getExpression().get()).isEqualTo("custkey + 1 + custkey + 2");
        assertThat(modelOptional.get().getColumns().get(6).getExpression().get()).isEqualTo("sum(custkey) + 1");
    }

    @Test
    public void testTwoParameterCall()
    {
        Manifest manifest = Manifest.builder()
                .setCatalog("test")
                .setSchema("test")
                .setModels(List.of(
                        Model.model("Customer",
                                "select * from main.customer",
                                List.of(
                                        Column.column("custkey", WrenTypes.INTEGER, null, true),
                                        Column.column("name", WrenTypes.VARCHAR, null, true),
                                        Column.column("custkey_concat_name", WrenTypes.INTEGER, null, true, "{{concat('custkey', 'name')}}"),
                                        Column.column("custkey_callAddOne", WrenTypes.INTEGER, null, true, "{{addPrefixOne('custkey')}}"),
                                        Column.column("custkey_pass1Macro", WrenTypes.INTEGER, null, true, "{{pass1Macro('custkey', 'name', concat)}}")),
                                "pk")))
                .setMacros(List.of(
                        Macro.macro("concat", "(text: Expression, text2: Expression) => {{ text }} || {{ text2 }}"),
                        Macro.macro("addPrefixOne", "(text: Expression) => {{concat(\"'1'\", text)}}"),
                        Macro.macro("pass1Macro", "(text: Expression, text2: Expression, cf: Macro) => {{cf(text, text2)}}")))
                .build();

        WrenMDL mdl = WrenMDL.fromManifest(manifest);
        Optional<Model> modelOptional = mdl.getModel("Customer");
        assertThat(modelOptional).isPresent();
        assertThat(modelOptional.get().getColumns().get(2).getExpression().get()).isEqualTo("custkey || name");
        assertThat(modelOptional.get().getColumns().get(3).getExpression().get()).isEqualTo("'1' || custkey");
        assertThat(modelOptional.get().getColumns().get(4).getExpression().get()).isEqualTo("custkey || name");
    }

    @Test
    public void testZeroParameterCall()
    {
        Manifest manifest = Manifest.builder()
                .setCatalog("test")
                .setSchema("test")
                .setModels(List.of(
                        Model.model("Customer",
                                "select * from main.customer",
                                List.of(
                                        Column.column("custkey", WrenTypes.INTEGER, null, true),
                                        Column.column("name", WrenTypes.VARCHAR, null, true),
                                        Column.column("standardTime", WrenTypes.INTEGER, null, true, "{{standardTime()}}"),
                                        Column.column("callStandardTime", WrenTypes.INTEGER, null, true, "{{callStandardTime()}}"),
                                        Column.column("passStandardTime", WrenTypes.INTEGER, null, true, "{{passStandardTime(standardTime)}}")),
                                "pk")))
                .setMacros(List.of(
                        Macro.macro("standardTime", "() => standardTime"),
                        Macro.macro("callStandardTime", "() => {{callStandardTime()}}"),
                        Macro.macro("passStandardTime", "(cf: Macro) => {{cf()}}")))
                .build();

        WrenMDL mdl = WrenMDL.fromManifest(manifest);
        Optional<Model> modelOptional = mdl.getModel("Customer");
        assertThat(modelOptional).isPresent();
        assertThat(modelOptional.get().getColumns().get(2).getExpression().get()).isEqualTo("standardTime");
        assertThat(modelOptional.get().getColumns().get(2).getExpression().get()).isEqualTo("standardTime");
        assertThat(modelOptional.get().getColumns().get(2).getExpression().get()).isEqualTo("standardTime");
    }
}
