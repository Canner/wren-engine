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

package io.wren.main.wireprotocol.message;

import io.wren.main.wireprotocol.FormatCodes.FormatCode;
import io.wren.main.wireprotocol.MessagePlanner;
import org.testng.annotations.Test;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

import static io.wren.main.wireprotocol.message.Bind.bind;
import static io.wren.main.wireprotocol.message.Describe.describePortal;
import static io.wren.main.wireprotocol.message.Describe.describeStatement;
import static io.wren.main.wireprotocol.message.Execute.execute;
import static io.wren.main.wireprotocol.message.Parse.parse;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMessagePlanner
{
    @Test
    public void testPlan()
    {
        List<Plan> expected = List.of(
                parse("", "select * from test", (short) 0, new int[0]),
                bind("", "", new FormatCode[0], List.of(), new FormatCode[0]),
                execute("", 0));
        Queue<Plan> plans = new ArrayDeque<>(expected);

        List<Plan> planned = MessagePlanner.plan(plans);
        assertThat(planned).isEqualTo(expected);

        expected = List.of(
                parse("", "select * from test", (short) 0, new int[0]),
                describeStatement(""),
                bind("", "", new FormatCode[0], List.of(), new FormatCode[0]),
                execute("", 0));
        assertThat(MessagePlanner.plan(new ArrayDeque<>(expected))).isEqualTo(expected);
    }

    @Test
    public void testOptimizeDescribePortalAndExecute()
    {
        Plan parse = parse("", "select * from test", (short) 0, new int[0]);
        Plan bind = bind("", "", new FormatCode[0], List.of(), new FormatCode[0]);
        Plan describePortal = describePortal("");
        Plan execute = execute("", 0);
        List<Plan> actual = List.of(parse, bind, describePortal, execute);

        List<Plan> planned = MessagePlanner.plan(new ArrayDeque<>(actual));
        assertThat(planned.contains(describePortal)).isFalse();
        assertThat(planned.contains(execute)).isFalse();
        assertThat(planned).anyMatch(p -> p instanceof DescribePortalAndExecute);
        assertThat(planned).anyMatch(p -> p instanceof SendResult);

        Plan anotherParse = parse("test", "select * from test", (short) 0, new int[0]);
        Plan anotherBind = bind("test", "test", new FormatCode[0], List.of(), new FormatCode[0]);
        Plan anotherDescribePortal = describePortal("test");

        actual = List.of(anotherParse, anotherBind, anotherDescribePortal, parse, bind, describePortal, execute);
        planned = MessagePlanner.plan(new ArrayDeque<>(actual));
        assertThat(planned.contains(anotherDescribePortal)).isTrue();
        assertThat(planned.contains(describePortal)).isFalse();
        assertThat(planned.contains(execute)).isFalse();
        assertThat(planned.get(5)).isInstanceOf(DescribePortalAndExecute.class);
        assertThat(planned.get(6)).isInstanceOf(SendResult.class);
        assertThat(((DescribePortalAndExecute) planned.get(5)).getPortalName()).isEqualTo("");

        actual = List.of(parse, bind, describePortal, parse, bind, describePortal, execute);
        planned = MessagePlanner.plan(new ArrayDeque<>(actual));
        assertThat(planned.size()).isEqualTo(7);
        assertThat(planned.contains(describePortal)).isTrue();
        assertThat(planned.indexOf(describePortal)).isEqualTo(2);
        assertThat(planned.get(5)).isInstanceOf(DescribePortalAndExecute.class);
        assertThat(planned.get(6)).isInstanceOf(SendResult.class);
    }
}
