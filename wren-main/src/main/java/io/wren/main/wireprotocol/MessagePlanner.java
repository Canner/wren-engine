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

package io.wren.main.wireprotocol;

import io.wren.main.wireprotocol.message.Describe;
import io.wren.main.wireprotocol.message.Execute;
import io.wren.main.wireprotocol.message.ExecuteAndSendRowDescription;
import io.wren.main.wireprotocol.message.Plan;
import io.wren.main.wireprotocol.message.SendResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class MessagePlanner
{
    private MessagePlanner() {}

    public static List<Plan> plan(Queue<Plan> messages)
    {
        List<Plan> plans = new ArrayList<>(messages.size());
        Map<String, IndexedDescribePortal> portalDescriptions = new HashMap<>();
        int i = 0;
        while (!(messages.isEmpty())) {
            Plan message = messages.poll();
            if (message instanceof Describe.DescribePortal) {
                Describe.DescribePortal describePortal = (Describe.DescribePortal) message;
                portalDescriptions.put(describePortal.getTargetName(), new IndexedDescribePortal(i, describePortal));
            }
            if (message instanceof Execute) {
                Execute execute = (Execute) message;
                IndexedDescribePortal indexedDescribePortal = portalDescriptions.get(execute.getPortalName());
                if (indexedDescribePortal != null) {
                    ExecuteAndSendRowDescription executeAndSendRowDescription = new ExecuteAndSendRowDescription(execute);
                    plans.set(indexedDescribePortal.index, executeAndSendRowDescription);
                    plans.add(new SendResult(executeAndSendRowDescription));
                    i++;
                    continue;
                }
            }
            plans.add(message);
            i++;
        }
        return plans;
    }

    static class IndexedDescribePortal
    {
        final int index;
        final Describe.DescribePortal describePortal;

        IndexedDescribePortal(int index, Describe.DescribePortal describePortal)
        {
            this.index = index;
            this.describePortal = describePortal;
        }
    }
}
