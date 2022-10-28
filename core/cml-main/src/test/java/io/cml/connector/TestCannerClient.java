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

package io.cml.connector;

import com.google.common.collect.ImmutableList;
import io.cml.connector.canner.CannerClient;
import io.cml.connector.canner.CannerConfig;
import io.cml.connector.canner.dto.TableDto;
import io.cml.connector.canner.dto.WorkspaceDto;
import io.cml.spi.ConnectorRecordIterator;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCannerClient
{
    private final CannerClient cannerClient;

    public TestCannerClient()
    {
        CannerConfig cannerConfig = new CannerConfig()
                .setCannerUrl("http://localhost")
                .setAvailableWorkspace("localtest")
                .setPersonalAccessToken("Mzc3ZTg4MzgtOGM2Zi00NWRmLTgwMWQtMzYwNGMxMzBhNWFiX2dtbC10ZXN0OkVQSEw4a1ZuSDJwaDRha29ZNUI1cG5uUTJQc1N3MFJJ");
        cannerClient = new CannerClient(cannerConfig);
    }

    @Test(enabled = false)
    public void testListWorkspace()
    {
        List<WorkspaceDto> workspaceDtos = cannerClient.listWorkspaces();
        assertThat(workspaceDtos.size()).isEqualTo(2);
    }

    @Test(enabled = false)
    public void testIsWorkspaceExist()
    {
        assertThat(cannerClient.getOneWorkspace("fake")).isEmpty();
        assertThat(cannerClient.getOneWorkspace("tworkspace")).isPresent();
    }

    @Test(enabled = false)
    public void testListTables()
    {
        Optional<WorkspaceDto> workspaceDto = cannerClient.getWorkspaceBySqlName("tworkspace");
        List<TableDto> tables = cannerClient.listTables(workspaceDto.get().getId());
        assertThat(tables.size()).isEqualTo(3);
    }

    @Test(enabled = false)
    public void testQuery()
    {
        int count = 0;

        ConnectorRecordIterator iterator = cannerClient.query("select * from tiny_orders limit 100;", ImmutableList.of());
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        assertThat(count).isEqualTo(100);
    }

    @Test(enabled = false)
    public void testDescribe()
    {
        int count = 0;
        CannerClient.TrinoRecordIterator result = cannerClient.describe("select * from tpch.tiny.orders", ImmutableList.of());
        while (result.hasNext()) {
            result.next();
            count++;
        }
        assertThat(count).isEqualTo(9);
    }
}
