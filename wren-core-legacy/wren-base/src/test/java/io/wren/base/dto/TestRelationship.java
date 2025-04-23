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

import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.sql.SqlFormatter.formatSql;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestRelationship
{
    @Test
    public void testOrdering()
    {
        Assertions.assertThat(Relationship.SortKey.Ordering.get("asc")).isEqualTo(Relationship.SortKey.Ordering.ASC);
        Assertions.assertThat(Relationship.SortKey.Ordering.get("ASC")).isEqualTo(Relationship.SortKey.Ordering.ASC);
        Assertions.assertThat(Relationship.SortKey.Ordering.get("aSC")).isEqualTo(Relationship.SortKey.Ordering.ASC);
        Assertions.assertThat(Relationship.SortKey.Ordering.get("DESC")).isEqualTo(Relationship.SortKey.Ordering.DESC);
        assertThatThrownBy(() -> Relationship.SortKey.Ordering.get("foo")).hasMessage("Unsupported ordering");
    }

    @Test
    public void testQualifiedCondition()
    {
        String expected = "(\"A\".\"c1\" = \"B\".\"c1\")";
        Relationship relationship = Relationship.relationship("name", List.of("A", "B"), JoinType.ONE_TO_ONE, "A.c1 = B.c1");
        assertThat(formatSql(relationship.getQualifiedCondition())).isEqualTo(expected);
        relationship = Relationship.relationship("name", List.of("A", "B"), JoinType.ONE_TO_ONE, "\"A\".c1 = \"B\".c1");
        assertThat(formatSql(relationship.getQualifiedCondition())).isEqualTo(expected);
        relationship = Relationship.relationship("name", List.of("A", "B"), JoinType.ONE_TO_ONE, "A.\"c1\" = B.\"c1\"");
        assertThat(formatSql(relationship.getQualifiedCondition())).isEqualTo(expected);
    }
}
