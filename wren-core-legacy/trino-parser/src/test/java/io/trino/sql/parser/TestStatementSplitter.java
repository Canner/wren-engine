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
package io.trino.sql.parser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.parser.StatementSplitter.Statement;
import static io.trino.sql.parser.StatementSplitter.isEmptyStatement;
import static io.trino.sql.parser.StatementSplitter.squeezeStatement;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestStatementSplitter
{
    @Test
    public void testSplitterIncomplete()
    {
        StatementSplitter splitter = new StatementSplitter(" select * FROM foo  ");
        assertEquals(ImmutableList.of(), splitter.getCompleteStatements());
        assertEquals("select * FROM foo", splitter.getPartialStatement());
    }

    @Test
    public void testSplitterEmptyInput()
    {
        StatementSplitter splitter = new StatementSplitter("");
        assertEquals(ImmutableList.of(), splitter.getCompleteStatements());
        assertEquals("", splitter.getPartialStatement());
    }

    @Test
    public void testSplitterEmptyStatements()
    {
        StatementSplitter splitter = new StatementSplitter(";;;");
        assertEquals(ImmutableList.of(), splitter.getCompleteStatements());
        assertEquals("", splitter.getPartialStatement());
    }

    @Test
    public void testSplitterSingle()
    {
        StatementSplitter splitter = new StatementSplitter("select * from foo;");
        assertEquals(statements("select * from foo", ";"), splitter.getCompleteStatements());
        assertEquals("", splitter.getPartialStatement());
    }

    @Test
    public void testSplitterMultiple()
    {
        StatementSplitter splitter = new StatementSplitter(" select * from  foo ; select * from t; select * from ");
        assertEquals(statements("select * from  foo", ";", "select * from t", ";"), splitter.getCompleteStatements());
        assertEquals("select * from", splitter.getPartialStatement());
    }

    @Test
    public void testSplitterMultipleWithEmpty()
    {
        StatementSplitter splitter = new StatementSplitter("; select * from  foo ; select * from t;;;select * from ");
        assertEquals(statements("select * from  foo", ";", "select * from t", ";"), splitter.getCompleteStatements());
        assertEquals("select * from", splitter.getPartialStatement());
    }

    @Test
    public void testSplitterCustomDelimiters()
    {
        String sql = "// select * from  foo // select * from t;//select * from ";
        StatementSplitter splitter = new StatementSplitter(sql, ImmutableSet.of(";", "//"));
        assertEquals(statements("select * from  foo", "//", "select * from t", ";"), splitter.getCompleteStatements());
        assertEquals("select * from", splitter.getPartialStatement());
    }

    @Test
    public void testSplitterErrorBeforeComplete()
    {
        StatementSplitter splitter = new StatementSplitter(" select * from z# oops ; select ");
        assertEquals(statements("select * from z# oops", ";"), splitter.getCompleteStatements());
        assertEquals("select", splitter.getPartialStatement());
    }

    @Test
    public void testSplitterErrorAfterComplete()
    {
        StatementSplitter splitter = new StatementSplitter("select * from foo; select z# oops ");
        assertEquals(statements("select * from foo", ";"), splitter.getCompleteStatements());
        assertEquals("select z# oops", splitter.getPartialStatement());
    }

    @Test
    public void testSplitterWithQuotedString()
    {
        String sql = "select 'foo bar' x from dual";
        StatementSplitter splitter = new StatementSplitter(sql);
        assertEquals(ImmutableList.of(), splitter.getCompleteStatements());
        assertEquals(sql, splitter.getPartialStatement());
    }

    @Test
    public void testSplitterWithIncompleteQuotedString()
    {
        String sql = "select 'foo', 'bar";
        StatementSplitter splitter = new StatementSplitter(sql);
        assertEquals(ImmutableList.of(), splitter.getCompleteStatements());
        assertEquals(sql, splitter.getPartialStatement());
    }

    @Test
    public void testSplitterWithEscapedSingleQuote()
    {
        String sql = "select 'hello''world' from dual";
        StatementSplitter splitter = new StatementSplitter(sql + ";");
        assertEquals(statements(sql, ";"), splitter.getCompleteStatements());
        assertEquals("", splitter.getPartialStatement());
    }

    @Test
    public void testSplitterWithQuotedIdentifier()
    {
        String sql = "select \"0\"\"bar\" from dual";
        StatementSplitter splitter = new StatementSplitter(sql + ";");
        assertEquals(statements(sql, ";"), splitter.getCompleteStatements());
        assertEquals("", splitter.getPartialStatement());
    }

    @Test
    public void testSplitterWithBackquote()
    {
        String sql = "select  ` f``o o ` from dual";
        StatementSplitter splitter = new StatementSplitter(sql);
        assertEquals(ImmutableList.of(), splitter.getCompleteStatements());
        assertEquals(sql, splitter.getPartialStatement());
    }

    @Test
    public void testSplitterWithDigitIdentifier()
    {
        String sql = "select   1x  from dual";
        StatementSplitter splitter = new StatementSplitter(sql);
        assertEquals(ImmutableList.of(), splitter.getCompleteStatements());
        assertEquals(sql, splitter.getPartialStatement());
    }

    @Test
    public void testSplitterWithSingleLineComment()
    {
        StatementSplitter splitter = new StatementSplitter("--empty\n;-- start\nselect * -- junk\n-- hi\nfrom foo; -- done");
        assertEquals(statements("--empty", ";", "-- start\nselect * -- junk\n-- hi\nfrom foo", ";"), splitter.getCompleteStatements());
        assertEquals("-- done", splitter.getPartialStatement());
    }

    @Test
    public void testSplitterWithMultiLineComment()
    {
        StatementSplitter splitter = new StatementSplitter("/* empty */;/* start */ select * /* middle */ from foo; /* end */");
        assertEquals(statements("/* empty */", ";", "/* start */ select * /* middle */ from foo", ";"), splitter.getCompleteStatements());
        assertEquals("/* end */", splitter.getPartialStatement());
    }

    @Test
    public void testSplitterWithSingleLineCommentPartial()
    {
        String sql = "-- start\nselect * -- junk\n-- hi\nfrom foo -- done";
        StatementSplitter splitter = new StatementSplitter(sql);
        assertEquals(ImmutableList.of(), splitter.getCompleteStatements());
        assertEquals(sql, splitter.getPartialStatement());
    }

    @Test
    public void testSplitterWithMultiLineCommentPartial()
    {
        String sql = "/* start */ select * /* middle */ from foo /* end */";
        StatementSplitter splitter = new StatementSplitter(sql);
        assertEquals(ImmutableList.of(), splitter.getCompleteStatements());
        assertEquals(sql, splitter.getPartialStatement());
    }

    @Test
    public void testIsEmptyStatement()
    {
        assertTrue(isEmptyStatement(""));
        assertTrue(isEmptyStatement(" "));
        assertTrue(isEmptyStatement("\t\n "));
        assertTrue(isEmptyStatement("--foo\n  --what"));
        assertTrue(isEmptyStatement("/* oops */"));
        assertFalse(isEmptyStatement("x"));
        assertFalse(isEmptyStatement("select"));
        assertFalse(isEmptyStatement("123"));
        assertFalse(isEmptyStatement("z#oops"));
    }

    @Test
    public void testSqueezeStatement()
    {
        String sql = "select   *  from\n foo\n  order by x ; ";
        assertEquals("select * from foo order by x ;", squeezeStatement(sql));
    }

    @Test
    public void testSqueezeStatementWithIncompleteQuotedString()
    {
        String sql = "select   *  from\n foo\n  where x = 'oops";
        assertEquals("select * from foo where x = 'oops", squeezeStatement(sql));
    }

    @Test
    public void testSqueezeStatementWithBackquote()
    {
        String sql = "select  `  f``o  o`` `   from dual";
        assertEquals("select `  f``o  o`` ` from dual", squeezeStatement(sql));
    }

    @Test
    public void testSqueezeStatementAlternateDelimiter()
    {
        String sql = "select   *  from\n foo\n  order by x // ";
        assertEquals("select * from foo order by x //", squeezeStatement(sql));
    }

    @Test
    public void testSqueezeStatementError()
    {
        String sql = "select   *  from z#oops";
        assertEquals("select * from z#oops", squeezeStatement(sql));
    }

    private static List<Statement> statements(String... args)
    {
        checkArgument(args.length % 2 == 0, "arguments not paired");
        ImmutableList.Builder<Statement> list = ImmutableList.builder();
        for (int i = 0; i < args.length; i += 2) {
            list.add(new Statement(args[i], args[i + 1]));
        }
        return list.build();
    }
}
