package tests;

import com.github.chengyuxing.common.console.Printer;
import com.github.chengyuxing.sql.util.SqlHighlighter;
import com.github.chengyuxing.sql.util.SqlHighlighter.TAG;
import org.junit.Test;

import java.util.*;
import java.util.function.Function;

import static org.junit.Assert.*;

public class SqlHighlighterRegressionTest {
    @Test
    public void punctuationWhitespaceAndLineEndingsArePreservedExactly() {
        for (String sql : new String[]{"", " \t\r\n", "select 1;", "select 1;\n\n", "\nselect (1);\r\n",
                "-- user's remark\r\nselect :id;\r\n", "select @foo, {call f(:id)};"}) {
            record(sql);
        }
    }

    @Test
    public void manyLiteralsAndFormerPlaceholderCharactersCannotCollide() {
        StringBuilder sql = new StringBuilder("select వ0, ");
        for (int i = 0; i < 100; i++) {
            if (i > 0) sql.append(", ");
            sql.append("'వ").append(i).append(" $value'");
        }
        sql.append(";\n");
        Map<TAG, List<String>> tags = record(sql.toString());
        assertEquals(100, tags.get(TAG.QUOTE_STRING).size());
        assertTrue(tags.get(TAG.OTHER).contains("వ0"));
    }

    @Test
    public void quotedValuesAndIdentifiersAreOpaqueIncludingAcrossLines() {
        Map<TAG, List<String>> tags = record("select '/* not a comment */', 'Bob''s -- text', "
                + "'first\r\n-- still text\nlast', \"a\"\"--:id\", `escaped``:param`;\n");
        assertEquals(5, tags.get(TAG.QUOTE_STRING).size());
        assertFalse(tags.containsKey(TAG.LINE_COMMENT));
        assertFalse(tags.containsKey(TAG.BLOCK_COMMENT));
        assertFalse(tags.containsKey(TAG.NAMED_PARAMETER));
    }

    @Test
    public void ordinaryCommentsCannotConsumeQuotesInFollowingCode() {
        Map<TAG, List<String>> tags = record("-- user's 'unbalanced remark\nselect :id, 'value'; "
                + "/* outer ' /* nested :ignored */ still outer */ select :other;\r\n");
        assertEquals(Arrays.asList("id", "other"), tags.get(TAG.NAMED_PARAMETER));
        assertEquals(Collections.singletonList("'value'"), tags.get(TAG.QUOTE_STRING));
        assertEquals(Collections.singletonList("-- user's 'unbalanced remark"), tags.get(TAG.LINE_COMMENT));
        assertEquals(Collections.singletonList("/* outer ' /* nested :ignored */ still outer */"), tags.get(TAG.BLOCK_COMMENT));
    }

    @Test
    public void dollarQuotesAreOpaqueAndRequireAnIdentifierBoundary() {
        Map<TAG, List<String>> tags = record("do $$begin :hidden; -- text\nend$$; select "
                + "$tag$:hidden /* text */$other$$tag$, $标签$:hidden$标签$, foo$tag$bar, :visible;");
        assertEquals(3, tags.get(TAG.QUOTE_STRING).size());
        assertEquals(Collections.singletonList("visible"), tags.get(TAG.NAMED_PARAMETER));
        assertFalse(tags.containsKey(TAG.LINE_COMMENT));
        assertFalse(tags.containsKey(TAG.BLOCK_COMMENT));
        assertTrue(tags.get(TAG.OTHER).contains("foo$tag$bar"));
    }

    @Test
    public void parametersKeepNestedPathsAndPostgresqlCastsStayOutsideThem() {
        Map<TAG, List<String>> tags = record("select :user.addresses[0]['first.name']::text, "
                + ":other[\"--key\"], 1::int, ':hidden', \"id:hidden\";\n");
        assertEquals(Arrays.asList("user.addresses[0]['first.name']", "other[\"--key\"]"), tags.get(TAG.NAMED_PARAMETER));
    }

    @Test
    public void singleLetterFunctionsAndAdjacentNumbersAreRecognized() {
        Map<TAG, List<String>> tags = record("SELECT f(:id), _f (:id), schema.f(:id), 1+2, 3.14, .5, 6e-3, -7, *;");
        assertEquals(Arrays.asList("f", "_f", "schema.f"), tags.get(TAG.FUNCTION));
        assertEquals(Arrays.asList("1", "2", "3.14", ".5", "6e-3", "7"), tags.get(TAG.NUMBER));
        assertTrue(tags.get(TAG.KEYWORD).contains("SELECT"));
        assertEquals(Collections.singletonList("*"), tags.get(TAG.ASTERISK));
    }

    @Test
    public void metadataTemplatesAndRabbitScriptKeepTheirTagsAndInnerStyles() {
        String sql = "  -- @cache 30m\r\n-- #check :age > 30 throw '年龄不能大于30岁'\n"
                + "-- #if :name != blank\n-- //TEMPLATE-BEGIN:myCnd\n-- //TEMPLATE-END\n"
                + "select :id; -- #if :notAScript\n";
        Map<TAG, List<String>> tags = record(sql);
        assertEquals(1, tags.get(TAG.METADATA_DEFINE_COMMENT).size());
        assertEquals(2, tags.get(TAG.RABBIT_SCRIPT_COMMENT).size());
        assertEquals(2, tags.get(TAG.INLINE_TEMPLATE_COMMENT).size());
        assertEquals(Collections.singletonList("-- #if :notAScript"), tags.get(TAG.LINE_COMMENT));
        assertEquals(Arrays.asList("age", "name", "myCnd", "id"), tags.get(TAG.NAMED_PARAMETER));
        assertTrue(tags.get(TAG.OTHER).contains("#check"));
        String colored = SqlHighlighter.highlight(sql, Printer::removeStyle,
                (tag, content) -> tag == TAG.NAMED_PARAMETER ? Printer.colorful(content,
                        com.github.chengyuxing.common.console.Style.CYAN) : content);
        assertEquals(sql, Printer.removeStyle(colored));
    }

    @Test
    public void postgresqlEscapeStringsProtectBackslashEscapedQuotes() {
        String escaped = "E'one\\' -- :hidden /* text */'";
        Map<TAG, List<String>> tags = record("select " + escaped + ", :visible;");
        assertEquals(Collections.singletonList("visible"), tags.get(TAG.NAMED_PARAMETER));
        assertEquals(Collections.singletonList(escaped.substring(1)), tags.get(TAG.QUOTE_STRING));
        assertFalse(tags.containsKey(TAG.LINE_COMMENT));
        assertFalse(tags.containsKey(TAG.BLOCK_COMMENT));
        tags = record("select 'C:\\', :visible;");
        assertEquals(Collections.singletonList("visible"), tags.get(TAG.NAMED_PARAMETER));
    }

    @Test
    public void incompleteSqlPreservesTextAndTreatsOpenRegionsAsOpaque() {
        for (String sql : new String[]{"select 'open :hidden\n", "select $tag$open :hidden\n",
                "select /* open :hidden\n", "select `open :hidden\n", "select \"open :hidden\n"}) {
            assertFalse(record(sql).containsKey(TAG.NAMED_PARAMETER));
        }
    }

    @Test
    public void callbackFailureReturnsTheCompleteOriginalSql() {
        String sql = "select :id;\n\n";
        assertEquals(sql, SqlHighlighter.highlight(sql, Function.identity(), (tag, content) -> {
            throw new IllegalStateException("test callback failure");
        }));
    }

    private static Map<TAG, List<String>> record(String sql) {
        Map<TAG, List<String>> tags = new EnumMap<>(TAG.class);
        String result = SqlHighlighter.highlight(sql, Function.identity(), (tag, content) -> {
            tags.computeIfAbsent(tag, ignored -> new ArrayList<>()).add(content);
            return content;
        });
        assertEquals(sql, result);
        assertEquals(sql, Printer.removeStyle(SqlHighlighter.ansi(sql)));
        return tags;
    }
}
