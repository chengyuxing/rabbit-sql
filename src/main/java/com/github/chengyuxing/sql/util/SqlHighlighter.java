package com.github.chengyuxing.sql.util;

import com.github.chengyuxing.common.console.Style;
import com.github.chengyuxing.common.console.Printer;
import com.github.chengyuxing.common.script.ast.impl.KeyExpressionParser;
import com.github.chengyuxing.common.script.lang.Constants;
import com.github.chengyuxing.common.script.lang.TokenType;
import com.github.chengyuxing.common.script.lexer.RabbitScriptLexer;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.util.StringUtils;
import com.github.chengyuxing.sql.Keywords;
import com.github.chengyuxing.sql.XQLFileManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Sql highlight util.
 */
public final class SqlHighlighter {
    private static final Logger log = LoggerFactory.getLogger(SqlHighlighter.class);
    public static final Pattern QUOTE_PATTERN = Pattern.compile("'(''|[^'])*'|\"([^\"])*\"", Pattern.MULTILINE);
    public static final Pattern BLOCK_COMMENT_PATTERN = Pattern.compile("(/\\*.*?\\*/)", Pattern.DOTALL | Pattern.MULTILINE);
    public static final Pattern METADATA_NAME_PATTERN = Pattern.compile("@[a-zA-Z]\\w+");
    @SuppressWarnings("UnnecessaryUnicodeEscape")
    private static final String SUBSTR_KEY_PREFIX = "\u0c35";
    private static final Pattern SPLITTER_PATTERN = Pattern.compile("([\\s,():;{}]+)");

    public enum TAG {
        FUNCTION("func_name("),
        KEYWORD("sql statement keyword"),
        NUMBER("1, 3.14"),
        POSTGRESQL_FUNCTION_BODY_SYMBOL("$$"),
        ASTERISK("*"),
        QUOTE_STRING("'' or \"\""),
        LINE_COMMENT("-- comment"),
        BLOCK_COMMENT("/*  */"),
        METADATA_DEFINE_COMMENT("-- @name value"),
        RABBIT_SCRIPT_COMMENT("-- #if, --#for, ..."),
        INLINE_TEMPLATE_COMMENT("--//TEMPLATE-BEGIN:name, --//TEMPLATE-END"),
        NAMED_PARAMETER(":name, :user.id"),
        OTHER("");

        private final String description;

        TAG(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

    /**
     * Build highlight sql for console if console is active.
     *
     * @param sql SQL string
     * @return normal SQL string or highlight SQL string
     */
    public static String highlightIfAnsiCapable(String sql) {
        if (System.console() != null && System.getenv().get("TERM") != null) {
            return ansi(sql);
        }
        return sql;
    }

    /**
     * Highlight sql string with ansi color.
     *
     * @param sql SQL string
     * @return highlighted SQL
     */
    public static String ansi(String sql) {
        return highlight(sql, Printer::removeStyle, (tag, content) -> {
            switch (tag) {
                case FUNCTION:
                    return Printer.colorful(content, Style.BLUE);
                case KEYWORD:
                    return Printer.colorful(content, Style.DARK_PURPLE);
                case NUMBER:
                    return Printer.colorful(content, Style.DARK_CYAN);
                case POSTGRESQL_FUNCTION_BODY_SYMBOL:
                case QUOTE_STRING:
                    if (content.startsWith("'")) {
                        return Printer.colorful(content, Style.DARK_GREEN);
                    }
                    return content;
                case ASTERISK:
                    return Printer.colorful(content, Style.YELLOW);
                case METADATA_DEFINE_COMMENT:
                case RABBIT_SCRIPT_COMMENT:
                case INLINE_TEMPLATE_COMMENT:
                    // to replace the end to new style begin
                    String commented = content.replace(Printer.endStyle(), Printer.beginStyle(Style.SILVER));
                    return Printer.beginStyle(Style.SILVER) + commented + Printer.endStyle();
                case LINE_COMMENT:
                case BLOCK_COMMENT:
                    return Printer.colorful(content, Style.SILVER);
                case NAMED_PARAMETER:
                    return Printer.colorful(content, Style.CYAN);
                case OTHER:
                    if (StringUtils.equalsAnyIgnoreCase(content, RabbitScriptLexer.DIRECTIVES)) {
                        return Printer.colorful(content, Style.DARK_YELLOW);
                    }
                    if (METADATA_NAME_PATTERN.matcher(content).matches()) {
                        return "@" + Printer.colorful(content.substring(1), Style.DEFAULT_FG);
                    }
                    if (StringUtils.equalsAny(content, Constants.NULL,
                            Constants.BLANK, Constants.TRUE,
                            Constants.FALSE, TokenType.FOR_OF.name(),
                            TokenType.FOR_PROPERTY_AS.name(), TokenType.CHECK_THROW.name())) {
                        return Printer.colorful(content, Style.DARK_PURPLE);
                    }
                    return content;
                default:
                    return content;
            }
        });
    }

    /**
     * Custom highlight SQL string.
     *
     * @param sql                 SQL string
     * @param commentStyleCleaner clean the comment which has highlight words after the words handler, get the
     *                            original comment to do the next step line comment script resolve handler:
     *                            {@link TAG#METADATA_DEFINE_COMMENT METADATA_DEFINE_COMMENT}
     *                            {@link TAG#INLINE_TEMPLATE_COMMENT INLINE_TEMPLATE_COMMENT}
     *                            {@link TAG#RABBIT_SCRIPT_COMMENT RABBIT_SCRIPT_COMMENT}
     * @param replacer            colored content function: ({@link TAG tag}, content) -&gt; colored content
     * @return highlighted SQL
     */
    public static String highlight(String sql, Function<String, String> commentStyleCleaner, BiFunction<TAG, String, String> replacer) {
        try {
            Pair<String, Map<String, String>> r = escapeSubstring(sql);
            String rSql = r.getItem1();
            Pair<List<String>, List<String>> x = StringUtils.regexSplit(rSql, SPLITTER_PATTERN, 1);
            List<String> words = x.getItem1();
            List<String> delimiters = x.getItem2();
            StringBuilder sb = new StringBuilder();
            for (int i = 0, j = words.size(); i < j; i++) {
                String word = words.get(i);
                String replacement = word;
                if (!StringUtils.isEmpty(word)) {
                    // functions highlight
                    if (!StringUtils.equalsAnyIgnoreCase(word, Keywords.STANDARD) && detectFunction(word, i, j, delimiters)) {
                        replacement = replacer.apply(TAG.FUNCTION, word);
                        // named parameter
                    } else if (detectNamedParameter(word, i, delimiters)) {
                        replacement = replacer.apply(TAG.NAMED_PARAMETER, word);
                        // keywords highlight
                    } else if (StringUtils.equalsAnyIgnoreCase(word, Keywords.STANDARD)) {
                        replacement = replacer.apply(TAG.KEYWORD, word);
                        // number highlight
                    } else if (StringUtils.isNumber(word)) {
                        replacement = replacer.apply(TAG.NUMBER, word);
                        // PostgreSQL function body block highlight
                    } else if (word.equals("$$")) {
                        replacement = replacer.apply(TAG.POSTGRESQL_FUNCTION_BODY_SYMBOL, word);
                        // symbol '*' highlight
                    } else if (word.equals("*")) {
                        replacement = replacer.apply(TAG.ASTERISK, word);
                    } else if (!word.matches(SUBSTR_KEY_PREFIX + "\\d+")) {
                        replacement = replacer.apply(TAG.OTHER, word);
                    }
                }
                sb.append(replacement);
                if (i < j - 1) {
                    sb.append(delimiters.get(i));
                }
            }
            String colorfulSql = sb.toString();
            // reinsert the sub string
            Map<String, String> subStr = r.getItem2();
            for (Map.Entry<String, String> e : subStr.entrySet()) {
                colorfulSql = colorfulSql.replace(e.getKey(), replacer.apply(TAG.QUOTE_STRING, e.getValue()));
            }
            // resolve single comment
            String[] sqlLines = colorfulSql.split("\n");
            for (int i = 0; i < sqlLines.length; i++) {
                String line = sqlLines[i];
                int lineCmtIdx = findLineCommentIndex(line);
                if (lineCmtIdx != -1) {
                    String cleanedLine = commentStyleCleaner.apply(line);
                    String head = line.substring(0, lineCmtIdx);
                    String tail = line.substring(lineCmtIdx);
                    if (StringUtils.isEmpty(head)) {
                        // @name value
                        if (XQLFileManager.META_DATA_PATTERN.matcher(cleanedLine).matches()) {
                            sqlLines[i] = head + replacer.apply(TAG.METADATA_DEFINE_COMMENT, tail);
                            continue;
                        }
                        // inline template
                        if (XQLFileManager.INLINE_TEMPLATE_BEGIN_PATTERN.matcher(cleanedLine).matches() ||
                                XQLFileManager.INLINE_TEMPLATE_END_PATTERN.matcher(cleanedLine).matches()) {
                            sqlLines[i] = head + replacer.apply(TAG.INLINE_TEMPLATE_COMMENT, tail);
                            continue;
                        }
                        // rabbit script
                        if (RabbitScriptLexer.DIRECTIVES_PATTERN.matcher(commentStyleCleaner.apply(tail.substring(2))).matches()) {
                            sqlLines[i] = head + replacer.apply(TAG.RABBIT_SCRIPT_COMMENT, tail);
                            continue;
                        }
                    }
                    sqlLines[i] = head + replacer.apply(TAG.LINE_COMMENT, tail);
                }
            }
            colorfulSql = String.join("\n", sqlLines);
            // resolve block comment
            Matcher matcher = BLOCK_COMMENT_PATTERN.matcher(colorfulSql);
            StringBuffer blockCmtBuf = new StringBuffer();
            while (matcher.find()) {
                matcher.appendReplacement(
                        blockCmtBuf,
                        Matcher.quoteReplacement(replacer.apply(TAG.BLOCK_COMMENT, matcher.group()))
                );
            }
            matcher.appendTail(blockCmtBuf);
            return blockCmtBuf.toString();
        } catch (Exception e) {
            log.error("highlight sql error.", e);
            return sql;
        }
    }

    /**
     * Finds the index of the start of a line comment (--) in the given line.
     * It skips over any single or double-quoted strings, and only identifies
     * '--' as a comment if it is not within a quoted string.
     *
     * @param line the line of text to search for a line comment
     * @return the index of the start of the line comment, or -1 if no line comment is found
     */
    private static int findLineCommentIndex(String line) {
        boolean inSingle = false;
        boolean inDouble = false;

        for (int i = 0; i < line.length() - 1; i++) {
            char c = line.charAt(i);

            if (c == '\'' && !inDouble) {
                if (inSingle && i + 1 < line.length() && line.charAt(i + 1) == '\'') {
                    i++; // skip escaped ''
                } else {
                    inSingle = !inSingle;
                }
                continue;
            }

            if (c == '"' && !inSingle) {
                inDouble = !inDouble;
                continue;
            }

            if (!inSingle && !inDouble && c == '-' && line.charAt(i + 1) == '-') {
                return i;
            }
        }
        return -1;
    }

    /**
     * Detect probably is function or not.
     *
     * @param i          word index
     * @param j          max word length
     * @param delimiters delimiters
     * @return true or false
     */
    private static boolean detectFunction(String word, int i, int j, List<String> delimiters) {
        if (!word.matches("^[a-zA-Z][\\w.]+")) {
            return false;
        }
        if (delimiters.isEmpty()) {
            return false;
        }
        if (i >= delimiters.size()) {
            return false;
        }
        if (i < j - 1) {
            return StringUtils.indexOfNonWhitespace(delimiters.get(i), "(") != -1;
        }
        return false;
    }

    private static boolean detectNamedParameter(String word, int i, List<String> delimiters) {
        if (!KeyExpressionParser.EXPRESSION_PATTERN.matcher(word).matches()) {
            return false;
        }
        if (delimiters.isEmpty()) {
            return false;
        }
        int idx = Math.max(0, i - 1);
        if (idx >= delimiters.size()) {
            return false;
        }
        String prefix = delimiters.get(idx).trim();
        return prefix.endsWith(":") && !prefix.endsWith("::");
    }

    /**
     * Escape SQL substring ({@code '} or {@code "}) to unique string holder and save the substring map.
     *
     * @param sql SQL string
     * @return [SQL string with unique string holder, substring map]
     */
    private static Pair<String, Map<String, String>> escapeSubstring(final String sql) {
        if (!sql.contains("'") && !sql.contains("\"")) {
            return Pair.of(sql, Collections.emptyMap());
        }
        AtomicInteger index = new AtomicInteger();
        Map<String, String> map = new HashMap<>();
        StringBuilder sb = new StringBuilder();
        StringUtils.scan(sql, QUOTE_PATTERN, 0, (seg, hit) -> {
            if (hit) {
                String key = SUBSTR_KEY_PREFIX + index.getAndIncrement();
                map.put(key, seg);
                sb.append(key);
            } else {
                sb.append(seg);
            }
        });
        return Pair.of(sb.toString(), map);
    }
}
