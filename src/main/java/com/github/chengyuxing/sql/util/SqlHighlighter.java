package com.github.chengyuxing.sql.util;

import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.console.Printer;
import com.github.chengyuxing.common.Patterns;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.util.StringUtils;
import com.github.chengyuxing.sql.Keywords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Sql highlight util.
 */
public final class SqlHighlighter {
    private static final Logger log = LoggerFactory.getLogger(SqlHighlighter.class);
    public static final Pattern STR_PATTERN = Pattern.compile("'(''|[^'])*'", Pattern.MULTILINE);
    public static final Pattern BLOCK_COMMENT_PATTERN = Pattern.compile("(/\\*.*?\\*/)", Pattern.DOTALL | Pattern.MULTILINE);

    public enum TAG {
        FUNCTION,
        KEYWORD,
        NUMBER,
        /**
         * $$
         * body
         * $$
         */
        POSTGRESQL_FUNCTION_BODY_SYMBOL,
        ASTERISK,
        SINGLE_QUOTE_STRING,
        LINE_COMMENT,
        BLOCK_COMMENT,
        NAMED_PARAMETER,
        OTHER
    }

    /**
     * Build highlight sql for console if console is active.
     *
     * @param sql sql string
     * @return normal sql string or highlight sql string
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
     * @param sql sql string
     * @return highlighted sql
     */
    public static String ansi(String sql) {
        return highlight(sql, (tag, content) -> {
            switch (tag) {
                case FUNCTION:
                    return Printer.colorful(content, Color.BLUE);
                case KEYWORD:
                    return Printer.colorful(content, Color.DARK_PURPLE);
                case NUMBER:
                    return Printer.colorful(content, Color.DARK_CYAN);
                case POSTGRESQL_FUNCTION_BODY_SYMBOL:
                case SINGLE_QUOTE_STRING:
                    return Printer.colorful(content, Color.DARK_GREEN);
                case ASTERISK:
                    return Printer.colorful(content, Color.YELLOW);
                case LINE_COMMENT:
                    return Printer.colorful(content, Color.SILVER);
                case BLOCK_COMMENT:
                    return Printer.colorful(content.replaceAll("\033\\[\\d{2}m|\033\\[0m", ""), Color.SILVER);
                case NAMED_PARAMETER:
                    return Printer.colorful(content, Color.CYAN);
                default:
                    return content;
            }
        });
    }

    /**
     * Custom highlight sql string.
     *
     * @param sql      sql string
     * @param replacer colored content function: ({@link TAG tag}, content) -&gt; colored content
     * @return highlighted sql
     */
    public static String highlight(String sql, BiFunction<TAG, String, String> replacer) {
        try {
            Pair<String, Map<String, String>> r = escapeSubstring(sql);
            String rSql = r.getItem1();
            Pair<List<String>, List<String>> x = StringUtils.regexSplit(rSql, "(?<d>[\\s,\\[\\]():;{}]+)", "d");
            List<String> words = x.getItem1();
            List<String> delimiters = x.getItem2();
            StringBuilder sb = new StringBuilder();
            for (int i = 0, j = words.size(); i < j; i++) {
                String word = words.get(i);
                String replacement = word;
                if (!word.trim().isEmpty()) {
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
                    } else if (StringUtils.isNumeric(word)) {
                        replacement = replacer.apply(TAG.NUMBER, word);
                        // PostgreSQL function body block highlight
                    } else if (word.equals("$$")) {
                        replacement = replacer.apply(TAG.POSTGRESQL_FUNCTION_BODY_SYMBOL, word);
                        // symbol '*' highlight
                    } else if (word.equals("*")) {
                        replacement = replacer.apply(TAG.ASTERISK, word);
                    } else {
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
            for (String key : subStr.keySet()) {
                colorfulSql = colorfulSql.replace(key, replacer.apply(TAG.SINGLE_QUOTE_STRING, subStr.get(key)));
            }
            // resolve single comment
            String[] sqlLines = colorfulSql.split("\n");
            for (int i = 0; i < sqlLines.length; i++) {
                String line = sqlLines[i];
                if (line.trim().startsWith("--")) {
                    sqlLines[i] = replacer.apply(TAG.LINE_COMMENT, line);
                } else if (line.contains("--")) {
                    int idx = line.indexOf("--");
                    sqlLines[i] = line.substring(0, idx) + replacer.apply(TAG.LINE_COMMENT, line.substring(idx));
                }
            }
            colorfulSql = String.join("\n", sqlLines);
            // resolve block comment
            StringBuilder parsedSql = new StringBuilder();
            Matcher matcher = BLOCK_COMMENT_PATTERN.matcher(colorfulSql);
            int lastMatchEnd = 0;
            while (matcher.find()) {
                String match = matcher.group();
                parsedSql.append(colorfulSql, lastMatchEnd, matcher.start());
                parsedSql.append(replacer.apply(TAG.BLOCK_COMMENT, match));
                lastMatchEnd = matcher.end();
            }
            parsedSql.append(colorfulSql.substring(lastMatchEnd));
            return parsedSql.toString();
        } catch (Exception e) {
            log.error("highlight sql error.", e);
            return sql;
        }
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
        if (i < j - 1) {
            return delimiters.get(i).trim().startsWith("(");
        }
        return false;
    }

    private static boolean detectNamedParameter(String word, int i, List<String> delimiters) {
        if (!word.matches(Patterns.VAR_KEY_PATTERN)) {
            return false;
        }
        int idx = i > 0 ? i - 1 : i;
        String prefix = delimiters.get(idx).trim();
        return prefix.endsWith(":") && !prefix.endsWith("::");
    }

    /**
     * Escape sql substring (single quotes) to unique string holder and save the substring map.
     *
     * @param sql sql string
     * @return [sql string with unique string holder, substring map]
     */
    private static Pair<String, Map<String, String>> escapeSubstring(final String sql) {
        //noinspection UnnecessaryUnicodeEscape
        if (!sql.contains("'")) {
            return Pair.of(sql, Collections.emptyMap());
        }
        Matcher m = STR_PATTERN.matcher(sql);
        Map<String, String> map = new HashMap<>();
        StringBuilder sb = new StringBuilder();
        int pos = 0;
        while (m.find()) {
            int start = m.start();
            int end = m.end();
            String str = m.group();
            String holder = UUID.randomUUID().toString();
            map.put(holder, str);
            sb.append(sql, pos, start).append(holder);
            pos = end;
        }
        sb.append(sql, pos, sql.length());
        return Pair.of(sb.toString(), map);
    }
}
