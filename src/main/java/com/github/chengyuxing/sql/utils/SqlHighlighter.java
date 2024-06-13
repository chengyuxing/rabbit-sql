package com.github.chengyuxing.sql.utils;

import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.console.Printer;
import com.github.chengyuxing.common.script.Patterns;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.Keywords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static com.github.chengyuxing.sql.utils.SqlUtil.getBlockAnnotation;

/**
 * Sql highlight util.
 */
public final class SqlHighlighter {
    private static final Logger log = LoggerFactory.getLogger(SqlHighlighter.class);

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
        LINE_ANNOTATION,
        BLOCK_ANNOTATION,
        NAMED_PARAMETER
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
                case LINE_ANNOTATION:
                    return Printer.colorful(content, Color.SILVER);
                case BLOCK_ANNOTATION:
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
            Pair<String, Map<String, String>> r = SqlUtil.escapeSubstring(sql);
            String rSql = r.getItem1();
            Pair<List<String>, List<String>> x = StringUtil.regexSplit(rSql, "(?<d>[\\s,\\[\\]():;{}]+)", "d");
            List<String> words = x.getItem1();
            List<String> delimiters = x.getItem2();
            StringBuilder sb = new StringBuilder();
            for (int i = 0, j = words.size(); i < j; i++) {
                String word = words.get(i);
                String replacement = word;
                if (!word.trim().isEmpty()) {
                    // functions highlight
                    if (!StringUtil.equalsAnyIgnoreCase(word, Keywords.STANDARD) && detectFunction(word, i, j, delimiters)) {
                        replacement = replacer.apply(TAG.FUNCTION, word);
                        // named parameter
                    } else if (detectNamedParameter(word, i, delimiters)) {
                        replacement = replacer.apply(TAG.NAMED_PARAMETER, word);
                        // keywords highlight
                    } else if (StringUtil.equalsAnyIgnoreCase(word, Keywords.STANDARD)) {
                        replacement = replacer.apply(TAG.KEYWORD, word);
                        // number highlight
                    } else if (StringUtil.isNumeric(word)) {
                        replacement = replacer.apply(TAG.NUMBER, word);
                        // PostgreSQL function body block highlight
                    } else if (word.equals("$$")) {
                        replacement = replacer.apply(TAG.POSTGRESQL_FUNCTION_BODY_SYMBOL, word);
                        // symbol '*' highlight
                    } else if (word.equals("*")) {
                        replacement = replacer.apply(TAG.ASTERISK, word);
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
            // resolve single annotation
            String[] sqlLines = colorfulSql.split("\n");
            for (int i = 0; i < sqlLines.length; i++) {
                String line = sqlLines[i];
                if (line.trim().startsWith("--")) {
                    sqlLines[i] = replacer.apply(TAG.LINE_ANNOTATION, line);
                } else if (line.contains("--")) {
                    int idx = line.indexOf("--");
                    sqlLines[i] = line.substring(0, idx) + replacer.apply(TAG.LINE_ANNOTATION, line.substring(idx));
                }
            }
            colorfulSql = String.join("\n", sqlLines);
            // resolve block annotation
            if (colorfulSql.contains("/*") && colorfulSql.contains("*/")) {
                List<String> annotations = getBlockAnnotation(colorfulSql);
                for (String annotation : annotations) {
                    colorfulSql = colorfulSql.replace(annotation, replacer.apply(TAG.BLOCK_ANNOTATION, annotation));
                }
            }
            return colorfulSql;
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
}
