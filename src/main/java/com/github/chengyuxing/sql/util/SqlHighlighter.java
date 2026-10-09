package com.github.chengyuxing.sql.util;

import com.github.chengyuxing.common.console.Style;
import com.github.chengyuxing.common.console.Printer;
import com.github.chengyuxing.common.script.ast.impl.KeyExpressionParser;
import com.github.chengyuxing.common.script.lang.Constants;
import com.github.chengyuxing.common.script.lang.TokenType;
import com.github.chengyuxing.common.script.lexer.RabbitScriptLexer;
import com.github.chengyuxing.common.util.StringUtils;
import com.github.chengyuxing.sql.Keywords;
import com.github.chengyuxing.sql.XQLFileManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Sql highlight util.
 */
public final class SqlHighlighter {
    private static final Logger log = LoggerFactory.getLogger(SqlHighlighter.class);
    public static final Pattern METADATA_NAME_PATTERN = Pattern.compile("@[a-zA-Z]\\w+");
    private static final Pattern WORD_PATTERN = Pattern.compile("[#@]?[\\p{L}_][\\p{L}\\p{N}_$]*(?:\\.[\\p{L}_][\\p{L}\\p{N}_$]*)*");
    private static final Pattern NUMBER_PATTERN = Pattern.compile("(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][+-]?\\d+)?");
    private static final Pattern DOLLAR_DELIMITER_PATTERN = Pattern.compile("\\$(?:[\\p{L}_][\\p{L}\\p{N}_]*)?\\$");
    private static final Set<String> KEYWORDS = new HashSet<>(Arrays.asList(Keywords.STANDARD));

    public enum TAG {
        FUNCTION("func_name("),
        KEYWORD("sql statement keyword"),
        NUMBER("1, 3.14"),
        /** Legacy delimiter tag; complete dollar-quoted values now use QUOTE_STRING. */
        POSTGRESQL_FUNCTION_BODY_SYMBOL("$$"),
        ASTERISK("*"),
        QUOTE_STRING("quoted string or identifier, including dollar quoting"),
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
                    if (content.startsWith("'") || content.startsWith("$")) {
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
     * Custom highlight SQL string. Scans original text without placeholder substitution.
     * Quoted values (including dollar quoting), identifiers and comments are opaque tokens.
     * RabbitScript and metadata comments additionally highlight their contents.
     *
     * @param sql                 SQL string
     * @param commentStyleCleaner removes existing styles before classifying comments
     * @param replacer            colored content function: (tag, content) -&gt; colored content
     * @return highlighted SQL; the original SQL if highlighting fails
     */
    public static String highlight(String sql, Function<String, String> commentStyleCleaner, BiFunction<TAG, String, String> replacer) {
        try {
            return highlightSource(sql, commentStyleCleaner, replacer);
        } catch (Exception e) {
            log.error("highlight sql error.", e);
            return sql;
        }
    }

    private static String highlightSource(String sql, Function<String, String> cleaner, BiFunction<TAG, String, String> replacer) {
        StringBuilder result = new StringBuilder(sql.length());
        Matcher words = WORD_PATTERN.matcher(sql);
        Matcher numbers = NUMBER_PATTERN.matcher(sql);
        Matcher parameters = KeyExpressionParser.EXPRESSION_PATTERN.matcher(sql);
        Matcher dollars = DOLLAR_DELIMITER_PATTERN.matcher(sql);
        int length = sql.length();
        for (int i = 0; i < length; ) {
            char c = sql.charAt(i);
            if (sql.startsWith("--", i)) {
                int end = i + 2;
                while (end < length && sql.charAt(end) != '\n' && sql.charAt(end) != '\r') {
                    end++;
                }
                String comment = sql.substring(i, end);
                TAG tag = commentTag(sql, i, comment, cleaner);
                if (tag != TAG.LINE_COMMENT) {
                    comment = "--" + highlightSource(comment.substring(2), cleaner, replacer);
                }
                result.append(replacer.apply(tag, comment));
                i = end;
            } else if (sql.startsWith("/*", i)) {
                int end = blockCommentEnd(sql, i);
                result.append(replacer.apply(TAG.BLOCK_COMMENT, sql.substring(i, end)));
                i = end;
            } else if (c == '\'' || c == '"' || c == '`') {
                boolean escaped = c == '`' || c == '\'' && i > 0
                        && (sql.charAt(i - 1) == 'E' || sql.charAt(i - 1) == 'e')
                        && (i == 1 || !isIdentifierPart(sql.charAt(i - 2)));
                int end = quotedEnd(sql, i, c, escaped);
                result.append(replacer.apply(TAG.QUOTE_STRING, sql.substring(i, end)));
                i = end;
            } else if (c == '$' && (i == 0 || !isIdentifierPart(sql.charAt(i - 1)))
                    && dollars.region(i, length).lookingAt()) {
                String delimiter = dollars.group();
                int close = sql.indexOf(delimiter, dollars.end());
                int end = close < 0 ? length : close + delimiter.length();
                result.append(replacer.apply(TAG.QUOTE_STRING, sql.substring(i, end)));
                i = end;
            } else if (c == ':' && (i == 0 || sql.charAt(i - 1) != ':')
                    && parameters.region(i + 1, length).lookingAt()) {
                result.append(':').append(replacer.apply(TAG.NAMED_PARAMETER, parameters.group()));
                i = parameters.end();
            } else if (words.region(i, length).lookingAt()) {
                String word = words.group();
                int end = words.end();
                TAG tag = TAG.OTHER;
                if (KEYWORDS.contains(word.toLowerCase(Locale.ROOT))) {
                    tag = TAG.KEYWORD;
                } else if (word.charAt(0) != '#' && word.charAt(0) != '@' && followedByParenthesis(sql, end)) {
                    tag = TAG.FUNCTION;
                }
                result.append(replacer.apply(tag, word));
                i = end;
            } else if (numbers.region(i, length).lookingAt()) {
                result.append(replacer.apply(TAG.NUMBER, numbers.group()));
                i = numbers.end();
            } else if (c == '*') {
                result.append(replacer.apply(TAG.ASTERISK, "*"));
                i++;
            } else {
                result.append(c);
                i++;
            }
        }
        return result.toString();
    }

    private static TAG commentTag(String sql, int start, String comment, Function<String, String> cleaner) {
        int lineStart = Math.max(sql.lastIndexOf('\n', start - 1), sql.lastIndexOf('\r', start - 1)) + 1;
        String head = cleaner.apply(sql.substring(lineStart, start));
        if (!StringUtils.isBlank(head)) {
            return TAG.LINE_COMMENT;
        }
        String line = head + cleaner.apply(comment);
        if (XQLFileManager.META_DATA_PATTERN.matcher(line).matches()) {
            return TAG.METADATA_DEFINE_COMMENT;
        }
        if (XQLFileManager.INLINE_TEMPLATE_BEGIN_PATTERN.matcher(line).matches()
                || XQLFileManager.INLINE_TEMPLATE_END_PATTERN.matcher(line).matches()) {
            return TAG.INLINE_TEMPLATE_COMMENT;
        }
        if (RabbitScriptLexer.DIRECTIVES_PATTERN.matcher(cleaner.apply(comment.substring(2))).matches()) {
            return TAG.RABBIT_SCRIPT_COMMENT;
        }
        return TAG.LINE_COMMENT;
    }

    private static int quotedEnd(String sql, int start, char quote, boolean backslashEscapes) {
        for (int i = start + 1; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (backslashEscapes && c == '\\' && i + 1 < sql.length()) {
                i++;
            } else if (c == quote) {
                if (i + 1 < sql.length() && sql.charAt(i + 1) == quote) {
                    i++;
                } else {
                    return i + 1;
                }
            }
        }
        return sql.length();
    }

    private static int blockCommentEnd(String sql, int start) {
        int depth = 1;
        int i = start + 2;
        while (i < sql.length()) {
            if (sql.startsWith("/*", i)) {
                depth++;
                i += 2;
            } else if (sql.startsWith("*/", i)) {
                i += 2;
                if (--depth == 0) {
                    return i;
                }
            } else {
                i++;
            }
        }
        return sql.length();
    }

    private static boolean followedByParenthesis(String sql, int start) {
        while (start < sql.length() && Character.isWhitespace(sql.charAt(start))) {
            start++;
        }
        return start < sql.length() && sql.charAt(start) == '(';
    }

    private static boolean isIdentifierPart(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '$';
    }
}
